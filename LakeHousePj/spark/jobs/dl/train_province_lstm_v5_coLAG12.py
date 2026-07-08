"""
ML Pipeline: Train LSTM Deep Learning Model & Forecast Province Hotel Volume
=============================================================================

Source table: gold.gold.fact_province_month_dl_features (~22 features after v5.2 pruning)

Architecture:
- 2-layer LSTM + LayerNorm + Temporal Attention
- Deeper FC head: Linear(hidden) → GELU → Dropout → Linear(16) → ReLU → Linear(1)
- HybridLoss: 70% HuberLoss + 30% SMAPELoss
- RobustScaler (median/IQR)
- CosineAnnealingWarmRestarts scheduler

Features v5.2: 22 optimal features (reduced from 42 to prevent overfitting on 790 samples)
  Temporal (2): month_sin, month_cos
  Hotel lag (3): hotel_vol_lag_12, rolling_3m, momentum
  Hotness lag (3): hotness_lag_12, rolling_3m, momentum
  Volume (3): total_posts, total_comments, unique_authors
  Engagement (3): avg_likes_per_post, avg_saves_per_post, viral_post_ratio
  NLP (3): avg_sentiment, sentiment_std, reply_ratio
  Aspect (4): avg_aspect_scenery, avg_aspect_food, avg_aspect_price, avg_aspect_service
  Hotel quality (4): avg_hotel_score, hotel_score_std, domestic_review_ratio, hotel_vol_growth

Output:
- Model: province_hotel_volume_forecaster_lstm_v5.2 (MLflow registry)
- Table: gold.gold.province_month_forecast_lstm_next12
"""

import sys
sys.path.append('/opt/spark/jobs')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType,
    DoubleType, TimestampType
)
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import mlflow
import mlflow.pytorch
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://minio:9000'
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin123'

import pickle
import json
import warnings
warnings.filterwarnings('ignore')

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

from utils.iceberg_utils import create_iceberg_table_if_not_exists

# ============================================================
# Configuration
# ============================================================

MLFLOW_TRACKING_URI = "http://mlflow:5000"
EXPERIMENT_NAME     = "province_hotel_volume_forecasting_lstm"
MODEL_NAME          = "province_hotel_volume_forecaster_lstm_v5"

DL_FEATURES_TABLE = "gold.gold.fact_province_month_dl_features"

FORECAST_MONTHS = 12

# 3-way time-based split (train/val/test)
# 75% train → 12.5% val → 12.5% test
TRAIN_RATIO = 0.7
VAL_RATIO   = 0.15
TEST_RATIO  = 1.0 - TRAIN_RATIO - VAL_RATIO

# Model hyperparameters
SEQUENCE_LENGTH = 3
HIDDEN_SIZE     = 96
NUM_LAYERS      = 2
DROPOUT         = 0.15
LEARNING_RATE   = 0.0005
EPOCHS          = 200
BATCH_SIZE      = 32
PATIENCE        = 25
WEIGHT_DECAY    = 1e-5

# Hyperparameter Search Space for Automated Tuning
TUNING_TRIALS = 5
SEARCH_HIDDEN_SIZE = [32, 48, 64, 96]
SEARCH_DROPOUT = [0.15, 0.30, 0.45]
SEARCH_LEARNING_RATE = [0.0001, 0.0005, 0.001]
SEARCH_WEIGHT_DECAY = [1e-6, 1e-5, 5e-5]

TARGET = "hotel_review_volume"

# ============================================================
# Feature Groups
# ============================================================

TEMPORAL_FEATURES = ["month_sin", "month_cos"]

HOTEL_LAG_FEATURES = [
    "hotel_vol_lag_12", "hotel_vol_rolling_3m", "hotel_vol_momentum",
]

# Hotness lag features
HOTNESS_LAG_FEATURES = [
    "hotness_lag_12", "hotness_rolling_3m", "hotness_momentum",
]

VOLUME_FEATURES = [
    "total_posts", "total_comments", "unique_authors",
]

ENGAGEMENT_FEATURES = [
    "avg_likes_per_post", "avg_saves_per_post", "viral_post_ratio",
]

NLP_FEATURES = [
    "avg_sentiment", "sentiment_std", "reply_ratio",
]

ASPECT_FEATURES = [
    "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price", "avg_aspect_service",
    "avg_aspect_transport", "avg_aspect_accommodation",
]

HOTEL_FEATURES = [
    "avg_hotel_score", "hotel_score_std", "domestic_review_ratio", "hotel_vol_growth",
]

CUSTOM_FEATURES = []

# Full feature set (v5.2: 22 optimal features — reduced from 42)
ALL_FEATURES = (
    TEMPORAL_FEATURES
    + HOTEL_LAG_FEATURES
    + HOTNESS_LAG_FEATURES
    + VOLUME_FEATURES
    + ENGAGEMENT_FEATURES
    + NLP_FEATURES
    + ASPECT_FEATURES
    + HOTEL_FEATURES
)

# Feature separation for late fusion architecture
SEQ_FEATURES = [c for c in ALL_FEATURES if c != "hotel_vol_lag_12"]
ANCHOR_FEATURE = "hotel_vol_lag_12"

# ============================================================
# Loss Functions
# ============================================================

class SMAPELoss(nn.Module):
    """Symmetric MAPE — less biased than MAPE for values near 0."""
    def forward(self, pred, target):
        denom = (torch.abs(target) + torch.abs(pred)) / 2.0 + 1e-8
        return torch.mean(torch.abs(pred - target) / denom)


class HybridLoss(nn.Module):
    """70% HuberLoss (stable gradient) + 30% SMAPELoss (drives MAPE down)."""
    def __init__(self, delta=0.5, smape_weight=0.3):
        super().__init__()
        self.huber = nn.HuberLoss(delta=delta)
        self.smape = SMAPELoss()
        self.w = smape_weight

    def forward(self, pred, target):
        return (1 - self.w) * self.huber(pred, target) + self.w * self.smape(pred, target)


# ============================================================
# LSTM Model
# ============================================================

class TemporalAttention(nn.Module):
    def __init__(self, hidden_size):
        super().__init__()
        self.attn = nn.Linear(hidden_size, 1)

    def forward(self, lstm_output):
        scores  = self.attn(lstm_output).squeeze(-1)
        weights = torch.softmax(scores, dim=1)
        context = torch.bmm(weights.unsqueeze(1), lstm_output).squeeze(1)
        return context, weights


class LSTMForecaster(nn.Module):
    """
    2-layer LSTM + LayerNorm + Temporal Attention + late fusion + seasonal residual FC head.
    """
    def __init__(self, input_size, hidden_size, num_layers, dropout):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size, hidden_size=hidden_size,
            num_layers=num_layers, batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0
        )
        self.layer_norm = nn.LayerNorm(hidden_size)
        self.attention  = TemporalAttention(hidden_size)
        self.fc = nn.Sequential(
            nn.Linear(hidden_size + 1, hidden_size), # input: context + scaled_target_lag
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 16),
            nn.ReLU(),
            nn.Linear(16, 1)
        )

    def forward(self, x, target_lag_scaled, target_lag_log):
        lstm_out, _  = self.lstm(x)
        lstm_out     = self.layer_norm(lstm_out)
        context, _   = self.attention(lstm_out)
        
        # Late fusion of short-term context and long-term seasonality
        fusion       = torch.cat([context, target_lag_scaled], dim=-1)
        delta        = self.fc(fusion).squeeze(-1)
        
        # Residual connection on log scale
        return target_lag_log.squeeze(-1) + delta


class TimeSeriesDataset(Dataset):
    def __init__(self, X_seq, X_anchor_scaled, X_anchor_log, y):
        self.X_seq = torch.FloatTensor(X_seq)
        self.X_anchor_scaled = torch.FloatTensor(X_anchor_scaled).unsqueeze(-1)
        self.X_anchor_log = torch.FloatTensor(X_anchor_log).unsqueeze(-1)
        self.y = torch.FloatTensor(y)

    def __len__(self):          return len(self.X_seq)
    def __getitem__(self, idx): return (self.X_seq[idx], self.X_anchor_scaled[idx], self.X_anchor_log[idx]), self.y[idx]


# ============================================================
# Spark Session
# ============================================================

def create_spark_session():
    return SparkSession.builder \
        .appName("ML_Province_HotelVolume_LSTM_v5") \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.gold",         "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gold.type",    "hive") \
        .config("spark.sql.catalog.gold.uri",     "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.gold.warehouse","s3a://gold/lakehouse") \
        .config("spark.hadoop.fs.s3a.endpoint",   "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores",  "2") \
        .getOrCreate()


# ============================================================
# Step 1: Load features
# ============================================================

def load_features(spark):
    print("\n" + "=" * 80)
    print("STEP 1: LOADING PRE-COMPUTED DL FEATURES")
    print("=" * 80)

    df = spark.table(DL_FEATURES_TABLE)
    # Filter out extremely sparse years 2020-2022 to improve model stability
    df = df.filter(F.col("year") >= 2023)
    total     = df.count()
    provinces = df.select("province_sk").distinct().count()
    print(f"  Table:     {DL_FEATURES_TABLE}")
    print(f"  Rows:      {total}, Provinces: {provinces}")

    # Verify new columns exist in table
    available_cols = set(df.columns)
    missing = [c for c in ALL_FEATURES if c not in available_cols]
    if missing:
        print(f"  WARNING: {len(missing)} feature(s) missing from table: {missing}")
        print("  These will be filled with 0. Check Gold ETL pipeline.")

    select_cols = list({
        *ALL_FEATURES, TARGET, "year_month", "month",
        "province_sk", "province_name", "region"
    } & available_cols) + [
        c for c in [TARGET, "year_month", "month", "province_sk", "province_name", "region"]
        if c not in available_cols
    ]
    # Safe select: only existing columns
    safe_features = [f for f in ALL_FEATURES if f in available_cols]
    select_cols   = safe_features + [TARGET, "year_month", "month",
                                     "province_sk", "province_name", "region"]
    df = df.select(*list(dict.fromkeys(select_cols)))  # deduplicate

    # Fill missing feature columns with 0
    for c in missing:
        df = df.withColumn(c, F.lit(0.0))

    before = df.count()
    df = df.dropna(subset=HOTEL_LAG_FEATURES + [TARGET])
    df = df.filter(F.col(TARGET) > 0)
    after = df.count()

    print(f"  Dropped {before - after} rows (NULL lags or zero target), remaining: {after}")
    print(f"  Features: {len(ALL_FEATURES)} total")
    print(f"    Temporal({len(TEMPORAL_FEATURES)}) + "
          f"HotelLag({len(HOTEL_LAG_FEATURES)}) + "
          f"HotnessLag({len(HOTNESS_LAG_FEATURES)}) + "
          f"Volume({len(VOLUME_FEATURES)}) + "
          f"Engagement({len(ENGAGEMENT_FEATURES)}) + "
          f"NLP({len(NLP_FEATURES)}) + "
          f"Aspect({len(ASPECT_FEATURES)}) + "
          f"Hotel({len(HOTEL_FEATURES)}) + "
          f"Custom({len(CUSTOM_FEATURES)})")
    return df


# ============================================================
# Step 2: Preprocessing helpers
# ============================================================

def clip_and_scale_entire(df_pd, train_end, features):
    """
    Fit clip thresholds and RobustScaler on Train period only,
    apply consistently to the entire dataset (no time-boundary sequence loss).
    """
    df = df_pd.copy()

    # --- Outlier clipping (applied to entire df first) ---
    GROWTH_MIN, GROWTH_MAX = -1.0, 5.0
    df["hotel_vol_growth"] = df["hotel_vol_growth"].clip(GROWTH_MIN, GROWTH_MAX)
    if "engagement_score" in df.columns:
        df["engagement_score"] = np.log1p(df["engagement_score"])

    # Create temporary train subset to compute train-only statistics
    train_mask = df["year_month"] <= train_end
    train_raw = df[train_mask].copy()

    # p99 clipping computed on train only
    clip_cols = ["total_posts", "total_comments", "avg_likes_per_post",
                 "avg_saves_per_post", "engagement_score"]
    clip_thresholds = {}
    for col in clip_cols:
        if col in features:
            p99 = train_raw[col].quantile(0.99)
            if p99 > 0:
                clip_thresholds[col] = p99

    # Apply clipping thresholds to the entire df and the train subset
    for col, threshold in clip_thresholds.items():
        df[col] = df[col].clip(upper=threshold)
        train_raw[col] = train_raw[col].clip(upper=threshold)

    print(f"  Clip thresholds (train p99): { {k: f'{v:.2f}' for k,v in clip_thresholds.items()} }")

    # --- RobustScaler: fit on train only ---
    scaler = RobustScaler()
    scaler.fit(train_raw[features])
    df[features] = scaler.transform(df[features])

    return df, scaler, clip_thresholds


def prepare_sequences(df_pd_scaled, df_pd_unscaled, seq_features, anchor_feature, target, seq_length):
    X_seq, X_anchor_scaled, X_anchor_log, y_all, meta_all = [], [], [], [], []
    anchor_idx = ALL_FEATURES.index(anchor_feature)
    
    for province_sk, group_scaled in df_pd_scaled.groupby("province_sk"):
        group_scaled = group_scaled.sort_values("year_month")
        group_unscaled = df_pd_unscaled[df_pd_unscaled["province_sk"] == province_sk].sort_values("year_month")
        
        # Features values scaled (excluding anchor for LSTM)
        seq_values = group_scaled[seq_features].values
        
        # All features scaled values (used to extract scaled anchor)
        all_values_scaled = group_scaled[ALL_FEATURES].values
        
        # Target values (already log1p-transformed)
        targets = group_scaled[target].values
        
        # Raw anchor values from df_pd_unscaled
        lags_raw = group_unscaled[anchor_feature].values
        lags_log = np.log1p(lags_raw.astype(float))
        
        yms = group_scaled["year_month"].values
        
        for i in range(seq_length, len(group_scaled)):
            target_lag_scaled = all_values_scaled[i, anchor_idx]
            target_lag_log = lags_log[i]
            
            seq = seq_values[i - seq_length:i]
            
            X_seq.append(seq)
            X_anchor_scaled.append(target_lag_scaled)
            X_anchor_log.append(target_lag_log)
            y_all.append(targets[i])
            meta_all.append({
                "province_sk":   province_sk,
                "year_month":    int(yms[i]),
                "province_name": group_scaled["province_name"].iloc[i],
            })
    return np.array(X_seq), np.array(X_anchor_scaled), np.array(X_anchor_log), np.array(y_all), meta_all


def compute_metrics(y_true, y_pred, prefix=""):
    """Compute all metrics on log scale, plus MAPE/SMAPE on actual scale."""
    y_true_actual = np.expm1(y_true)
    y_pred_actual = np.expm1(np.clip(y_pred, 0.0, None))

    mask      = y_true_actual > 0
    mape      = float(np.mean(np.abs(
        (y_true_actual[mask] - y_pred_actual[mask]) / y_true_actual[mask]
    )) * 100) if np.sum(mask) > 0 else 0.0

    smape_log = float(
        2 * np.mean(np.abs(y_true - y_pred) /
                    (np.abs(y_true) + np.abs(y_pred) + 1e-8)) * 100
    )

    return {
        f"{prefix}rmse":        float(np.sqrt(mean_squared_error(y_true, y_pred))),
        f"{prefix}mae":         float(mean_absolute_error(y_true, y_pred)),
        f"{prefix}r2":          float(r2_score(y_true, y_pred)),
        f"{prefix}mape_actual": mape,
        f"{prefix}smape_log":   smape_log,
    }


# ============================================================
# Step 3b: Single LSTM training run (reusable)
# ============================================================

def train_single_lstm(train_data, y_train, val_data, y_val, test_data, y_test,
                      input_size, config, run_name, device):
    """Train a single LSTM configuration."""
    X_train_seq, X_train_anchor_scaled, X_train_anchor_log = train_data
    X_val_seq, X_val_anchor_scaled, X_val_anchor_log = val_data
    X_test_seq, X_test_anchor_scaled, X_test_anchor_log = test_data

    hidden_size = config.get("hidden_size", HIDDEN_SIZE)
    dropout = config.get("dropout", DROPOUT)
    learning_rate = config.get("learning_rate", LEARNING_RATE)
    weight_decay = config.get("weight_decay", WEIGHT_DECAY)

    model = LSTMForecaster(
        input_size=input_size, hidden_size=hidden_size,
        num_layers=NUM_LAYERS, dropout=dropout
    ).to(device)

    total_params = sum(p.numel() for p in model.parameters())

    optimizer  = torch.optim.Adam(model.parameters(),
                                  lr=learning_rate, weight_decay=weight_decay)
    criterion  = HybridLoss(delta=0.5, smape_weight=0.3)
    # FIX 7: CosineAnnealingWarmRestarts.step(epoch) — step theo epoch position
    scheduler  = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(
        optimizer, T_0=30, T_mult=2, eta_min=1e-6
    )

    train_loader = DataLoader(TimeSeriesDataset(X_train_seq, X_train_anchor_scaled, X_train_anchor_log, y_train),
                              batch_size=BATCH_SIZE, shuffle=True)
    val_loader   = DataLoader(TimeSeriesDataset(X_val_seq, X_val_anchor_scaled, X_val_anchor_log, y_val),
                              batch_size=BATCH_SIZE, shuffle=False)

    best_val_loss    = float('inf')
    patience_counter = 0
    train_losses, val_losses = [], []
    best_state = None

    for epoch in range(EPOCHS):
        # --- Train ---
        model.train()
        epoch_loss = 0.0
        for xb, yb in train_loader:
            seq, anchor_scaled, anchor_log = xb
            seq, anchor_scaled, anchor_log, yb = (
                seq.to(device), anchor_scaled.to(device), anchor_log.to(device), yb.to(device)
            )
            optimizer.zero_grad()
            loss = criterion(model(seq, anchor_scaled, anchor_log), yb)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            epoch_loss += loss.item() * len(seq)
        avg_train = epoch_loss / len(X_train_seq)
        train_losses.append(avg_train)

        # --- Validate ---
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for xb, yb in val_loader:
                seq, anchor_scaled, anchor_log = xb
                seq, anchor_scaled, anchor_log, yb = (
                    seq.to(device), anchor_scaled.to(device), anchor_log.to(device), yb.to(device)
                )
                val_loss += criterion(model(seq, anchor_scaled, anchor_log), yb).item() * len(seq)
        avg_val = val_loss / max(len(X_val_seq), 1)
        val_losses.append(avg_val)

        # Step by epoch position
        scheduler.step(epoch)

        if (epoch + 1) % 20 == 0:
            lr = optimizer.param_groups[0]['lr']
            print(f"    [{run_name}] Epoch {epoch+1}/{EPOCHS} | "
                  f"Train: {avg_train:.5f} | Val: {avg_val:.5f} | LR: {lr:.6f}")

        # Early stopping on VAL with warmup to allow the model to move away from the epoch 0 baseline
        if epoch > 0 and avg_val < best_val_loss:
            best_val_loss    = avg_val
            patience_counter = 0
            best_state       = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            if epoch >= 100:  # Start patience check after epoch 50 (warmup phase)
                patience_counter += 1
                if patience_counter >= PATIENCE:
                    print(f"    [{run_name}] Early stopping at epoch {epoch+1}")
                    break

    # Load best checkpoint
    model.load_state_dict(best_state)
    model.to(device)

    # --- Evaluate on TEST ---
    model.eval()
    with torch.no_grad():
        y_pred_train = model(
            torch.FloatTensor(X_train_seq).to(device),
            torch.FloatTensor(X_train_anchor_scaled).unsqueeze(-1).to(device),
            torch.FloatTensor(X_train_anchor_log).unsqueeze(-1).to(device)
        ).cpu().numpy()
        
        y_pred_val = model(
            torch.FloatTensor(X_val_seq).to(device),
            torch.FloatTensor(X_val_anchor_scaled).unsqueeze(-1).to(device),
            torch.FloatTensor(X_val_anchor_log).unsqueeze(-1).to(device)
        ).cpu().numpy()
        
        y_pred_test = model(
            torch.FloatTensor(X_test_seq).to(device),
            torch.FloatTensor(X_test_anchor_scaled).unsqueeze(-1).to(device),
            torch.FloatTensor(X_test_anchor_log).unsqueeze(-1).to(device)
        ).cpu().numpy()

    y_pred_train = np.clip(y_pred_train, 0.0, None)
    y_pred_val   = np.clip(y_pred_val,   0.0, None)
    y_pred_test  = np.clip(y_pred_test,  0.0, None)

    train_metrics = compute_metrics(y_train, y_pred_train, "train_")
    val_metrics   = compute_metrics(y_val,   y_pred_val,   "val_")
    test_metrics  = compute_metrics(y_test,  y_pred_test,  "test_")

    all_metrics = {
        **train_metrics, **val_metrics, **test_metrics,
        "best_val_loss":  float(best_val_loss),
        "epochs_trained": len(train_losses),
        "total_params":   total_params,
        "input_size":     input_size,
    }

    return model, all_metrics, train_losses, val_losses, y_pred_test


# ============================================================
# Step 3c: Main LSTM training
# ============================================================

def train_model(df):
    print("\n" + "=" * 80)
    print("STEP 3: TRAINING LSTM MODEL v5")
    print("=" * 80)

    select_cols = ALL_FEATURES + [TARGET, "year_month", "province_sk", "province_name", "region"]
    df_pd       = df.select(select_cols).toPandas()
    df_pd       = df_pd.sort_values(["province_sk", "year_month"])
    df_pd[ALL_FEATURES] = df_pd[ALL_FEATURES].fillna(0)

    # Log-transform target BEFORE split
    df_pd[TARGET] = np.log1p(df_pd[TARGET].astype(float))

    # Determine split boundaries based on sequence percentiles (sequence-based chronological split)
    ym_seqs = []
    for province_sk, group in df_pd.groupby("province_sk"):
        group = group.sort_values("year_month")
        yms = group["year_month"].values[SEQUENCE_LENGTH:]
        ym_seqs.extend(yms)
    
    ym_seqs = sorted(ym_seqs)
    n_seqs = len(ym_seqs)
    train_end = ym_seqs[int(n_seqs * TRAIN_RATIO) - 1]
    val_end   = ym_seqs[int(n_seqs * (TRAIN_RATIO + VAL_RATIO)) - 1]

    print(f"  Split boundaries (sequence-based): train≤{train_end} | val {train_end+1}–{val_end} | test>{val_end}")

    # Clip and scale the entire dataframe using train-derived parameters
    df_pd_scaled, scaler, clip_thresholds = clip_and_scale_entire(
        df_pd, train_end, ALL_FEATURES
    )

    # Build sequences for the entire dataset (prevents boundary sequence loss!)
    X_seq, X_anchor_scaled, X_anchor_log, y_all, meta_all = prepare_sequences(
        df_pd_scaled, df_pd, SEQ_FEATURES, ANCHOR_FEATURE, TARGET, SEQUENCE_LENGTH
    )

    # Chronologically split sequences based on the target year_month
    X_train_seq, X_train_anchor_scaled, X_train_anchor_log, y_train = [], [], [], []
    X_val_seq,   X_val_anchor_scaled,   X_val_anchor_log,   y_val   = [], [], [], []
    X_test_seq,  X_test_anchor_scaled,  X_test_anchor_log,  y_test  = [], [], [], []
    meta_test        = []

    for i in range(len(X_seq)):
        ym = meta_all[i]["year_month"]
        if ym <= train_end:
            X_train_seq.append(X_seq[i])
            X_train_anchor_scaled.append(X_anchor_scaled[i])
            X_train_anchor_log.append(X_anchor_log[i])
            y_train.append(y_all[i])
        elif ym <= val_end:
            X_val_seq.append(X_seq[i])
            X_val_anchor_scaled.append(X_anchor_scaled[i])
            X_val_anchor_log.append(X_anchor_log[i])
            y_val.append(y_all[i])
        else:
            X_test_seq.append(X_seq[i])
            X_test_anchor_scaled.append(X_anchor_scaled[i])
            X_test_anchor_log.append(X_anchor_log[i])
            y_test.append(y_all[i])
            meta_test.append(meta_all[i])

    X_train_seq, X_train_anchor_scaled, X_train_anchor_log, y_train = np.array(X_train_seq), np.array(X_train_anchor_scaled), np.array(X_train_anchor_log), np.array(y_train)
    X_val_seq,   X_val_anchor_scaled,   X_val_anchor_log,   y_val   = np.array(X_val_seq),   np.array(X_val_anchor_scaled),   np.array(X_val_anchor_log),   np.array(y_val)
    X_test_seq,  X_test_anchor_scaled,  X_test_anchor_log,  y_test  = np.array(X_test_seq),  np.array(X_test_anchor_scaled),  np.array(X_test_anchor_log),  np.array(y_test)

    if len(X_train_seq) == 0:
        raise ValueError("Training set is empty after sequence preparation. "
                         "Check TRAIN_RATIO and data size.")

    print(f"\n  Sequences — Train: {len(X_train_seq)} | Val: {len(X_val_seq)} | Test: {len(X_test_seq)}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"  Device: {device}")

    # -------------------------------------------------------
    # Hyperparameter Tuning (Random Search)
    # -------------------------------------------------------
    import random
    
    # Sample configurations
    configs = []
    # Seed for deterministic trial choices
    random.seed(42)
    for _ in range(TUNING_TRIALS):
        configs.append({
            "hidden_size": random.choice(SEARCH_HIDDEN_SIZE),
            "dropout": random.choice(SEARCH_DROPOUT),
            "learning_rate": random.choice(SEARCH_LEARNING_RATE),
            "weight_decay": random.choice(SEARCH_WEIGHT_DECAY)
        })
        
    # Always include current baseline configuration as one of the trials
    configs.append({
        "hidden_size": HIDDEN_SIZE,
        "dropout": DROPOUT,
        "learning_rate": LEARNING_RATE,
        "weight_decay": WEIGHT_DECAY
    })
    
    # Deduplicate
    unique_configs = []
    for c in configs:
        if c not in unique_configs:
            unique_configs.append(c)
            
    print(f"\n  [Tuning] Starting Random Search with {len(unique_configs)} configurations...")
    best_val_loss = float('inf')
    best_config = None
    best_model = None
    best_metrics = None
    best_y_pred_test = None
    best_train_losses = None
    best_val_losses = None

    train_data = (X_train_seq, X_train_anchor_scaled, X_train_anchor_log)
    val_data   = (X_val_seq, X_val_anchor_scaled, X_val_anchor_log)
    test_data  = (X_test_seq, X_test_anchor_scaled, X_test_anchor_log)

    for idx, config in enumerate(unique_configs):
        print(f"\n  [Tuning] Trial {idx+1}/{len(unique_configs)} | Cấu hình: {config}")
        model_trial, metrics_trial, train_losses, val_losses, y_pred_test_trial = train_single_lstm(
            train_data, y_train, val_data, y_val, test_data, y_test,
            input_size=len(SEQ_FEATURES),
            config=config,
            run_name=f"trial_{idx+1}",
            device=device
        )
        val_loss = metrics_trial["best_val_loss"]
        print(f"  [Tuning] Trial {idx+1} kết thúc. Best Val Loss: {val_loss:.5f} | Train R²: {metrics_trial['train_r2']:.4f} | Val R²: {metrics_trial['val_r2']:.4f}")
        
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_config = config
            best_model = model_trial
            best_metrics = metrics_trial
            best_y_pred_test = y_pred_test_trial
            best_train_losses = train_losses
            best_val_losses = val_losses

    print(f"\n" + "="*80)
    print("  [Tuning] HOÀN THÀNH TÌM KIẾM SIÊU THAM SỐ")
    print(f"  [Tuning] Cấu hình tốt nhất: {best_config}")
    print(f"  [Tuning] Best Val Loss tương ứng: {best_val_loss:.5f}")
    print("="*80 + "\n")

    # -------------------------------------------------------
    # Main LSTM run (log best config to MLflow)
    # -------------------------------------------------------
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    print("\n  Logging best LSTM model to MLflow...")
    with mlflow.start_run(run_name=f"lstm_v5_tuned_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as main_run:

        print(f"\n  BEST MODEL RESULTS:")
        print(f"  Train: RMSE={best_metrics['train_rmse']:.4f}  MAE={best_metrics['train_mae']:.4f}  R²={best_metrics['train_r2']:.4f}")
        print(f"  Val:   RMSE={best_metrics['val_rmse']:.4f}    MAE={best_metrics['val_mae']:.4f}    R²={best_metrics['val_r2']:.4f}")
        print(f"  Test:  RMSE={best_metrics['test_rmse']:.4f}   MAE={best_metrics['test_mae']:.4f}   R²={best_metrics['test_r2']:.4f}")
        print(f"  MAPE(actual)={best_metrics['test_mape_actual']:.2f}%  SMAPE(log)={best_metrics['test_smape_log']:.2f}%")
        print(f"  Train-Val R² gap:  {best_metrics['train_r2'] - best_metrics['val_r2']:.4f}")
        print(f"  Val-Test R² gap:   {best_metrics['val_r2']   - best_metrics['test_r2']:.4f}")

        mlflow.log_params({
            "model_version":    "lstm_v5_tuned",
            "source_table":     DL_FEATURES_TABLE,
            "sequence_length":  SEQUENCE_LENGTH,
            "hidden_size":      best_config["hidden_size"],
            "num_layers":       NUM_LAYERS,
            "dropout":          best_config["dropout"],
            "learning_rate":    best_config["learning_rate"],
            "loss":             "HybridLoss_Huber0.5_SMAPE0.3",
            "scaler_type":      "RobustScaler",
            "epochs_max":       EPOCHS,
            "batch_size":       BATCH_SIZE,
            "patience":         PATIENCE,
            "weight_decay":     str(best_config["weight_decay"]),
            "scheduler":        "CosineAnnealingWarmRestarts_T0=30_step_by_epoch",
            "num_features":     len(ALL_FEATURES),
            "split":            f"train{int(TRAIN_RATIO*100)}/val{int(VAL_RATIO*100)}/test{int(TEST_RATIO*100)}",
            "tuning_trials":    len(unique_configs),
            "best_config_str":  str(best_config),
            "fix_1_val_split":  "True",
            "fix_2_clip_leak":  "True",
            "fix_3_hotness_lag":"True",
            "fix_4_aspect":     "True",
            "fix_7_scheduler":  "True",
            "fix_8_growth_pct": "True",
            "fix_9_bn_removed": "True",
        })
        mlflow.log_metrics(best_metrics)

        # Plots
        _log_training_plots(
            best_train_losses, best_val_losses, y_test, best_y_pred_test, best_metrics, meta_test
        )

        # Save artifacts
        mlflow.pytorch.log_model(best_model, "model")
        _save_artifacts(scaler, clip_thresholds)

        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, MODEL_NAME)
        print(f"  Model registered: {MODEL_NAME}")

    return best_model, scaler, clip_thresholds, df_pd_scaled, device, best_config


# ============================================================
# Plotting helpers
# ============================================================

def _log_training_plots(train_losses, val_losses, y_test, y_pred_test, metrics, meta_test):
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(train_losses, label='Train')
    ax.plot(val_losses,   label='Val (early stopping)')
    ax.set_xlabel('Epoch'); ax.set_ylabel('Loss')
    ax.set_title('LSTM v5 — Training & Validation Loss')
    ax.legend()
    mlflow.log_figure(fig, "loss_curve.png"); plt.close()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(y_test, y_pred_test, alpha=0.5, s=15)
    max_val = float(max(y_test.max(), y_pred_test.max()))
    ax.plot([0, max_val], [0, max_val], 'r--')
    ax.set_xlabel('Actual (log1p)'); ax.set_ylabel('Predicted (log1p)')
    ax.set_title(f"LSTM v5: Actual vs Predicted — Test Set (R²={metrics['test_r2']:.3f})")
    mlflow.log_figure(fig, "actual_vs_predicted_test.png"); plt.close()

    fig, ax = plt.subplots(figsize=(10, 6))
    residuals = y_test - y_pred_test
    ax.scatter(y_pred_test, residuals, alpha=0.5, s=15)
    ax.axhline(0, color='r', linestyle='--')
    ax.set_xlabel('Predicted'); ax.set_ylabel('Residual (actual - predicted)')
    ax.set_title('Residuals — Test Set')
    mlflow.log_figure(fig, "residuals_test.png"); plt.close()

    if meta_test:
        fig, ax = plt.subplots(figsize=(14, 6))
        sp  = meta_test[0]['province_sk']
        idx = [i for i, m in enumerate(meta_test) if m['province_sk'] == sp]
        if idx:
            ax.plot([meta_test[i]['year_month'] for i in idx],
                    [y_test[i]       for i in idx], label='Actual',    marker='o')
            ax.plot([meta_test[i]['year_month'] for i in idx],
                    [y_pred_test[i]  for i in idx], label='Predicted', marker='x')
            ax.set_title(f"Sample province: {meta_test[0]['province_name']} — Test Period")
            ax.legend(); plt.xticks(rotation=45)
        mlflow.log_figure(fig, "time_series_sample.png"); plt.close()


def _save_artifacts(scaler, clip_thresholds):
    with open("/tmp/scaler_lstm_v5.pkl", "wb") as f:
        pickle.dump(scaler, f)
    mlflow.log_artifact("/tmp/scaler_lstm_v5.pkl", "artifacts")

    config = {
        "hotness_lag_features":  HOTNESS_LAG_FEATURES,
        "hotel_lag_features":    HOTEL_LAG_FEATURES,
        "aspect_features":       ASPECT_FEATURES,
        "removed_features_v5_2": [
            "total_hotel_reviews (= target copy)",
            "hotness_score (current, use lags only)",
            "comments_per_post, engagement_score, social_to_booking_ratio (redundant)",
            "positive_ratio, negative_ratio, word count metrics, emoji sentiment (redundant/noise)",
            "aspect_transport, aspect_accommodation (sparse)",
            "high_score_ratio, couple_ratio, family_ratio, solo_ratio (multicollinear)",
            "sentiment_polarity_change, hotel_vol_std_rolling_3m (noise)",
        ],
        "fixes_applied": {
            "FIX1_train_val_test_split":   True,
            "FIX2_clip_leakage_fixed":     True,
            "FIX3_hotness_lag_added":      True,
            "FIX4_aspect_features_added":  True,
            "FIX5_ablation_study":         False,
            "FIX6_baselines":              False,
            "FIX7_scheduler_step_fixed":   True,
            "FIX8_growth_pct_fixed":       True,
            "FIX9_batchnorm_removed":      True,
        },
        "forecast_assumption":
            "Social/NLP/aspect features beyond lag window use persistence assumption "
            "(held at most recent known value). Hotel volume lags are updated "
            "autoregressively from model predictions.",
    }
    with open("/tmp/feature_config_v5.json", "w") as f:
        json.dump(config, f, indent=2)
    mlflow.log_artifact("/tmp/feature_config_v5.json", "artifacts")


# ============================================================
# Step 4: Forecast 12 months
# ============================================================

def _scale_value(raw, feat_idx, scaler):
    """Scale a single raw value using fitted RobustScaler."""
    s = scaler.scale_[feat_idx]
    return (raw - scaler.center_[feat_idx]) / s if s != 0 else 0.0


def create_forecast_table(spark):
    schema = StructType([
        StructField("province_sk",                    LongType(),   False),
        StructField("province_name",                  StringType(), False),
        StructField("region",                         StringType(), False),
        StructField("year",                           IntegerType(),False),
        StructField("month",                          IntegerType(),False),
        StructField("year_month",                     IntegerType(),False),
        StructField("horizon_month",                  IntegerType(),False),
        StructField("predicted_hotel_volume",     DoubleType(), True),
        StructField("predicted_hotel_volume_actual",  DoubleType(), True),
        StructField("predicted_growth_pct",           DoubleType(), True),
        StructField("forecast_date",                  StringType(), False),
        StructField("model_version",                  StringType(), False),
    ])
    create_iceberg_table_if_not_exists(
        spark=spark, database="gold",
        table_name="province_month_forecast_lstm_next12",
        schema=schema, partition_by=["year", "month"],
        table_properties={"format-version": "2",
                          "write.format.default": "parquet",
                          "write.parquet.compression-codec": "snappy"},
        catalog="gold"
    )


def forecast_12_months(spark, model, scaler, clip_thresholds, df_pd, device):
    print("\n" + "=" * 80)
    print("STEP 4: FORECASTING 12 MONTHS (autoregressive)")
    print("=" * 80)
    print("  NOTE: Social/NLP/aspect features beyond seq window use persistence")
    print("        assumption (most recent known value carried forward).")
    print("        Hotel volume lags are updated from model predictions.")

    create_forecast_table(spark)

    fi      = {f: ALL_FEATURES.index(f) for f in ALL_FEATURES}
    fi_seq  = {f: SEQ_FEATURES.index(f) for f in SEQ_FEATURES}
    results = []
    import math

    for province_sk, group in df_pd.groupby("province_sk"):
        group = group.sort_values("year_month")
        if len(group) < SEQUENCE_LENGTH:
            continue

        province_name = group["province_name"].iloc[-1]
        region        = group["region"].iloc[-1] if "region" in group.columns else "Unknown"

        last_seq      = group[SEQ_FEATURES].values[-SEQUENCE_LENGTH:].copy()
        last_ym       = int(group["year_month"].iloc[-1])
        last_date     = datetime.strptime(str(last_ym), '%Y%m')

        recent_volumes = list(np.expm1(group[TARGET].values[-max(SEQUENCE_LENGTH, 12):]))

        for horizon in range(1, FORECAST_MONTHS + 1):
            total_m = last_date.year * 12 + last_date.month + horizon
            ty  = (total_m - 1) // 12
            tm  = (total_m - 1) % 12 + 1
            tym = int(f"{ty:04d}{tm:02d}")

            # Aligned Target Lag: fetch the volume of the target month from 12 months ago
            if len(recent_volumes) >= 12:
                target_lag_raw = recent_volumes[-12]
            else:
                target_lag_raw = np.mean(recent_volumes)
            target_lag_scaled = _scale_value(target_lag_raw, fi[ANCHOR_FEATURE], scaler)
            target_lag_log = np.log1p(target_lag_raw)
            
            model.eval()
            with torch.no_grad():
                seq_tensor = torch.FloatTensor(last_seq).unsqueeze(0).to(device)
                scaled_tensor = torch.FloatTensor([[target_lag_scaled]]).to(device)
                log_tensor = torch.FloatTensor([[target_lag_log]]).to(device)
                
                pred_log    = model(seq_tensor, scaled_tensor, log_tensor).cpu().item()
                pred_log    = float(max(0.0, pred_log))
                pred_actual = float(np.expm1(pred_log))

            # Growth vs PREVIOUS step
            prev_volume = recent_volumes[-1] if recent_volumes else 0.0
            growth_pct  = float(
                (pred_actual - prev_volume) / prev_volume * 100.0
            ) if prev_volume > 0 else 0.0

            results.append({
                "province_sk":                   int(province_sk),
                "province_name":                 str(province_name),
                "region":                        str(region),
                "year":                          int(ty),
                "month":                         int(tm),
                "year_month":                    int(tym),
                "horizon_month":                 int(horizon),
                "predicted_hotel_volume":    pred_log,
                "predicted_hotel_volume_actual": pred_actual,
                "predicted_growth_pct":          growth_pct,
                "forecast_date":                 datetime.now().strftime('%Y-%m-%d'),
                "model_version":                 "lstm_v5",
            })

            recent_volumes.append(pred_actual)

            # --- Update sequence for next step ---
            new_row = last_seq[-1].copy()

            # Temporal
            new_row[fi_seq["month_sin"]] = _scale_value(math.sin(2 * math.pi * tm / 12), fi["month_sin"], scaler)
            new_row[fi_seq["month_cos"]] = _scale_value(math.cos(2 * math.pi * tm / 12), fi["month_cos"], scaler)

            # Hotel volume lags (autoregressive update)
            # hotel_vol_lag_12 is NOT in SEQ_FEATURES, so it is not updated here.
            if len(recent_volumes) >= 3:
                new_row[fi_seq["hotel_vol_rolling_3m"]] = _scale_value(
                    float(np.mean(recent_volumes[-3:])), fi["hotel_vol_rolling_3m"], scaler
                )
            lag1 = recent_volumes[-1] if len(recent_volumes) >= 1 else 0.0
            lag3 = recent_volumes[-3] if len(recent_volumes) >= 3 else lag1
            new_row[fi_seq["hotel_vol_momentum"]] = _scale_value(
                lag1 - lag3, fi["hotel_vol_momentum"], scaler
            )

            # hotel_vol_growth (clamp consistent with training preprocessing)
            prev_vol = recent_volumes[-2] if len(recent_volumes) >= 2 else 0.0
            growth   = (pred_actual - prev_vol) / prev_vol if prev_vol > 0 else 0.0
            growth   = max(-1.0, min(5.0, growth))
            new_row[fi_seq["hotel_vol_growth"]] = _scale_value(growth, fi["hotel_vol_growth"], scaler)

            # Hotness lags: persistence assumption
            # (no TikTok data for future → hold last known value)
            # These are kept as-is in new_row (already copied from last_seq[-1])

            last_seq = np.vstack([last_seq[1:], new_row])

    print(f"  Generated {len(results)} predictions ({FORECAST_MONTHS} months × provinces)")

    forecast_df = spark.createDataFrame(results)
    forecast_df.write.format("iceberg").mode("overwrite") \
        .save("gold.gold.province_month_forecast_lstm_next12")

    export_path = "s3a://gold/dl_forecast/province_hotel_volume_forecast_lstm_v5"

    forecast_df.coalesce(1).write.mode("overwrite").parquet(export_path)
    print(f"  Exported: {export_path}")
    forecast_df.orderBy("province_name", "year_month").show(10, truncate=False)

    return forecast_df


# ============================================================
# Main
# ============================================================

def main():
    print("\n" + "=" * 80)
    print("ML PIPELINE: PROVINCE HOTEL VOLUME FORECASTING (LSTM v5)")
    print("=" * 80)
    print(f"  Source:   {DL_FEATURES_TABLE}")
    print(f"  Features: {len(ALL_FEATURES)} total")
    print(f"  Split:    {int(TRAIN_RATIO*100)}/{int(VAL_RATIO*100)}/{int(TEST_RATIO*100)} (train/val/test)")
    print(f"  Fixes:    FIX1(val split) FIX2(clip leak) FIX3(hotness lag) "
          f"FIX4(aspect) FIX7(scheduler) "
          f"FIX8(growth) FIX9(bn)")
    print(f"  Start:    {datetime.now()}")
    print("=" * 80)

    spark = create_spark_session()

    try:
        df = load_features(spark)
        model, scaler, clip_thresholds, df_pd, device, best_config = train_model(df)
        forecast_12_months(spark, model, scaler, clip_thresholds, df_pd, device)

        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"  Model:                 {MODEL_NAME}")
        print(f"  Best Hyperparameters:  {best_config}")
        print(f"  Table:                 gold.gold.province_month_forecast_lstm_next12")
        print(f"  Features:              {len(ALL_FEATURES)} from {DL_FEATURES_TABLE}")
        print(f"  MLflow:                {MLFLOW_TRACKING_URI}")
        print(f"  End:                   {datetime.now()}")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
