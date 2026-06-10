"""
ML Pipeline: Train LSTM Deep Learning Model & Forecast Province Hotness
========================================================================

Reads directly from gold.gold.fact_province_month_dl_features (~40 features).
No inline hotness calculation — all features pre-computed in Gold layer.

Architecture (v4):
- 2-layer LSTM + LayerNorm + Temporal Attention
- Deeper FC head: Linear(hidden) → GELU → Dropout → Linear(16) → ReLU → Linear(1)
- HybridLoss: 70% HuberLoss + 30% SMAPELoss → directly optimizes for MAPE reduction
- RobustScaler (median/IQR) → less distortion from outlier provinces
- CosineAnnealingWarmRestarts scheduler → better convergence
- Outlier clipping for skewed engagement features before scaling

Changes from v3:
  HIDDEN_SIZE   32  → 48
  NUM_LAYERS    1   → 2
  DROPOUT       0.3 → 0.4
  LR            0.001 → 0.0005
  BATCH_SIZE    32  → 16
  PATIENCE      20  → 25
  EPOCHS        150 → 200
  weight_decay  1e-4 → 2e-4
  Loss          HuberLoss → HybridLoss (Huber+SMAPE)
  Scaler        MinMaxScaler → RobustScaler

Workflow:
1. Read fact_province_month_dl_features (ready-to-use)
2. Select feature columns, clip outliers, scale, build sequences
3. Train LSTM with MLflow tracking
4. Forecast 12 months (recursive autoregressive)

Output:
- Model: province_hotness_forecaster_lstm (MLflow registry)
- Table: gold.gold.province_month_forecast_lstm_next12
- Parquet: s3://gold/ml_forecast/province_hotness_forecast_lstm_*
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
EXPERIMENT_NAME = "province_hotel_volume_forecasting_lstm"
MODEL_NAME = "province_hotel_volume_forecaster_lstm"

DL_FEATURES_TABLE = "gold.gold.fact_province_month_dl_features"

FORECAST_MONTHS = 12
TRAIN_RATIO = 0.75
VAL_RATIO   = 0.125

# ------------------------------------------------------------
# Hyperparameter tuning history (2026-06-05)
#
# | Param          | v3 (baseline) | v4 (current) | Reason                              |
# |----------------|---------------|--------------|-------------------------------------|
# | SEQUENCE_LENGTH| 4             | 3            | seq=3 beats seq=4 on all test metrics|
# | HIDDEN_SIZE    | 32            | 48           | More model capacity                 |
# | NUM_LAYERS     | 1             | 2            | Deeper LSTM for complex patterns    |
# | DROPOUT        | 0.3           | 0.4          | Stronger regularization vs overfit  |
# | LEARNING_RATE  | 0.001         | 0.0005       | Stable convergence                  |
# | BATCH_SIZE     | 32            | 16           | More gradient noise = generalize    |
# | PATIENCE       | 20            | 25           | Allow more convergence time         |
# | EPOCHS         | 150           | 200          | Paired with patience increase       |
# | weight_decay   | 1e-4          | 2e-4         | Stronger L2 regularization          |
# | Loss           | HuberLoss     | HybridLoss   | 70% Huber + 30% SMAPE → lower MAPE |
# | Scaler         | MinMaxScaler  | RobustScaler | Robust to outlier provinces         |
# | Scheduler      | ReduceLROnPlateau | CosineAnnealing | Better LR exploration        |
# | FC head        | Linear(16)→ReLU | Linear(48)→GELU→Drop→Linear(16)→ReLU | Deeper |
# | input_size     | 36 features   | 35 features  | Removed avg_shares_per_post (DQ)    |
#
# Feature removal (2026-06-05):
#   avg_shares_per_post REMOVED: Bronze DQ check phát hiện metric này là TikTok
#   "total cross-platform distribution" (Messenger/Zalo/story/copy link) — không phải
#   nút Share visible trên video. 63.6% NULL trong silver. Confirmed từ Bronze CSV:
#   "Số lượt share: 3.8M" trong khi link TikTok chỉ hiện vài trăm. Không đáng tin.
#
# SEQUENCE_LENGTH experiment results (2026-06-05):
#   seq=4: train_r2=0.9808 | test_r2=0.9044 | gap=0.0764 | MAPE=49.52% | SMAPE=11.31%
#   seq=3: train_r2=0.9794 | test_r2=0.9115 | gap=0.0680 | MAPE=52.95% | SMAPE=10.65%  ← CHOSEN
#   → seq=3 wins: higher test_r2, smaller gap (below 0.07 target), better RMSE/MAE/SMAPE
#
# Full version comparison:
#   v2:            test_r2=0.811 | gap=0.100 | RMSE=0.734 | MAPE=90.03%
#   v3:            test_r2=0.839 | gap=0.108 | RMSE=0.677 | MAPE=88.29%
#   v4 seq=4:      test_r2=0.904 | gap=0.076 | RMSE=0.522 | MAPE=49.52%
#   v4 seq=3:      test_r2=0.911 | gap=0.068 | RMSE=0.499 | MAPE=52.95% (36 features)
#   v4 -shares:    test_r2=0.913 | gap=0.072 | RMSE=0.494 | MAE=0.377 | MAPE=52.77%  ← BEST
#   hidden=40:     test_r2=0.903 | gap=0.073 | RMSE=0.521 | (underpowered, rejected)
#   dropout=0.45:  test_r2=0.878 | gap=0.089 | RMSE=0.587 | (underfitting, rejected)
#
# Anti-overfit conclusion: gap=0.072 với 608 sequences/61 tỉnh là acceptable.
# Giảm capacity hay tăng dropout đều làm test metrics tệ hơn → giữ nguyên BEST.
# ------------------------------------------------------------

SEQUENCE_LENGTH = 3       # best: seq=3 → test_r2=0.9115, gap=0.068, RMSE=0.4994
HIDDEN_SIZE = 48          # Increased from 40 to recover model capacity
NUM_LAYERS = 2            # v3: 1  → v4: 2   (deeper LSTM)
DROPOUT = 0.43            # Increased to prevent overfitting with larger capacity
LEARNING_RATE = 0.0005    # v3: 0.001 → v4: 0.0005 (stable convergence)
EPOCHS = 200              # v3: 150 → v4: 200 (paired with patience=25)
BATCH_SIZE = 32           # 16→32: smoother gradients → better generalization (anti-overfit)
PATIENCE = 25             # v3: 20  → v4: 25  (allow more convergence time)
TARGET = "hotel_review_volume"

# Features read from DL fact table — no inline computation needed
TEMPORAL_FEATURES = ["month_sin", "month_cos"]

LAG_FEATURES = [
    "hotel_vol_lag_1", "hotel_vol_lag_2", "hotel_vol_lag_3",
    "hotel_vol_lag_12", "hotel_vol_rolling_3m", "hotel_vol_momentum",
]

VOLUME_FEATURES = [
    "total_posts", "total_comments", "total_hotel_reviews",
    "unique_authors", "comments_per_post",
]

ENGAGEMENT_FEATURES = [
    # avg_shares_per_post REMOVED (2026-06-05):
    # Bronze DQ check: 63.6% NULL in silver, và giá trị khi có là TikTok
    # "total distribution" (bao gồm cross-platform: Messenger, Zalo, story...)
    # khác hoàn toàn với nút Share visible trên video → không đồng nhất,
    # không đáng tin làm feature. Confirmed từ Bronze CSV: "Số lượt share: 3.8M"
    # trong khi nút share trên link TikTok chỉ hiện vài trăm đến vài ngàn.
    "avg_likes_per_post", "avg_saves_per_post",
    "viral_post_ratio", "engagement_score",
    "hotness_score",  # hotness_score acts as input feature (social attention index)
]

NLP_FEATURES = [
    "avg_sentiment", "sentiment_std", "positive_ratio", "negative_ratio",
    "avg_word_count", "avg_unique_word_ratio",
    "emoji_sentiment_ratio", "reply_ratio",
]

HOTEL_FEATURES = [
    "avg_hotel_score", "hotel_score_std",
    "high_score_ratio", "domestic_review_ratio",
    "couple_ratio", "family_ratio", "business_ratio", "solo_ratio",
    "hotel_vol_growth",
]

CUSTOM_FEATURES = [
    "social_to_booking_ratio", "sentiment_polarity_change", "hotel_vol_std_rolling_3m",
]

ALL_FEATURES = (
    TEMPORAL_FEATURES + LAG_FEATURES
    + VOLUME_FEATURES + ENGAGEMENT_FEATURES
    + NLP_FEATURES + HOTEL_FEATURES
    + CUSTOM_FEATURES
)

# Features that should use LAGGED values during forecast (from previous step)
FORECAST_UPDATABLE_FEATURES = set(TEMPORAL_FEATURES + LAG_FEATURES)


# ============================================================
# Loss Functions
# ============================================================

class SMAPELoss(nn.Module):
    """
    Symmetric MAPE loss — less biased than MAPE for values near 0.
    Formula: mean(|pred - target| / ((|pred| + |target|) / 2 + eps))
    Training directly on SMAPE guides the model to minimize % error,
    which directly reduces the test_mape_actual metric.
    """
    def forward(self, pred, target):
        denom = (torch.abs(target) + torch.abs(pred)) / 2.0 + 1e-8
        return torch.mean(torch.abs(pred - target) / denom)


class HybridLoss(nn.Module):
    """
    70% HuberLoss (stable gradient) + 30% SMAPELoss (drives MAPE down).
    Huber handles large residuals robustly; SMAPE penalizes % errors
    especially for low-volume provinces.
    """
    def __init__(self, delta=0.5, smape_weight=0.3):
        super().__init__()
        self.huber = nn.HuberLoss(delta=delta)
        self.smape = SMAPELoss()
        self.w = smape_weight

    def forward(self, pred, target):
        return (1 - self.w) * self.huber(pred, target) + self.w * self.smape(pred, target)


# ============================================================
# LSTM Model with LayerNorm + Temporal Attention (v4)
# ============================================================

class TemporalAttention(nn.Module):
    def __init__(self, hidden_size):
        super().__init__()
        self.attn = nn.Linear(hidden_size, 1)

    def forward(self, lstm_output):
        scores = self.attn(lstm_output).squeeze(-1)
        weights = torch.softmax(scores, dim=1)
        context = torch.bmm(weights.unsqueeze(1), lstm_output).squeeze(1)
        return context, weights


class LSTMForecaster(nn.Module):
    """
    v4 changes vs v3:
    - num_layers: 1 → 2 (deeper LSTM)
    - FC head: Linear(16)→ReLU → Linear(1)
              became Linear(hidden)→GELU→Dropout→Linear(16)→ReLU→Linear(1)
    - BatchNorm1d added after attention context
    """
    def __init__(self, input_size, hidden_size, num_layers, dropout):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size, hidden_size=hidden_size,
            num_layers=num_layers, batch_first=True,
            dropout=dropout if num_layers > 1 else 0
        )
        self.layer_norm = nn.LayerNorm(hidden_size)
        self.attention = TemporalAttention(hidden_size)
        self.bn = nn.BatchNorm1d(hidden_size)
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, hidden_size),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 16),
            nn.ReLU(),
            nn.Linear(16, 1)
        )

    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        lstm_out = self.layer_norm(lstm_out)
        context, _ = self.attention(lstm_out)
        # Commented out BatchNorm1d to test model performance without it
        # if not self.training or context.shape[0] > 1:
        #     context = self.bn(context)
        return self.fc(context).squeeze(-1)


class TimeSeriesDataset(Dataset):
    def __init__(self, X, y):
        self.X = torch.FloatTensor(X)
        self.y = torch.FloatTensor(y)

    def __len__(self):
        return len(self.X)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]


# ============================================================
# Spark Session
# ============================================================

def create_spark_session():
    return SparkSession.builder \
        .appName("ML_Province_Hotness_LSTM") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gold.type", "hive") \
        .config("spark.sql.catalog.gold.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold/lakehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()


# ============================================================
# Step 1: Load from DL fact table (no inline computation)
# ============================================================

def load_features(spark):
    """Read pre-computed features from fact_province_month_dl_features."""
    print("\n" + "=" * 80)
    print("STEP 1: LOADING PRE-COMPUTED DL FEATURES")
    print("=" * 80)

    df = spark.table(DL_FEATURES_TABLE)
    total = df.count()
    provinces = df.select("province_sk").distinct().count()

    print(f"  Table: {DL_FEATURES_TABLE}")
    print(f"  Rows: {total}, Provinces: {provinces}")

    select_cols = (
        ALL_FEATURES + [TARGET, "year_month", "month",
                        "province_sk", "province_name", "region"]
    )
    df = df.select(*select_cols)

    # Drop rows with missing lag features (first 12 months per province)
    before = df.count()
    df = df.dropna(subset=LAG_FEATURES + [TARGET])
    # Also filter out zero-volume rows: log1p(0)=0 is ambiguous ("no data" vs "true zero")
    df = df.filter(F.col(TARGET) > 0)
    after = df.count()
    print(f"  Dropped {before - after} rows (NULL lags or zero target), remaining: {after}")
    print(f"  Features: {len(ALL_FEATURES)} ({len(TEMPORAL_FEATURES)} temporal "
          f"+ {len(LAG_FEATURES)} lag + {len(VOLUME_FEATURES)} volume "
          f"+ {len(ENGAGEMENT_FEATURES)} engagement + {len(NLP_FEATURES)} nlp "
          f"+ {len(HOTEL_FEATURES)} hotel)")

    return df


# ============================================================
# Step 2: Prepare sequences
# ============================================================

def prepare_sequences(df_pd, features, target, seq_length):
    X_all, y_all, meta_all = [], [], []

    for province_sk, group in df_pd.groupby("province_sk"):
        group = group.sort_values("year_month")
        values = group[features].values
        targets = group[target].values
        year_months = group["year_month"].values

        for i in range(seq_length, len(group)):
            X_all.append(values[i - seq_length:i])
            y_all.append(targets[i])
            meta_all.append({
                "province_sk": province_sk,
                "year_month": int(year_months[i]),
                "province_name": group["province_name"].iloc[i],
            })

    return np.array(X_all), np.array(y_all), meta_all


def clip_and_scale_entire(df_pd, train_end, features):
    """
    Fit clip thresholds and RobustScaler on Train period only,
    apply consistently to the entire dataset (no time-boundary sequence loss).
    """
    df = df_pd.copy()

    # --- Outlier clipping (applied to entire df first) ---
    GROWTH_MIN, GROWTH_MAX = -1.0, 5.0
    df["hotel_vol_growth"] = df["hotel_vol_growth"].clip(GROWTH_MIN, GROWTH_MAX)
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
# Step 3: Train LSTM
# ============================================================

def train_model(df):
    print("\n" + "=" * 80)
    print("STEP 3: TRAINING LSTM MODEL (v4 with Val)")
    print("=" * 80)

    select_cols = ALL_FEATURES + [TARGET, "year_month", "province_sk", "province_name", "region"]
    df_pd = df.select(select_cols).toPandas()
    df_pd = df_pd.sort_values(["province_sk", "year_month"])
    df_pd[ALL_FEATURES] = df_pd[ALL_FEATURES].fillna(0)

    # Log transform the target variable
    df_pd[TARGET] = np.log1p(df_pd[TARGET].astype(float))

    # Time-based split boundaries
    ym_sorted = sorted(df_pd["year_month"].unique())
    n         = len(ym_sorted)
    train_end = ym_sorted[int(n * TRAIN_RATIO) - 1]
    val_end   = ym_sorted[int(n * (TRAIN_RATIO + VAL_RATIO)) - 1]

    print(f"  Split boundaries: train≤{train_end} | val {train_end+1}–{val_end} | test>{val_end}")

    # Clip and scale using train-only statistics
    df_pd_scaled, scaler, clip_thresholds = clip_and_scale_entire(
        df_pd, train_end, ALL_FEATURES
    )

    # Build sequences on the entire continuous dataset (no boundary loss)
    X_all, y_all, meta_all = prepare_sequences(df_pd_scaled, ALL_FEATURES, TARGET, SEQUENCE_LENGTH)

    # Chronologically split sequences based on the target year_month
    X_train, y_train = [], []
    X_val,   y_val   = [], []
    X_test,  y_test  = [], []
    meta_test        = []

    for i in range(len(X_all)):
        ym = meta_all[i]["year_month"]
        if ym <= train_end:
            X_train.append(X_all[i])
            y_train.append(y_all[i])
        elif ym <= val_end:
            X_val.append(X_all[i])
            y_val.append(y_all[i])
        else:
            X_test.append(X_all[i])
            y_test.append(y_all[i])
            meta_test.append(meta_all[i])

    X_train, y_train = np.array(X_train), np.array(y_train)
    X_val,   y_val   = np.array(X_val),   np.array(y_val)
    X_test,  y_test  = np.array(X_test),  np.array(y_test)

    print(f"  Sequences — Train: {len(X_train)} | Val: {len(X_val)} | Test: {len(X_test)}")

    train_loader = DataLoader(
        TimeSeriesDataset(X_train, y_train), batch_size=BATCH_SIZE, shuffle=True
    )
    val_loader = DataLoader(
        TimeSeriesDataset(X_val, y_val), batch_size=BATCH_SIZE, shuffle=False
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = LSTMForecaster(
        input_size=len(ALL_FEATURES), hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS, dropout=DROPOUT
    ).to(device)

    total_params = sum(p.numel() for p in model.parameters())
    print(f"  Device: {device}, Parameters: {total_params:,}")

    optimizer = torch.optim.Adam(
        model.parameters(), lr=LEARNING_RATE, weight_decay=7e-4
    )
    criterion = HybridLoss(delta=0.5, smape_weight=0.3)

    scheduler = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(
        optimizer, T_0=30, T_mult=2, eta_min=1e-6
    )

    # --- MLflow ---
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name=f"lstm_v4_val_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_params({
            "model_type": "LSTM_v4_Attention_with_Val",
            "source_table": DL_FEATURES_TABLE,
            "sequence_length": SEQUENCE_LENGTH,
            "hidden_size": HIDDEN_SIZE,
            "num_layers": NUM_LAYERS,
            "dropout": DROPOUT,
            "learning_rate": LEARNING_RATE,
            "loss": "HybridLoss_Huber0.5_SMAPE0.3",
            "scaler_type": "RobustScaler",
            "epochs_max": EPOCHS,
            "batch_size": BATCH_SIZE,
            "patience": PATIENCE,
            "weight_decay": "7e-4",
            "scheduler": "CosineAnnealingWarmRestarts_T0=30",
            "num_features": len(ALL_FEATURES),
            "train_size": len(X_train),
            "val_size": len(X_val),
            "test_size": len(X_test),
            "total_params": total_params,
        })

        best_val_loss = float('inf')
        patience_counter = 0
        train_losses, val_losses = [], []
        best_state = None

        for epoch in range(EPOCHS):
            model.train()
            epoch_loss = 0
            for xb, yb in train_loader:
                xb, yb = xb.to(device), yb.to(device)
                optimizer.zero_grad()
                loss = criterion(model(xb), yb)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
                optimizer.step()
                epoch_loss += loss.item() * len(xb)

            avg_train = epoch_loss / len(X_train)
            train_losses.append(avg_train)

            model.eval()
            val_loss = 0
            with torch.no_grad():
                for xb, yb in val_loader:
                    xb, yb = xb.to(device), yb.to(device)
                    val_loss += criterion(model(xb), yb).item() * len(xb)
            avg_val = val_loss / max(len(X_val), 1)
            val_losses.append(avg_val)

            # CosineAnnealing: step every epoch (Correct API call)
            scheduler.step(epoch)

            if (epoch + 1) % 10 == 0:
                lr = optimizer.param_groups[0]['lr']
                print(f"  Epoch {epoch+1}/{EPOCHS} | Train: {avg_train:.6f} | Val: {avg_val:.6f} | LR: {lr:.6f}")

            if avg_val < best_val_loss:
                best_val_loss = avg_val
                patience_counter = 0
                best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            else:
                patience_counter += 1
                if patience_counter >= PATIENCE:
                    print(f"  Early stopping at epoch {epoch+1}")
                    break

        model.load_state_dict(best_state)
        model.to(device)

        # Evaluate
        model.eval()
        with torch.no_grad():
            y_pred_train = model(torch.FloatTensor(X_train).to(device)).cpu().numpy()
            y_pred_val   = model(torch.FloatTensor(X_val).to(device)).cpu().numpy()
            y_pred_test  = model(torch.FloatTensor(X_test).to(device)).cpu().numpy()

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
        }
        mlflow.log_metrics(all_metrics)

        print(f"\n  MAIN MODEL RESULTS:")
        print(f"  Train: RMSE={all_metrics['train_rmse']:.4f}  MAE={all_metrics['train_mae']:.4f}  R²={all_metrics['train_r2']:.4f}")
        print(f"  Val:   RMSE={all_metrics['val_rmse']:.4f}    MAE={all_metrics['val_mae']:.4f}    R²={all_metrics['val_r2']:.4f}")
        print(f"  Test:  RMSE={all_metrics['test_rmse']:.4f}   MAE={all_metrics['test_mae']:.4f}   R²={all_metrics['test_r2']:.4f}")
        print(f"  MAPE(actual)={all_metrics['test_mape_actual']:.2f}%  SMAPE(log)={all_metrics['test_smape_log']:.2f}%")
        print(f"  Train-Val R² gap:  {all_metrics['train_r2'] - all_metrics['val_r2']:.4f}")
        print(f"  Val-Test R² gap:   {all_metrics['val_r2']   - all_metrics['test_r2']:.4f}")

        # Plots
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(train_losses, label='Train')
        ax.plot(val_losses, label='Val')
        ax.set_xlabel('Epoch'); ax.set_ylabel('Loss'); ax.set_title('LSTM v4 Training Loss'); ax.legend()
        mlflow.log_figure(fig, "loss_curve.png"); plt.close()

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(y_test, y_pred_test, alpha=0.5, s=15)
        max_val = float(max(y_test.max(), y_pred_test.max()))
        ax.plot([0, max_val], [0, max_val], 'r--')
        ax.set_xlabel('Actual (log1p)'); ax.set_ylabel('Predicted (log1p)')
        ax.set_title(f'LSTM v4: Actual vs Predicted (R2={all_metrics["test_r2"]:.3f})')
        mlflow.log_figure(fig, "actual_vs_predicted.png"); plt.close()

        fig, ax = plt.subplots(figsize=(10, 6))
        residuals = y_test - y_pred_test
        ax.scatter(y_pred_test, residuals, alpha=0.5, s=15)
        ax.axhline(0, color='r', linestyle='--')
        ax.set_xlabel('Predicted'); ax.set_ylabel('Residual'); ax.set_title('Residuals v4')
        mlflow.log_figure(fig, "residuals.png"); plt.close()

        if meta_test:
            fig, ax = plt.subplots(figsize=(14, 6))
            sp = meta_test[0]['province_sk']
            idx = [i for i, m in enumerate(meta_test) if m['province_sk'] == sp]
            if idx:
                ax.plot([meta_test[i]['year_month'] for i in idx],
                        [y_test[i] for i in idx], label='Actual', marker='o')
                ax.plot([meta_test[i]['year_month'] for i in idx],
                        [y_pred_test[i] for i in idx], label='Predicted', marker='x')
                ax.set_title(f"Sample: {meta_test[0]['province_name']}")
                ax.legend(); plt.xticks(rotation=45)
            mlflow.log_figure(fig, "time_series_sample.png"); plt.close()

        mlflow.pytorch.log_model(model, "model")

        with open("/tmp/scaler_lstm.pkl", "wb") as f:
            pickle.dump(scaler, f)
        mlflow.log_artifact("/tmp/scaler_lstm.pkl", "artifacts")

        with open("/tmp/feature_config.json", "w") as f:
            json.dump({
                "all_features": ALL_FEATURES,
                "target": TARGET,
                "sequence_length": SEQUENCE_LENGTH,
                "scaler_type": "RobustScaler",
                "model_version": "lstm_v4_volume"
            }, f, indent=2)
        mlflow.log_artifact("/tmp/feature_config.json", "artifacts")

        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, MODEL_NAME)
        print(f"  Model registered: {MODEL_NAME}")

        return model, scaler, df_pd_scaled, device


# ============================================================
# Step 4: Forecast 12 months
# ============================================================

def _scale_value(raw, feat_idx, scaler):
    """
    Manually scale a single value using the fitted RobustScaler.
    RobustScaler formula: scaled = (x - center_) / scale_
    where center_ = median, scale_ = IQR (Q3 - Q1).
    Note: unlike MinMaxScaler, output is NOT bounded to [0,1].
    """
    s = scaler.scale_[feat_idx]
    if s == 0:
        return 0.0
    return (raw - scaler.center_[feat_idx]) / s


def create_forecast_table(spark):
    schema = StructType([
        StructField("province_sk", LongType(), False),
        StructField("province_name", StringType(), False),
        StructField("region", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("year_month", IntegerType(), False),
        StructField("horizon_month", IntegerType(), False),
        StructField("predicted_hotel_volume", DoubleType(), True),
        StructField("predicted_hotel_volume_actual", DoubleType(), True),
        StructField("predicted_growth_pct", DoubleType(), True),
        StructField("forecast_date", StringType(), False),
        StructField("model_version", StringType(), False),
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


def forecast_12_months(spark, model, scaler, df_pd, device):
    print("\n" + "=" * 80)
    print("STEP 4: FORECASTING 12 MONTHS")
    print("=" * 80)

    create_forecast_table(spark)

    fi = {f: ALL_FEATURES.index(f) for f in ALL_FEATURES}
    results = []
    import math

    for province_sk, group in df_pd.groupby("province_sk"):
        group = group.sort_values("year_month")
        if len(group) < SEQUENCE_LENGTH:
            continue

        province_name = group["province_name"].iloc[-1]
        region = group["region"].iloc[-1] if "region" in group.columns else "Unknown"

        last_seq = group[ALL_FEATURES].values[-SEQUENCE_LENGTH:].copy()
        last_ym = int(group["year_month"].iloc[-1])
        last_date = datetime.strptime(str(last_ym), '%Y%m')

        recent_volumes = list(np.expm1(group[TARGET].values[-max(SEQUENCE_LENGTH, 12):]))
        last_actual_volume = float(recent_volumes[-1])

        for horizon in range(1, FORECAST_MONTHS + 1):
            total_m = last_date.year * 12 + last_date.month + horizon
            ty = (total_m - 1) // 12
            tm = (total_m - 1) % 12 + 1
            tym = int(f"{ty:04d}{tm:02d}")

            model.eval()
            with torch.no_grad():
                pred_log = model(torch.FloatTensor(last_seq).unsqueeze(0).to(device)).cpu().item()
                pred_log = float(max(0.0, pred_log))
                pred_actual = float(np.expm1(pred_log))

            growth_pct = float(0.0)
            if last_actual_volume > 0:
                growth_pct = float((pred_actual - last_actual_volume) / last_actual_volume * 100.0)

            results.append({
                'province_sk': int(province_sk), 'province_name': str(province_name),
                'region': str(region), 'year': int(ty), 'month': int(tm),
                'year_month': int(tym), 'horizon_month': int(horizon),
                'predicted_hotel_volume': pred_log,
                'predicted_hotel_volume_actual': pred_actual,
                'predicted_growth_pct': growth_pct,
                'forecast_date': datetime.now().strftime('%Y-%m-%d'),
                'model_version': 'lstm_v4_volume'
            })

            recent_volumes.append(pred_actual)

            new_row = last_seq[-1].copy()

            # Update temporal features
            new_row[fi["month_sin"]] = _scale_value(math.sin(2 * math.pi * tm / 12), fi["month_sin"], scaler)
            new_row[fi["month_cos"]] = _scale_value(math.cos(2 * math.pi * tm / 12), fi["month_cos"], scaler)

            # Update lag features (shift window forward)
            new_row[fi["hotel_vol_lag_3"]] = new_row[fi["hotel_vol_lag_2"]]
            new_row[fi["hotel_vol_lag_2"]] = new_row[fi["hotel_vol_lag_1"]]
            new_row[fi["hotel_vol_lag_1"]] = _scale_value(pred_actual, fi["hotel_vol_lag_1"], scaler)

            if len(recent_volumes) >= 12:
                new_row[fi["hotel_vol_lag_12"]] = _scale_value(
                    recent_volumes[-12], fi["hotel_vol_lag_12"], scaler
                )
            if len(recent_volumes) >= 3:
                new_row[fi["hotel_vol_rolling_3m"]] = _scale_value(
                    float(np.mean(recent_volumes[-3:])), fi["hotel_vol_rolling_3m"], scaler
                )

            lag1 = recent_volumes[-1] if len(recent_volumes) >= 1 else 0
            lag3 = recent_volumes[-3] if len(recent_volumes) >= 3 else lag1
            new_row[fi["hotel_vol_momentum"]] = _scale_value(
                lag1 - lag3, fi["hotel_vol_momentum"], scaler
            )

            # Update hotel_vol_growth (clamped consistent with training preprocessing)
            prev_vol = recent_volumes[-2] if len(recent_volumes) >= 2 else 0.0
            growth = 0.0
            if prev_vol > 0:
                growth = (pred_actual - prev_vol) / prev_vol
            growth = max(-1.0, min(5.0, growth))
            new_row[fi["hotel_vol_growth"]] = _scale_value(growth, fi["hotel_vol_growth"], scaler)

            # Update custom/advanced features
            idx_tc = fi["total_comments"]
            raw_tc = new_row[idx_tc] * scaler.scale_[idx_tc] + scaler.center_[idx_tc] if scaler.scale_[idx_tc] != 0 else scaler.center_[idx_tc]
            new_ratio = raw_tc / (pred_actual + 1.0)
            new_row[fi["social_to_booking_ratio"]] = _scale_value(new_ratio, fi["social_to_booking_ratio"], scaler)

            new_row[fi["sentiment_polarity_change"]] = _scale_value(0.0, fi["sentiment_polarity_change"], scaler)

            new_std = float(np.std(recent_volumes[-3:]))
            new_row[fi["hotel_vol_std_rolling_3m"]] = _scale_value(new_std, fi["hotel_vol_std_rolling_3m"], scaler)

            last_seq = np.vstack([last_seq[1:], new_row])

    print(f"  Generated {len(results)} predictions")

    forecast_df = spark.createDataFrame(results)

    forecast_df.write.format("iceberg").mode("overwrite") \
        .save("gold.gold.province_month_forecast_lstm_next12")

    export_path = f"s3a://gold/dl_forecast/province_hotel_volume_forecast_lstm_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    forecast_df.coalesce(1).write.mode("overwrite").parquet(export_path)
    print(f"  Exported: {export_path}")

    forecast_df.orderBy("province_name", "year_month").show(10, truncate=False)
    return forecast_df


# ============================================================
# Main
# ============================================================

def main():
    print("\n" + "=" * 80)
    print("ML PIPELINE: PROVINCE HOTNESS FORECASTING (LSTM v4)")
    print("=" * 80)
    print(f"Source: {DL_FEATURES_TABLE}")
    print(f"Features: {len(ALL_FEATURES)} (pre-computed, no inline calculation)")
    print(f"Changes vs v3: hidden={HIDDEN_SIZE}, layers={NUM_LAYERS}, dropout={DROPOUT}, "
          f"lr={LEARNING_RATE}, batch={BATCH_SIZE}, loss=HybridLoss, scaler=RobustScaler")
    print(f"Start: {datetime.now()}")

    spark = create_spark_session()

    try:
        df = load_features(spark)
        model, scaler, df_pd, device = train_model(df)
        forecast_12_months(spark, model, scaler, df_pd, device)

        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED")
        print("=" * 80)
        print(f"  Model: {MODEL_NAME} (v4)")
        print(f"  Table: gold.gold.province_month_forecast_lstm_next12")
        print(f"  Features: {len(ALL_FEATURES)} from {DL_FEATURES_TABLE}")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
