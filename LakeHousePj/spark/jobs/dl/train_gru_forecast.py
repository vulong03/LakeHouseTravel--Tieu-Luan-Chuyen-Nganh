"""
ML Pipeline: Train GRU Deep Learning Model & Forecast Province Hotel Volume
=============================================================================

Source table: gold.gold.fact_province_month_dl_features

Architecture:
- 1 or 2-layer GRU + LayerNorm + Temporal Attention (tuned via Optuna)
- Deeper FC head: Linear(hidden) → GELU → Dropout → Linear(16) → ReLU → Linear(1)
- HybridLoss: 70% HuberLoss + 30% SMAPELoss
- RobustScaler (applied to continuous features; temporal/ratio/sentiment excluded)
- CosineAnnealingWarmRestarts scheduler

Features: 39 total
  Temporal (2): month_sin, month_cos
  Hotel lag (3): hotel_vol_lag_12, hotel_vol_rolling_3m, hotel_vol_momentum
  Hotness lag (3): hotness_lag_12, hotness_rolling_3m, hotness_momentum
  Volume (3): total_posts, total_comments, comments_per_post
  Engagement (4): avg_likes_per_post, avg_saves_per_post, viral_post_ratio, engagement_score
  NLP (6): avg_sentiment, sentiment_std, positive_ratio, negative_ratio,
           emoji_sentiment_ratio, reply_ratio
  Aspect (6): avg_aspect_scenery, avg_aspect_food, avg_aspect_price,
              avg_aspect_service, avg_aspect_transport, avg_aspect_accommodation
  Hotel quality (9): avg_hotel_score, hotel_score_std, high_score_ratio,
                     domestic_review_ratio, couple_ratio, family_ratio,
                     business_ratio, solo_ratio, hotel_vol_growth
  Custom (3): social_to_booking_ratio, sentiment_polarity_change, hotel_vol_std_rolling_3m

Output:
- Model: province_hotel_volume_forecaster_gru_v5 (MLflow registry)
- Table: gold.gold.province_month_forecast_gru_next12
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

optuna_available = False
import sys
import os
LIB_PATH = "/tmp/pip_packages"
if LIB_PATH not in sys.path:
    sys.path.append(LIB_PATH)

try:
    import optuna
    optuna_available = True
except ImportError:
    import subprocess
    try:
        print(f"  Optuna not found. Trying to install via pip to {LIB_PATH}...")
        os.makedirs(LIB_PATH, exist_ok=True)
        subprocess.check_call([
            sys.executable, "-m", "pip", "install",
            f"--target={LIB_PATH}", "optuna"
        ])
        import optuna
        optuna_available = True
        print("  Optuna installed successfully.")
    except Exception as e:
        print(f"  WARNING: Failed to install optuna ({e}). Fallback to hardcoded hyperparameters.")

# ============================================================
# Configuration
# ============================================================

MLFLOW_TRACKING_URI = "http://mlflow:5000"
EXPERIMENT_NAME     = "province_hotel_volume_forecasting_gru"
MODEL_NAME          = "province_hotel_volume_forecaster_gru_v5"

DL_FEATURES_TABLE = "gold.gold.fact_province_month_dl_features"

FORECAST_MONTHS = 12

# 3-way time-based split (train/val/test)
# 75% train → 12.5% val → 12.5% test
TRAIN_RATIO = 0.75
VAL_RATIO   = 0.125
TEST_RATIO  = 1.0 - TRAIN_RATIO - VAL_RATIO

# Default/Fallback hyperparameters (aligned with v4 configurations)
SEQUENCE_LENGTH = 3
DEFAULT_HIDDEN_SIZE   = 48
DEFAULT_NUM_LAYERS    = 2
DEFAULT_DROPOUT       = 0.43
DEFAULT_LEARNING_RATE = 0.0005
DEFAULT_WEIGHT_DECAY  = 7e-4

EPOCHS          = 200
BATCH_SIZE      = 32
PATIENCE        = 25

# Fourier Seasonality Configuration
FOURIER_K = 1  # Set to 1 (baseline sin/cos), 2, 3 or higher to add harmonics

# Hyperparameter Search Space for Optuna
TUNING_EPOCHS   = 150
TUNING_PATIENCE = 20
N_TRIALS        = 100

TARGET = "hotel_review_volume"

# ============================================================
# Feature Groups
# ============================================================

TEMPORAL_FEATURES = []
for k in range(1, FOURIER_K + 1):
    if k == 1:
        TEMPORAL_FEATURES.extend(["month_sin", "month_cos"])
    else:
        TEMPORAL_FEATURES.extend([f"month_sin_k{k}", f"month_cos_k{k}"])

HOTEL_LAG_FEATURES = [
    "hotel_vol_lag_12", "hotel_vol_rolling_3m", "hotel_vol_momentum",
]

HOTNESS_LAG_FEATURES = [
    "hotness_lag_12", "hotness_rolling_3m", "hotness_momentum",
]

VOLUME_FEATURES = [
    "total_posts", "total_comments",
    "comments_per_post",
]

ENGAGEMENT_FEATURES = [
    "avg_likes_per_post", "avg_saves_per_post",
    "viral_post_ratio", "engagement_score",
]

NLP_FEATURES = [
    "avg_sentiment", "sentiment_std", "positive_ratio", "negative_ratio",
    "emoji_sentiment_ratio", "reply_ratio",
]

ASPECT_FEATURES = [
    "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price",
    "avg_aspect_service", "avg_aspect_transport", "avg_aspect_accommodation",
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

# Full feature set (39 features)
ALL_FEATURES = (
    TEMPORAL_FEATURES
    + HOTEL_LAG_FEATURES
    + HOTNESS_LAG_FEATURES
    + VOLUME_FEATURES
    + ENGAGEMENT_FEATURES
    + NLP_FEATURES
    + ASPECT_FEATURES
    + HOTEL_FEATURES
    + CUSTOM_FEATURES
)

UNSCALED_FEATURES = [
    "viral_post_ratio",
    "avg_sentiment", "sentiment_std", "positive_ratio", "negative_ratio", "emoji_sentiment_ratio", "reply_ratio",
    "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price", "avg_aspect_service", "avg_aspect_transport", "avg_aspect_accommodation",
    "high_score_ratio", "domestic_review_ratio", "couple_ratio", "family_ratio", "business_ratio", "solo_ratio",
    "sentiment_polarity_change"
] + TEMPORAL_FEATURES

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
# GRU Model (v5 — removed unused BatchNorm, FIX 9)
# ============================================================

class TemporalAttention(nn.Module):
    def __init__(self, hidden_size):
        super().__init__()
        self.attn = nn.Linear(hidden_size, 1)

    def forward(self, gru_output):
        scores  = self.attn(gru_output).squeeze(-1)
        weights = torch.softmax(scores, dim=1)
        context = torch.bmm(weights.unsqueeze(1), gru_output).squeeze(1)
        return context, weights


class GRUForecaster(nn.Module):
    """
    2-layer GRU + LayerNorm + Temporal Attention + deep FC head.
    """
    def __init__(self, input_size, hidden_size, num_layers, dropout):
        super().__init__()
        self.gru = nn.GRU(
            input_size=input_size, hidden_size=hidden_size,
            num_layers=num_layers, batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0
        )
        self.layer_norm = nn.LayerNorm(hidden_size)
        self.attention  = TemporalAttention(hidden_size)
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, hidden_size),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 16),
            nn.ReLU(),
            nn.Linear(16, 1)
        )

    def forward(self, x):
        gru_out, _  = self.gru(x)
        gru_out     = self.layer_norm(gru_out)
        context, _   = self.attention(gru_out)
        return self.fc(context).squeeze(-1)


class TimeSeriesDataset(Dataset):
    def __init__(self, X, y):
        self.X = torch.FloatTensor(X)
        self.y = torch.FloatTensor(y)

    def __len__(self):          return len(self.X)
    def __getitem__(self, idx): return self.X[idx], self.y[idx]


# ============================================================
# Spark Session
# ============================================================

def create_spark_session():
    return SparkSession.builder \
        .appName("ML_Province_HotelVolume_GRU_v5") \
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
    df = df.filter(F.col(TARGET) >= 0)
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

    # --- RobustScaler: fit on train only (excluding unscaled features) ---
    features_to_scale = [f for f in features if f not in UNSCALED_FEATURES]
    scaler = RobustScaler()
    scaler.fit(train_raw[features_to_scale])
    df[features_to_scale] = scaler.transform(df[features_to_scale])

    return df, scaler, clip_thresholds


def prepare_sequences(df_pd, features, target, seq_length):
    X_all, y_all, meta_all = [], [], []
    for province_sk, group in df_pd.groupby("province_sk"):
        group  = group.sort_values("year_month")
        values = group[features].values
        targets= group[target].values
        yms    = group["year_month"].values
        for i in range(seq_length, len(group)):
            X_all.append(values[i - seq_length:i])
            y_all.append(targets[i])
            meta_all.append({
                "province_sk":   province_sk,
                "year_month":    int(yms[i]),
                "province_name": group["province_name"].iloc[i],
            })
    return np.array(X_all), np.array(y_all), meta_all


def compute_metrics(y_true, y_pred, prefix=""):
    """Compute metrics on both log scale and actual scale consistently."""
    # 1. Log scale metrics (GRU target scale)
    rmse_log  = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae_log   = float(mean_absolute_error(y_true, y_pred))
    r2_log    = float(r2_score(y_true, y_pred))
    smape_log = float(
        2 * np.mean(np.abs(y_true - y_pred) /
                    (np.abs(y_true) + np.abs(y_pred) + 1e-8)) * 100
    )

    # 2. Actual scale metrics (expm1 conversion)
    y_true_actual = np.expm1(y_true)
    y_pred_actual = np.expm1(np.clip(y_pred, 0.0, None))

    rmse_actual = float(np.sqrt(mean_squared_error(y_true_actual, y_pred_actual)))
    mae_actual  = float(mean_absolute_error(y_true_actual, y_pred_actual))
    r2_actual   = float(r2_score(y_true_actual, y_pred_actual))

    # WAPE (Weighted Absolute Percentage Error)
    wape_actual = float(
        (np.sum(np.abs(y_true_actual - y_pred_actual)) / (np.sum(y_true_actual) + 1e-8)) * 100
    )

    # MAPE (only computed on actual values > 0)
    mask = y_true_actual > 0
    mape_actual = float(np.mean(np.abs(
        (y_true_actual[mask] - y_pred_actual[mask]) / y_true_actual[mask]
    )) * 100) if np.sum(mask) > 0 else 0.0

    # SMAPE on actual scale
    smape_actual = float(
        2 * np.mean(np.abs(y_true_actual - y_pred_actual) /
                    (np.abs(y_true_actual) + np.abs(y_pred_actual) + 1e-8)) * 100
    )

    return {
        f"{prefix}rmse_log":      rmse_log,
        f"{prefix}mae_log":       mae_log,
        f"{prefix}r2_log":        r2_log,
        f"{prefix}smape_log":     smape_log,

        f"{prefix}rmse_actual":   rmse_actual,
        f"{prefix}mae_actual":    mae_actual,
        f"{prefix}r2_actual":     r2_actual,
        f"{prefix}wape_actual":   wape_actual,
        f"{prefix}mape_actual":   mape_actual,
        f"{prefix}smape_actual":  smape_actual,
    }


# ============================================================
# Step 3b: Single GRU training run (reusable)
# ============================================================

def train_single_gru(X_train, y_train, X_val, y_val, X_test, y_test,
                      input_size, run_name, device,
                      hidden_size, num_layers, dropout, learning_rate, weight_decay,
                      epochs=EPOCHS, patience=PATIENCE):
    """
    Train a single GRU run.
    """
    model = GRUForecaster(
        input_size=input_size, hidden_size=hidden_size,
        num_layers=num_layers, dropout=dropout
    ).to(device)

    total_params = sum(p.numel() for p in model.parameters())

    optimizer  = torch.optim.Adam(model.parameters(),
                                  lr=learning_rate, weight_decay=weight_decay)
    criterion  = HybridLoss(delta=0.5, smape_weight=0.3)
    scheduler  = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(
        optimizer, T_0=30, T_mult=2, eta_min=1e-6
    )

    train_loader = DataLoader(TimeSeriesDataset(X_train, y_train),
                              batch_size=BATCH_SIZE, shuffle=True)
    val_loader   = DataLoader(TimeSeriesDataset(X_val,   y_val),
                              batch_size=BATCH_SIZE, shuffle=False)

    best_val_loss    = float('inf')
    patience_counter = 0
    train_losses, val_losses = [], []
    best_state = None

    for epoch in range(epochs):
        # --- Train ---
        model.train()
        epoch_loss = 0.0
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

        # --- Validate ---
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for xb, yb in val_loader:
                xb, yb = xb.to(device), yb.to(device)
                val_loss += criterion(model(xb), yb).item() * len(xb)
        avg_val = val_loss / max(len(X_val), 1)
        val_losses.append(avg_val)

        scheduler.step()

        if (epoch + 1) % 20 == 0:
            lr = optimizer.param_groups[0]['lr']
            print(f"    [{run_name}] Epoch {epoch+1}/{epochs} | "
                  f"Train: {avg_train:.5f} | Val: {avg_val:.5f} | LR: {lr:.6f}")

        # Early stopping on VAL
        if avg_val < best_val_loss:
            best_val_loss    = avg_val
            patience_counter = 0
            best_state       = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            patience_counter += 1
            if patience_counter >= patience:
                print(f"    [{run_name}] Early stopping at epoch {epoch+1}")
                break

    # Load best checkpoint
    model.load_state_dict(best_state)
    model.to(device)

    # --- Evaluate on TEST ---
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
        "total_params":   total_params,
        "input_size":     input_size,
    }

    return model, all_metrics, train_losses, val_losses, y_pred_test


def tune_hyperparameters(X_train, y_train, X_val, y_val, X_test, y_test, input_size, device):
    print("\n" + "=" * 80)
    print("HYPERPARAMETER TUNING WITH OPTUNA")
    print("=" * 80)

    optuna.logging.set_verbosity(optuna.logging.WARNING)

    def objective(trial):
        hidden_size   = trial.suggest_categorical("hidden_size", [16, 24, 32, 48, 64, 96, 128])
        num_layers    = trial.suggest_categorical("num_layers", [1, 2])
        dropout       = trial.suggest_float("dropout", 0.0, 0.6)
        learning_rate = trial.suggest_float("learning_rate", 1e-4, 1e-2, log=True)
        weight_decay  = trial.suggest_float("weight_decay", 1e-5, 1e-2, log=True)

        with mlflow.start_run(run_name=f"trial_{trial.number}", nested=True):
            mlflow.log_params({
                "hidden_size":   hidden_size,
                "num_layers":    num_layers,
                "dropout":       dropout,
                "learning_rate": learning_rate,
                "weight_decay":  weight_decay,
                "stage":         "tuning"
            })

            _, metrics, _, _, _ = train_single_gru(
                X_train, y_train, X_val, y_val, X_test, y_test,
                input_size=input_size,
                run_name=f"trial_{trial.number}",
                device=device,
                hidden_size=hidden_size,
                num_layers=num_layers,
                dropout=dropout,
                learning_rate=learning_rate,
                weight_decay=weight_decay,
                epochs=TUNING_EPOCHS,
                patience=TUNING_PATIENCE
            )

            mlflow.log_metrics({
                "val_loss": metrics["best_val_loss"],
                "val_rmse": metrics["val_rmse_log"],
                "val_mae":  metrics["val_mae_log"],
                "val_r2":   metrics["val_r2_log"]
            })

            print(f"    [Trial {trial.number}] Finished | val_loss: {metrics['best_val_loss']:.5f} | params: {trial.params}")

            return metrics["best_val_loss"]

    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=N_TRIALS)

    print("\n" + "=" * 80)
    print("TUNING COMPLETED")
    print(f"  Best trial: #{study.best_trial.number}")
    print(f"  Best Val Loss: {study.best_value:.5f}")
    best_params = study.best_params.copy()
    print(f"  Best Hyperparameters: {best_params}")
    print("=" * 80)

    return best_params


# ============================================================
# Step 3c: Main GRU training
# ============================================================

def train_model(df):
    print("\n" + "=" * 80)
    print("STEP 3: TRAINING GRU MODEL")
    print("=" * 80)

    select_cols = ALL_FEATURES + [TARGET, "year_month", "month", "province_sk", "province_name", "region"]
    df_pd       = df.select(select_cols).toPandas()
    df_pd       = df_pd.sort_values(["province_sk", "year_month"])
    df_pd[ALL_FEATURES] = df_pd[ALL_FEATURES].fillna(0)

    # Dynamically calculate Fourier seasonality features in Pandas
    for k in range(1, FOURIER_K + 1):
        if k == 1:
            df_pd["month_sin"] = np.sin(2 * np.pi * df_pd["month"] / 12.0)
            df_pd["month_cos"] = np.cos(2 * np.pi * df_pd["month"] / 12.0)
        else:
            df_pd[f"month_sin_k{k}"] = np.sin(2 * np.pi * k * df_pd["month"] / 12.0)
            df_pd[f"month_cos_k{k}"] = np.cos(2 * np.pi * k * df_pd["month"] / 12.0)

    df_pd[TARGET] = np.log1p(df_pd[TARGET].astype(float))

    # Determine split boundaries (chronological)
    ym_sorted = sorted(df_pd["year_month"].unique())
    n         = len(ym_sorted)
    train_end = ym_sorted[int(n * TRAIN_RATIO) - 1]
    val_end   = ym_sorted[int(n * (TRAIN_RATIO + VAL_RATIO)) - 1]

    print(f"  Split boundaries: train≤{train_end} | val {train_end+1}–{val_end} | test>{val_end}")

    # Clip and scale the entire dataframe using train-derived parameters
    df_pd_scaled, scaler, clip_thresholds = clip_and_scale_entire(
        df_pd, train_end, ALL_FEATURES
    )

    # Build sequences for the entire dataset
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

    if len(X_train) == 0:
        raise ValueError("Training set is empty after sequence preparation. "
                         "Check TRAIN_RATIO and data size.")

    print(f"\n  Sequences — Train: {len(X_train)} | Val: {len(X_val)} | Test: {len(X_test)}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"  Device: {device}")

    # -------------------------------------------------------
    # Main GRU run
    # -------------------------------------------------------
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    best_hparams = {
        "hidden_size":   DEFAULT_HIDDEN_SIZE,
        "num_layers":    DEFAULT_NUM_LAYERS,
        "dropout":       DEFAULT_DROPOUT,
        "learning_rate": DEFAULT_LEARNING_RATE,
        "weight_decay":  DEFAULT_WEIGHT_DECAY
    }

    if optuna_available:
        print("\n  Starting hyperparameter tuning parent run...")
        with mlflow.start_run(run_name=f"gru_v5_tuning_parent_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            best_hparams = tune_hyperparameters(
                X_train, y_train, X_val, y_val, X_test, y_test,
                input_size=len(ALL_FEATURES),
                device=device
            )
            mlflow.log_params({
                "best_" + k: v for k, v in best_hparams.items()
            })
            print(f"  Tuning complete. Best parameters to train: {best_hparams}")

    print("\n  Training main GRU...")
    with mlflow.start_run(run_name=f"gru_v5_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as main_run:

        model, metrics, train_losses, val_losses, y_pred_test = train_single_gru(
            X_train, y_train, X_val, y_val, X_test, y_test,
            input_size=len(ALL_FEATURES),
            run_name="full",
            device=device,
            hidden_size=best_hparams["hidden_size"],
            num_layers=best_hparams["num_layers"],
            dropout=best_hparams["dropout"],
            learning_rate=best_hparams["learning_rate"],
            weight_decay=best_hparams["weight_decay"],
            epochs=EPOCHS,
            patience=PATIENCE
        )

        print(f"\n  MAIN MODEL RESULTS:")
        print(f"  [LOG SCALE]")
        print(f"    Train: RMSE={metrics['train_rmse_log']:.4f}  MAE={metrics['train_mae_log']:.4f}  R²={metrics['train_r2_log']:.4f}  SMAPE={metrics['train_smape_log']:.2f}%")
        print(f"    Val:   RMSE={metrics['val_rmse_log']:.4f}    MAE={metrics['val_mae_log']:.4f}    R²={metrics['val_r2_log']:.4f}  SMAPE={metrics['val_smape_log']:.2f}%")
        print(f"    Test:  RMSE={metrics['test_rmse_log']:.4f}   MAE={metrics['test_mae_log']:.4f}   R²={metrics['test_r2_log']:.4f}  SMAPE={metrics['test_smape_log']:.2f}%")
        print(f"  [ACTUAL SCALE]")
        print(f"    Train: RMSE={metrics['train_rmse_actual']:.2f}  MAE={metrics['train_mae_actual']:.2f}  R²={metrics['train_r2_actual']:.4f}  WAPE={metrics['train_wape_actual']:.2f}%  MAPE={metrics['train_mape_actual']:.2f}%  SMAPE={metrics['train_smape_actual']:.2f}%")
        print(f"    Val:   RMSE={metrics['val_rmse_actual']:.2f}    MAE={metrics['val_mae_actual']:.2f}    R²={metrics['val_r2_actual']:.4f}  WAPE={metrics['val_wape_actual']:.2f}%  MAPE={metrics['val_mape_actual']:.2f}%  SMAPE={metrics['val_smape_actual']:.2f}%")
        print(f"    Test:  RMSE={metrics['test_rmse_actual']:.2f}   MAE={metrics['test_mae_actual']:.2f}   R²={metrics['test_r2_actual']:.4f}  WAPE={metrics['test_wape_actual']:.2f}%  MAPE={metrics['test_mape_actual']:.2f}%  SMAPE={metrics['test_smape_actual']:.2f}%")
        print(f"  [METRIC GAPS]")
        print(f"    Train-Val R² (Log) gap:  {metrics['train_r2_log'] - metrics['val_r2_log']:.4f}")
        print(f"    Val-Test R² (Log) gap:   {metrics['val_r2_log']   - metrics['test_r2_log']:.4f}")
        print(f"  Selected Hyperparameters: {best_hparams}")

        mlflow.log_params({
            "model_version":    "gru_v5_full",
            "source_table":     DL_FEATURES_TABLE,
            "sequence_length":  SEQUENCE_LENGTH,
            "hidden_size":      best_hparams["hidden_size"],
            "num_layers":       best_hparams["num_layers"],
            "dropout":          best_hparams["dropout"],
            "learning_rate":    best_hparams["learning_rate"],
            "loss":             "HybridLoss_Huber0.5_SMAPE0.3",
            "scaler_type":      "RobustScaler",
            "epochs_max":       EPOCHS,
            "batch_size":       BATCH_SIZE,
            "patience":         PATIENCE,
            "weight_decay":     str(best_hparams["weight_decay"]),
            "scheduler":        "CosineAnnealingWarmRestarts_T0=30_step_by_epoch",
            "num_features":     len(ALL_FEATURES),
            "split":            f"train{int(TRAIN_RATIO*100)}/val{int(VAL_RATIO*100)}/test{int(TEST_RATIO*100)}",
            "tuning_applied":   str(optuna_available)
        })
        mlflow.log_metrics(metrics)

        # Plots
        _log_training_plots(
            train_losses, val_losses, y_test, y_pred_test, metrics, meta_test
        )

        # Save artifacts
        mlflow.pytorch.log_model(model, "model")
        _save_artifacts(scaler, clip_thresholds)

        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, MODEL_NAME)
        print(f"  Model registered: {MODEL_NAME}")

    return model, scaler, clip_thresholds, df_pd_scaled, device


# ============================================================
# Plotting helpers
# ============================================================

def _log_training_plots(train_losses, val_losses, y_test, y_pred_test, metrics, meta_test):
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(train_losses, label='Train')
    ax.plot(val_losses,   label='Val')
    ax.set_xlabel('Epoch'); ax.set_ylabel('Loss')
    ax.set_title('GRU — Training & Validation Loss')
    ax.legend()
    mlflow.log_figure(fig, "loss_curve.png"); plt.close()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(y_test, y_pred_test, alpha=0.5, s=15)
    max_val = float(max(y_test.max(), y_pred_test.max()))
    ax.plot([0, max_val], [0, max_val], 'r--')
    ax.set_xlabel('Actual (log1p)'); ax.set_ylabel('Predicted (log1p)')
    ax.set_title(f"GRU: Actual vs Predicted — Test Set (R²={metrics['test_r2_log']:.3f})")
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
    with open("/tmp/scaler_gru_v5.pkl", "wb") as f:
        pickle.dump(scaler, f)
    mlflow.log_artifact("/tmp/scaler_gru_v5.pkl", "artifacts")

    config = {
        "all_features":     ALL_FEATURES,
        "target":           TARGET,
        "sequence_length":  SEQUENCE_LENGTH,
        "scaler_type":      "RobustScaler",
        "model_version":    "gru_v5",
        "split":            {"train": TRAIN_RATIO, "val": VAL_RATIO, "test": TEST_RATIO},
        "clip_thresholds":  {k: float(v) for k, v in clip_thresholds.items()},
        "hotness_lag_features":  HOTNESS_LAG_FEATURES,
        "aspect_features":       ASPECT_FEATURES,
        "forecast_assumption":
            "Social/NLP/aspect features beyond lag window use persistence assumption "
            "(held at most recent known value). Hotel volume lags are updated "
            "autoregressively from model predictions.",
    }
    with open("/tmp/feature_config_v5.json", "w") as f:
        json.dump(config, f, indent=2)
    mlflow.log_artifact("/tmp/feature_config_v5.json", "artifacts")


# ============================================================
# Step 4: Forecast 12 months (FIX 8: growth_pct bug fixed)
# ============================================================

def _scale_value(raw, feat_idx, scaler):
    """Scale a single raw value using fitted RobustScaler."""
    feature_name = ALL_FEATURES[feat_idx]
    features_to_scale = [f for f in ALL_FEATURES if f not in UNSCALED_FEATURES]
    if feature_name not in features_to_scale:
        return raw
    scaler_idx = features_to_scale.index(feature_name)
    s = scaler.scale_[scaler_idx]
    return (raw - scaler.center_[scaler_idx]) / s if s != 0 else 0.0


def _descale_value(scaled_val, feat_idx, scaler):
    """Descale a single scaled value back to raw using fitted RobustScaler."""
    feature_name = ALL_FEATURES[feat_idx]
    features_to_scale = [f for f in ALL_FEATURES if f not in UNSCALED_FEATURES]
    if feature_name not in features_to_scale:
        return scaled_val
    scaler_idx = features_to_scale.index(feature_name)
    s = scaler.scale_[scaler_idx]
    return scaled_val * s + scaler.center_[scaler_idx] if s != 0 else scaler.center_[scaler_idx]


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
        table_name="province_month_forecast_gru_next12",
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
    results = []
    import math

    for province_sk, group in df_pd.groupby("province_sk"):
        group = group.sort_values("year_month")
        if len(group) < SEQUENCE_LENGTH:
            continue

        province_name = group["province_name"].iloc[-1]
        region        = group["region"].iloc[-1] if "region" in group.columns else "Unknown"

        last_seq      = group[ALL_FEATURES].values[-SEQUENCE_LENGTH:].copy()
        last_ym       = int(group["year_month"].iloc[-1])
        last_date     = datetime.strptime(str(last_ym), '%Y%m')

        recent_volumes = list(np.expm1(group[TARGET].values[-max(SEQUENCE_LENGTH, 12):]))

        for horizon in range(1, FORECAST_MONTHS + 1):
            total_m = last_date.year * 12 + last_date.month + horizon
            ty  = (total_m - 1) // 12
            tm  = (total_m - 1) % 12 + 1
            tym = int(f"{ty:04d}{tm:02d}")

            model.eval()
            with torch.no_grad():
                pred_log    = model(torch.FloatTensor(last_seq).unsqueeze(0).to(device)).cpu().item()
                pred_log    = float(max(0.0, pred_log))
                pred_actual = float(np.expm1(pred_log))

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
                "model_version":                 "gru_v5",
            })

            recent_volumes.append(pred_actual)

            # --- Update sequence for next step ---
            new_row = last_seq[-1].copy()

            # Temporal (Fourier Seasonality based on FOURIER_K)
            for k in range(1, FOURIER_K + 1):
                if k == 1:
                    new_row[fi["month_sin"]] = math.sin(2 * math.pi * tm / 12.0)
                    new_row[fi["month_cos"]] = math.cos(2 * math.pi * tm / 12.0)
                else:
                    new_row[fi[f"month_sin_k{k}"]] = math.sin(2 * math.pi * k * tm / 12.0)
                    new_row[fi[f"month_cos_k{k}"]] = math.cos(2 * math.pi * k * tm / 12.0)

            # (hotel_vol_lag_1/2/3 removed from features, autoregressive state maintained in recent_volumes)

            if len(recent_volumes) >= 12:
                new_row[fi["hotel_vol_lag_12"]] = _scale_value(
                    recent_volumes[-12], fi["hotel_vol_lag_12"], scaler
                )
            if len(recent_volumes) >= 3:
                new_row[fi["hotel_vol_rolling_3m"]] = _scale_value(
                    float(np.mean(recent_volumes[-3:])), fi["hotel_vol_rolling_3m"], scaler
                )
            lag1 = recent_volumes[-1] if len(recent_volumes) >= 1 else 0.0
            lag3 = recent_volumes[-3] if len(recent_volumes) >= 3 else lag1
            new_row[fi["hotel_vol_momentum"]] = _scale_value(
                lag1 - lag3, fi["hotel_vol_momentum"], scaler
            )

            # hotel_vol_growth (clamp consistent with training preprocessing)
            prev_vol = recent_volumes[-2] if len(recent_volumes) >= 2 else 0.0
            growth   = (pred_actual - prev_vol) / prev_vol if prev_vol > 0 else 0.0
            growth   = max(-1.0, min(5.0, growth))
            new_row[fi["hotel_vol_growth"]] = _scale_value(growth, fi["hotel_vol_growth"], scaler)

            # social_to_booking_ratio
            idx_tc  = fi["total_comments"]
            raw_tc  = _descale_value(new_row[idx_tc], idx_tc, scaler)
            new_row[fi["social_to_booking_ratio"]] = _scale_value(
                raw_tc / (pred_actual + 1.0), fi["social_to_booking_ratio"], scaler
            )

            new_row[fi["sentiment_polarity_change"]] = _scale_value(
                0.0, fi["sentiment_polarity_change"], scaler
            )
            new_std = float(np.std(recent_volumes[-3:]))
            new_row[fi["hotel_vol_std_rolling_3m"]] = _scale_value(
                new_std, fi["hotel_vol_std_rolling_3m"], scaler
            )

            # Hotness lags: persistence assumption
            # (no TikTok data for future → hold last known value)
            # These are kept as-is in new_row (already copied from last_seq[-1])

            last_seq = np.vstack([last_seq[1:], new_row])

    print(f"  Generated {len(results)} predictions ({FORECAST_MONTHS} months × provinces)")

    forecast_df = spark.createDataFrame(results)
    forecast_df.write.format("iceberg").mode("overwrite") \
        .save("gold.gold.province_month_forecast_gru_next12")

    export_path = "s3a://gold/dl_forecast/province_hotel_volume_forecast_gru_v5"

    forecast_df.coalesce(1).write.mode("overwrite").parquet(export_path)
    print(f"  Exported: {export_path}")
    forecast_df.orderBy("province_name", "year_month").show(10, truncate=False)

    return forecast_df


# ============================================================
# Main
# ============================================================

def main():
    print("\n" + "=" * 80)
    print("ML PIPELINE: PROVINCE HOTEL VOLUME FORECASTING (GRU)")
    print("=" * 80)
    print(f"  Source:   {DL_FEATURES_TABLE}")
    print(f"  Features: {len(ALL_FEATURES)} total")
    print(f"  Split:    {int(TRAIN_RATIO*100)}/{int(VAL_RATIO*100)}/{int(TEST_RATIO*100)} (train/val/test)")
    print(f"  Start:    {datetime.now()}")
    print("=" * 80)

    spark = create_spark_session()

    try:
        df = load_features(spark)
        model, scaler, clip_thresholds, df_pd, device = train_model(df)
        forecast_12_months(spark, model, scaler, clip_thresholds, df_pd, device)

        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"  Model:      {MODEL_NAME}")
        print(f"  Table:      gold.gold.province_month_forecast_gru_next12")
        print(f"  Features:   {len(ALL_FEATURES)} from {DL_FEATURES_TABLE}")
        print(f"  MLflow:     {MLFLOW_TRACKING_URI}")
        print(f"  End:        {datetime.now()}")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
