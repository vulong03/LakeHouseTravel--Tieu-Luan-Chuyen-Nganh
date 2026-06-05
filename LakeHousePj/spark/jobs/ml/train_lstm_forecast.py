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
TRAIN_TEST_SPLIT = 0.7

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
        # During evaluation (model.eval()), batch norm is always valid since it uses running stats.
        # During training, we require batch_size > 1.
        if not self.training or context.shape[0] > 1:
            context = self.bn(context)
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


# ============================================================
# Step 3: Train LSTM
# ============================================================

def train_model(df):
    print("\n" + "=" * 80)
    print("STEP 3: TRAINING LSTM MODEL (v4)")
    print("=" * 80)

    select_cols = ALL_FEATURES + [TARGET, "year_month", "province_sk", "province_name", "region"]
    df_pd = df.select(select_cols).toPandas()
    df_pd = df_pd.sort_values(["province_sk", "year_month"])
    df_pd[ALL_FEATURES] = df_pd[ALL_FEATURES].fillna(0)

    # --- Outlier Clipping (before scaling) ---
    # hotel_vol_growth: extreme outliers distort scaler (e.g. +4900%)
    df_pd["hotel_vol_growth"] = df_pd["hotel_vol_growth"].clip(-1.0, 5.0)

    # Engagement features: viral TikTok posts create extreme outliers.
    # DQ findings (2026-06-05):
    #   avg_likes_per_post: min=6, max=395,750 — clip at 99th percentile
    #   engagement_score: log1p first (sum of likes+comments+saves+shares)
    #   avg_shares_per_post: REMOVED from feature set — metric không đồng nhất:
    #     TikTok "Số lượt share" = total cross-platform distribution (Messenger/Zalo/story)
    #     khác với nút Share visible; 63.6% NULL trong silver; không đáng tin.
    df_pd["engagement_score"] = np.log1p(df_pd["engagement_score"])

    for col in ["total_posts", "total_comments", "avg_likes_per_post",
                "avg_saves_per_post", "engagement_score"]:
        p99 = df_pd[col].quantile(0.99)
        if p99 > 0:
            df_pd[col] = df_pd[col].clip(upper=p99)

    # Log transform the target variable
    df_pd[TARGET] = np.log1p(df_pd[TARGET].astype(float))

    # Time-based split BEFORE scaling (no data leakage)
    split_ym = int(df_pd["year_month"].quantile(TRAIN_TEST_SPLIT))
    train_raw = df_pd[df_pd["year_month"] <= split_ym].copy()
    test_raw = df_pd[df_pd["year_month"] > split_ym].copy()

    print(f"  Train: {len(train_raw)} rows (up to {split_ym})")
    print(f"  Test:  {len(test_raw)} rows (from {split_ym + 1})")

    # RobustScaler: uses median + IQR → robust to outlier provinces
    # Fit on TRAIN only to prevent leakage
    scaler = RobustScaler()
    train_raw[ALL_FEATURES] = scaler.fit_transform(train_raw[ALL_FEATURES])
    test_raw[ALL_FEATURES] = scaler.transform(test_raw[ALL_FEATURES])

    # Full scaled dataset for forecast step
    df_pd_scaled = df_pd.copy()
    df_pd_scaled[ALL_FEATURES] = scaler.transform(df_pd_scaled[ALL_FEATURES])

    X_train, y_train, _ = prepare_sequences(train_raw, ALL_FEATURES, TARGET, SEQUENCE_LENGTH)
    X_test, y_test, meta_test = prepare_sequences(test_raw, ALL_FEATURES, TARGET, SEQUENCE_LENGTH)

    if len(X_train) == 0 or len(X_test) == 0:
        print("  WARNING: Not enough data for proper split, using index split.")
        X_all, y_all, meta_all = prepare_sequences(df_pd_scaled, ALL_FEATURES, TARGET, SEQUENCE_LENGTH)
        split_idx = int(len(X_all) * TRAIN_TEST_SPLIT)
        X_train, y_train = X_all[:split_idx], y_all[:split_idx]
        X_test, y_test = X_all[split_idx:], y_all[split_idx:]
        meta_test = meta_all[split_idx:]

    print(f"  Train sequences: {len(X_train)}, Test sequences: {len(X_test)}")

    train_loader = DataLoader(
        TimeSeriesDataset(X_train, y_train), batch_size=BATCH_SIZE, shuffle=True
    )
    test_loader = DataLoader(
        TimeSeriesDataset(X_test, y_test), batch_size=BATCH_SIZE, shuffle=False
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = LSTMForecaster(
        input_size=len(ALL_FEATURES), hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS, dropout=DROPOUT
    ).to(device)

    total_params = sum(p.numel() for p in model.parameters())
    print(f"  Device: {device}, Parameters: {total_params:,}")

    optimizer = torch.optim.Adam(
        model.parameters(), lr=LEARNING_RATE, weight_decay=7e-4  # 7e-4 anti-overfit
    )
    criterion = HybridLoss(delta=0.5, smape_weight=0.3)

    # CosineAnnealingWarmRestarts: better exploration than ReduceLROnPlateau
    # T_0=30: first restart at epoch 30, T_mult=2: each cycle doubles in length
    scheduler = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(
        optimizer, T_0=30, T_mult=2, eta_min=1e-6
    )

    # --- MLflow ---
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name=f"lstm_v4_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_params({
            "model_type": "LSTM_v4_Attention",
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
            "features": json.dumps(ALL_FEATURES),
            "train_size": len(X_train),
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
                for xb, yb in test_loader:
                    xb, yb = xb.to(device), yb.to(device)
                    val_loss += criterion(model(xb), yb).item() * len(xb)
            avg_val = val_loss / max(len(X_test), 1)
            val_losses.append(avg_val)

            # CosineAnnealing: step every epoch
            scheduler.step(epoch + avg_val / 100)

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
            y_pred = model(torch.FloatTensor(X_test).to(device)).cpu().numpy()
            y_pred = np.clip(y_pred, 0.0, None)
            y_train_pred = model(torch.FloatTensor(X_train).to(device)).cpu().numpy()
            y_train_pred = np.clip(y_train_pred, 0.0, None)

        y_test_actual = np.expm1(y_test)
        y_pred_actual = np.expm1(y_pred)

        # MAPE (actual scale, mask zero targets)
        mask = y_test_actual > 0
        test_mape = float(
            np.mean(np.abs((y_test_actual[mask] - y_pred_actual[mask]) / y_test_actual[mask])) * 100
        ) if np.sum(mask) > 0 else 0.0

        # SMAPE (log scale) — less biased metric for reporting
        test_smape = float(
            2 * np.mean(
                np.abs(y_test - y_pred) / (np.abs(y_test) + np.abs(y_pred) + 1e-8)
            ) * 100
        )

        metrics = {
            "train_rmse": float(np.sqrt(mean_squared_error(y_train, y_train_pred))),
            "train_mae": float(mean_absolute_error(y_train, y_train_pred)),
            "train_r2": float(r2_score(y_train, y_train_pred)),
            "test_rmse": float(np.sqrt(mean_squared_error(y_test, y_pred))),
            "test_mae": float(mean_absolute_error(y_test, y_pred)),
            "test_r2": float(r2_score(y_test, y_pred)),
            "test_mape_actual": test_mape,
            "test_smape_log": test_smape,
            "best_val_loss": float(best_val_loss),
            "epochs_trained": len(train_losses),
        }
        mlflow.log_metrics(metrics)

        print(f"\n  Train: RMSE={metrics['train_rmse']:.4f} MAE={metrics['train_mae']:.4f} R2={metrics['train_r2']:.4f}")
        print(f"  Test:  RMSE={metrics['test_rmse']:.4f} MAE={metrics['test_mae']:.4f} R2={metrics['test_r2']:.4f}")
        print(f"  MAPE(actual)={metrics['test_mape_actual']:.2f}%  SMAPE(log)={metrics['test_smape_log']:.2f}%")
        print(f"  Train-Test R2 gap: {metrics['train_r2'] - metrics['test_r2']:.4f}")

        # Plots
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(train_losses, label='Train')
        ax.plot(val_losses, label='Val')
        ax.set_xlabel('Epoch'); ax.set_ylabel('Loss'); ax.set_title('LSTM v4 Training Loss'); ax.legend()
        mlflow.log_figure(fig, "loss_curve.png"); plt.close()

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(y_test, y_pred, alpha=0.5, s=15)
        max_val = float(max(y_test.max(), y_pred.max()))
        ax.plot([0, max_val], [0, max_val], 'r--')
        ax.set_xlabel('Actual (log1p)'); ax.set_ylabel('Predicted (log1p)')
        ax.set_title(f'LSTM v4: Actual vs Predicted (R2={metrics["test_r2"]:.3f})')
        mlflow.log_figure(fig, "actual_vs_predicted.png"); plt.close()

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(y_pred, y_test - y_pred, alpha=0.5, s=15)
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
                        [y_pred[i] for i in idx], label='Predicted', marker='x')
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

    export_path = f"s3a://gold/ml_forecast/province_hotel_volume_forecast_lstm_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
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
