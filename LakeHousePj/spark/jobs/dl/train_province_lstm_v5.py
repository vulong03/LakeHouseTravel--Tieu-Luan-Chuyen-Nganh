"""
ML Pipeline: Train LSTM Deep Learning Model & Forecast Province Hotel Volume
=============================================================================

Source table: gold.gold.fact_province_month_dl_features (~47 features after v5 additions)

Architecture (v5):
- 2-layer LSTM + LayerNorm + Temporal Attention
- Deeper FC head: Linear(hidden) → GELU → Dropout → Linear(16) → ReLU → Linear(1)
- HybridLoss: 70% HuberLoss + 30% SMAPELoss
- RobustScaler (median/IQR)
- CosineAnnealingWarmRestarts scheduler

Changes from v4 → v5:
============================================================
🔴 FIX 1 [CRITICAL] Train/Val/Test split
   v4: 2-way split (70/30), test_loader dùng làm val cho early stopping
       → test metrics bị lạc quan (test set không còn khách quan)
   v5: 3-way split (60/20/20 time-based)
       train → học weights
       val   → early stopping, chọn epoch tốt nhất (KHÔNG báo metrics)
       test  → chỉ dùng 1 lần để report final metrics

🔴 FIX 2 [CRITICAL] Outlier clipping leakage
   v4: tính p99 trên toàn bộ df_pd TRƯỚC khi split
       → ngưỡng clip đã "nhìn thấy" test period → leakage nhẹ
   v5: chia train/val/test TRƯỚC, tính p99 trên train only,
       apply cùng ngưỡng cho val/test

🟡 FIX 3 Thêm hotness_lag features
   v4: chỉ dùng hotness_score (hiện tại), bỏ qua lag
   v5: thêm hotness_lag_1/2/3/12, hotness_rolling_3m, hotness_momentum
       → lập luận "TikTok có tác động trễ" có bằng chứng trong feature set

🟡 FIX 4 Thêm PhoBERT aspect features
   v4: bỏ qua avg_aspect_scenery/food/price/service/transport/accommodation
   v5: thêm đủ 6 aspect features
       → PhoBERT pipeline được tận dụng đầy đủ trong LSTM

🟢 FIX 7 Sửa scheduler.step() bug
   v4: scheduler.step(epoch + avg_val / 100)
       → CosineAnnealingWarmRestarts nhận epoch position, không nhận loss
   v5: scheduler.step(epoch) — đúng API

🟢 FIX 8 Sửa growth_pct bug trong forecast loop
   v4: last_actual_volume gán 1 lần trước loop
       → tất cả 12 horizon so growth với tháng cuối lịch sử cố định
   v5: prev_volume cập nhật mỗi bước horizon

🟢 FIX 9 Bỏ unused BatchNorm1d layer
   v4: self.bn khai báo nhưng bị comment out → lãng phí params
   v5: xóa hẳn self.bn

🟢 FIX 10 Framing rõ 12-month forecast assumption
   v5: doc rõ "social/NLP features dùng persistence assumption
       (giữ theo trạng thái gần nhất)" trong output table

Features v5: 47 total
  Temporal (2): month_sin, month_cos
  Hotel lag (6): hotel_vol_lag_1/2/3/12, rolling_3m, momentum
  Hotness lag (6): hotness_lag_1/2/3/12, rolling_3m, momentum  [NEW]
  Volume (5): total_posts, total_comments, total_hotel_reviews, unique_authors, comments_per_post
  Engagement (4): avg_likes_per_post, avg_saves_per_post, viral_post_ratio, engagement_score, hotness_score
  NLP (7): avg_sentiment, sentiment_std, positive_ratio, negative_ratio,
           avg_word_count, avg_unique_word_ratio, emoji_sentiment_ratio, reply_ratio
  Aspect (6): avg_aspect_scenery, avg_aspect_food, avg_aspect_price,  [NEW]
              avg_aspect_service, avg_aspect_transport, avg_aspect_accommodation
  Hotel quality (9): avg_hotel_score, hotel_score_std, high_score_ratio,
                     domestic_review_ratio, couple_ratio, family_ratio,
                     business_ratio, solo_ratio, hotel_vol_growth
  Custom (3): social_to_booking_ratio, sentiment_polarity_change, hotel_vol_std_rolling_3m

Output:
- Model: province_hotel_volume_forecaster_lstm_v5 (MLflow registry)
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

# FIX 1: 3-way time-based split (train/val/test)
# 70% train → 15% val → 15% test
TRAIN_RATIO = 0.70
VAL_RATIO   = 0.15
# TEST_RATIO  = 0.15 (implicit: remainder)

# Model hyperparameters (aligned with v4 configurations)
SEQUENCE_LENGTH = 3
HIDDEN_SIZE     = 48
NUM_LAYERS      = 2
DROPOUT         = 0.43
LEARNING_RATE   = 0.0005
EPOCHS          = 200
BATCH_SIZE      = 32
PATIENCE        = 25
WEIGHT_DECAY    = 7e-4

TARGET = "hotel_review_volume"

# ============================================================
# Feature Groups
# ============================================================

TEMPORAL_FEATURES = ["month_sin", "month_cos"]

HOTEL_LAG_FEATURES = [
    "hotel_vol_lag_1", "hotel_vol_lag_2", "hotel_vol_lag_3",
    "hotel_vol_lag_12", "hotel_vol_rolling_3m", "hotel_vol_momentum",
]

# FIX 3: hotness lag features (TikTok delayed signal)
HOTNESS_LAG_FEATURES = [
    "hotness_lag_1", "hotness_lag_2", "hotness_lag_3",
    "hotness_lag_12", "hotness_rolling_3m", "hotness_momentum",
]

VOLUME_FEATURES = [
    "total_posts", "total_comments", "total_hotel_reviews",
    "unique_authors", "comments_per_post",
]

ENGAGEMENT_FEATURES = [
    # avg_shares_per_post REMOVED (v4, 2026-06-05):
    # TikTok "total distribution" metric, 63.6% NULL, không đồng nhất → không tin cậy
    "avg_likes_per_post", "avg_saves_per_post",
    "viral_post_ratio", "engagement_score",
    "hotness_score",
]

NLP_FEATURES = [
    "avg_sentiment", "sentiment_std", "positive_ratio", "negative_ratio",
    "avg_word_count", "avg_unique_word_ratio",
    "emoji_sentiment_ratio", "reply_ratio",
]

# FIX 4: PhoBERT aspect features (previously unused)
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

# Full feature set (v5: 47 features)
ALL_FEATURES = (
    TEMPORAL_FEATURES
    + HOTEL_LAG_FEATURES
    + HOTNESS_LAG_FEATURES      # FIX 3: +6 new
    + VOLUME_FEATURES
    + ENGAGEMENT_FEATURES
    + NLP_FEATURES
    + ASPECT_FEATURES           # FIX 4: +6 new
    + HOTEL_FEATURES
    + CUSTOM_FEATURES
)

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
# LSTM Model (v5 — removed unused BatchNorm, FIX 9)
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
    2-layer LSTM + LayerNorm + Temporal Attention + deep FC head.
    v5: removed unused BatchNorm1d layer (FIX 9).
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
        # FIX 9: BatchNorm1d removed — was declared but never used in v4
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, hidden_size),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 16),
            nn.ReLU(),
            nn.Linear(16, 1)
        )

    def forward(self, x):
        lstm_out, _  = self.lstm(x)
        lstm_out     = self.layer_norm(lstm_out)
        context, _   = self.attention(lstm_out)
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

def train_single_lstm(X_train, y_train, X_val, y_val, X_test, y_test,
                      input_size, run_name, device):
    """
    FIX 1: val set dùng cho early stopping, test set chỉ báo metrics 1 lần ở cuối.
    FIX 7: scheduler.step(epoch) — đúng API CosineAnnealingWarmRestarts.
    """
    model = LSTMForecaster(
        input_size=input_size, hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS, dropout=DROPOUT
    ).to(device)

    total_params = sum(p.numel() for p in model.parameters())

    optimizer  = torch.optim.Adam(model.parameters(),
                                  lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)
    criterion  = HybridLoss(delta=0.5, smape_weight=0.3)
    # FIX 7: CosineAnnealingWarmRestarts.step(epoch) — step theo epoch position
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

    for epoch in range(EPOCHS):
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

        # --- Validate (FIX 1: separate val set) ---
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for xb, yb in val_loader:
                xb, yb = xb.to(device), yb.to(device)
                val_loss += criterion(model(xb), yb).item() * len(xb)
        avg_val = val_loss / max(len(X_val), 1)
        val_losses.append(avg_val)

        # FIX 7: step by epoch position (not loss value)
        scheduler.step(epoch)

        if (epoch + 1) % 20 == 0:
            lr = optimizer.param_groups[0]['lr']
            print(f"    [{run_name}] Epoch {epoch+1}/{EPOCHS} | "
                  f"Train: {avg_train:.5f} | Val: {avg_val:.5f} | LR: {lr:.6f}")

        # Early stopping on VAL (FIX 1)
        if avg_val < best_val_loss:
            best_val_loss    = avg_val
            patience_counter = 0
            best_state       = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        else:
            patience_counter += 1
            if patience_counter >= PATIENCE:
                print(f"    [{run_name}] Early stopping at epoch {epoch+1}")
                break

    # Load best checkpoint
    model.load_state_dict(best_state)
    model.to(device)

    # --- Evaluate on TEST (FIX 1: used ONLY here, after training complete) ---
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


# ============================================================
# Step 3c: Main LSTM training (FIX 5 ablation study removed)
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

    # Build sequences for the entire dataset (prevents boundary sequence loss!)
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
    # Main LSTM run (full features)
    # -------------------------------------------------------
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    print("\n  Training main LSTM (full features)...")
    with mlflow.start_run(run_name=f"lstm_v5_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as main_run:

        model, metrics, train_losses, val_losses, y_pred_test = train_single_lstm(
            X_train, y_train, X_val, y_val, X_test, y_test,
            input_size=len(ALL_FEATURES),
            run_name="full",
            device=device
        )

        print(f"\n  MAIN MODEL RESULTS:")
        print(f"  Train: RMSE={metrics['train_rmse']:.4f}  MAE={metrics['train_mae']:.4f}  R²={metrics['train_r2']:.4f}")
        print(f"  Val:   RMSE={metrics['val_rmse']:.4f}    MAE={metrics['val_mae']:.4f}    R²={metrics['val_r2']:.4f}")
        print(f"  Test:  RMSE={metrics['test_rmse']:.4f}   MAE={metrics['test_mae']:.4f}   R²={metrics['test_r2']:.4f}")
        print(f"  MAPE(actual)={metrics['test_mape_actual']:.2f}%  SMAPE(log)={metrics['test_smape_log']:.2f}%")
        print(f"  Train-Val R² gap:  {metrics['train_r2'] - metrics['val_r2']:.4f}")
        print(f"  Val-Test R² gap:   {metrics['val_r2']   - metrics['test_r2']:.4f}")

        mlflow.log_params({
            "model_version":    "lstm_v5_full",
            "source_table":     DL_FEATURES_TABLE,
            "sequence_length":  SEQUENCE_LENGTH,
            "hidden_size":      HIDDEN_SIZE,
            "num_layers":       NUM_LAYERS,
            "dropout":          DROPOUT,
            "learning_rate":    LEARNING_RATE,
            "loss":             "HybridLoss_Huber0.5_SMAPE0.3",
            "scaler_type":      "RobustScaler",
            "epochs_max":       EPOCHS,
            "batch_size":       BATCH_SIZE,
            "patience":         PATIENCE,
            "weight_decay":     str(WEIGHT_DECAY),
            "scheduler":        "CosineAnnealingWarmRestarts_T0=30_step_by_epoch",
            "num_features":     len(ALL_FEATURES),
            "split":            f"train{int(TRAIN_RATIO*100)}/val{int(VAL_RATIO*100)}/test20",
            "fix_1_val_split":  "True",
            "fix_2_clip_leak":  "True",
            "fix_3_hotness_lag":"True",
            "fix_4_aspect":     "True",
            "fix_7_scheduler":  "True",
            "fix_8_growth_pct": "True",
            "fix_9_bn_removed": "True",
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
        "all_features":     ALL_FEATURES,
        "target":           TARGET,
        "sequence_length":  SEQUENCE_LENGTH,
        "scaler_type":      "RobustScaler",
        "model_version":    "lstm_v5",
        "split":            {"train": TRAIN_RATIO, "val": VAL_RATIO, "test": 0.20},
        "clip_thresholds":  {k: float(v) for k, v in clip_thresholds.items()},
        "hotness_lag_features":  HOTNESS_LAG_FEATURES,
        "aspect_features":       ASPECT_FEATURES,
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
# Step 4: Forecast 12 months (FIX 8: growth_pct bug fixed)
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

            # FIX 8: growth vs PREVIOUS step (not fixed baseline)
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
            new_row[fi["month_sin"]] = _scale_value(math.sin(2 * math.pi * tm / 12), fi["month_sin"], scaler)
            new_row[fi["month_cos"]] = _scale_value(math.cos(2 * math.pi * tm / 12), fi["month_cos"], scaler)

            # Hotel volume lags (autoregressive update)
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
            raw_tc  = (new_row[idx_tc] * scaler.scale_[idx_tc] + scaler.center_[idx_tc]
                       if scaler.scale_[idx_tc] != 0 else scaler.center_[idx_tc])
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
    print(f"  Split:    {int(TRAIN_RATIO*100)}/{int(VAL_RATIO*100)}/20 (train/val/test)")
    print(f"  Fixes:    FIX1(val split) FIX2(clip leak) FIX3(hotness lag) "
          f"FIX4(aspect) FIX7(scheduler) "
          f"FIX8(growth) FIX9(bn)")
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
        print(f"  Table:      gold.gold.province_month_forecast_lstm_next12")
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
