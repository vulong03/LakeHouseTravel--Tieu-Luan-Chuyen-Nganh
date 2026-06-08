"""
DL Pipeline: Train GRU Deep Learning Model & Forecast Province Hotness
========================================================================
Designed as a direct comparison model to LSTM on the same features.

Reads directly from gold.gold.fact_province_month_dl_features (~40 features).
No inline hotness calculation — all features pre-computed in Gold layer.

Architecture:
- 2-layer GRU + LayerNorm + Temporal Attention
- Deeper FC head: Linear(hidden) → GELU → Dropout → Linear(16) → ReLU → Linear(1)
- HybridLoss: 70% HuberLoss + 30% SMAPELoss
- RobustScaler (median/IQR)
- CosineAnnealingWarmRestarts scheduler
- Outlier clipping for skewed engagement features before scaling

Workflow:
1. Read fact_province_month_dl_features
2. Select feature columns, clip outliers, scale, build sequences
3. Train GRU with MLflow tracking
4. Forecast 12 months (recursive autoregressive)

Output:
- Model: province_hotel_volume_forecaster_gru (MLflow registry)
- Table: gold.gold.province_month_forecast_gru_next12
- Parquet: s3://gold/dl_forecast/province_hotel_volume_forecast_gru_*
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
EXPERIMENT_NAME = "province_hotel_volume_forecasting_gru"
MODEL_NAME = "province_hotel_volume_forecaster_gru"

DL_FEATURES_TABLE = "gold.gold.fact_province_month_dl_features"

FORECAST_MONTHS = 12
TRAIN_TEST_SPLIT = 0.7

SEQUENCE_LENGTH = 3       # Same as LSTM
HIDDEN_SIZE = 48          # Same as LSTM
NUM_LAYERS = 2            # Same as LSTM
DROPOUT = 0.43            # Same as LSTM
LEARNING_RATE = 0.0005    # Same as LSTM
EPOCHS = 200              # Same as LSTM
BATCH_SIZE = 32           # Same as LSTM
PATIENCE = 25             # Same as LSTM
TARGET = "hotel_review_volume"

# Features read from DL fact table
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
    "avg_likes_per_post", "avg_saves_per_post",
    "viral_post_ratio", "engagement_score",
    "hotness_score",
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


# ============================================================
# Loss Functions
# ============================================================

class SMAPELoss(nn.Module):
    def forward(self, pred, target):
        denom = (torch.abs(target) + torch.abs(pred)) / 2.0 + 1e-8
        return torch.mean(torch.abs(pred - target) / denom)


class HybridLoss(nn.Module):
    def __init__(self, delta=0.5, smape_weight=0.3):
        super().__init__()
        self.huber = nn.HuberLoss(delta=delta)
        self.smape = SMAPELoss()
        self.w = smape_weight

    def forward(self, pred, target):
        return (1 - self.w) * self.huber(pred, target) + self.w * self.smape(pred, target)


# ============================================================
# GRU Model with LayerNorm + Temporal Attention
# ============================================================

class TemporalAttention(nn.Module):
    def __init__(self, hidden_size):
        super().__init__()
        self.attn = nn.Linear(hidden_size, 1)

    def forward(self, gru_output):
        scores = self.attn(gru_output).squeeze(-1)
        weights = torch.softmax(scores, dim=1)
        context = torch.bmm(weights.unsqueeze(1), gru_output).squeeze(1)
        return context, weights


class GRUForecaster(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, dropout):
        super().__init__()
        self.gru = nn.GRU(
            input_size=input_size, hidden_size=hidden_size,
            num_layers=num_layers, batch_first=True,
            dropout=dropout if num_layers > 1 else 0
        )
        self.layer_norm = nn.LayerNorm(hidden_size)
        self.attention = TemporalAttention(hidden_size)
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, hidden_size),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 16),
            nn.ReLU(),
            nn.Linear(16, 1)
        )

    def forward(self, x):
        gru_out, _ = self.gru(x)
        gru_out = self.layer_norm(gru_out)
        context, _ = self.attention(gru_out)
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
        .appName("ML_Province_Hotness_GRU") \
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
# Step 1: Load from DL fact table
# ============================================================

def load_features(spark):
    print("\n" + "=" * 80)
    print("STEP 1: LOADING PRE-COMPUTED DL FEATURES FOR GRU")
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

    # Drop rows with missing lags
    before = df.count()
    df = df.dropna(subset=LAG_FEATURES + [TARGET])
    df = df.filter(F.col(TARGET) > 0)
    after = df.count()
    print(f"  Dropped {before - after} rows, remaining: {after}")
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
# Step 3: Train GRU
# ============================================================

def train_model(df):
    print("\n" + "=" * 80)
    print("STEP 3: TRAINING GRU MODEL")
    print("=" * 80)

    select_cols = ALL_FEATURES + [TARGET, "year_month", "province_sk", "province_name", "region"]
    df_pd = df.select(select_cols).toPandas()
    df_pd = df_pd.sort_values(["province_sk", "year_month"])
    df_pd[ALL_FEATURES] = df_pd[ALL_FEATURES].fillna(0)

    # Outlier Clipping
    df_pd["hotel_vol_growth"] = df_pd["hotel_vol_growth"].clip(-1.0, 5.0)
    df_pd["engagement_score"] = np.log1p(df_pd["engagement_score"])

    for col in ["total_posts", "total_comments", "avg_likes_per_post",
                "avg_saves_per_post", "engagement_score"]:
        p99 = df_pd[col].quantile(0.99)
        if p99 > 0:
            df_pd[col] = df_pd[col].clip(upper=p99)

    # Log transform target
    df_pd[TARGET] = np.log1p(df_pd[TARGET].astype(float))

    # Time-based split
    split_ym = int(df_pd["year_month"].quantile(TRAIN_TEST_SPLIT))
    train_raw = df_pd[df_pd["year_month"] <= split_ym].copy()
    test_raw = df_pd[df_pd["year_month"] > split_ym].copy()

    print(f"  Train: {len(train_raw)} rows | Test: {len(test_raw)} rows")

    scaler = RobustScaler()
    train_raw[ALL_FEATURES] = scaler.fit_transform(train_raw[ALL_FEATURES])
    test_raw[ALL_FEATURES] = scaler.transform(test_raw[ALL_FEATURES])

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
    model = GRUForecaster(
        input_size=len(ALL_FEATURES), hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS, dropout=DROPOUT
    ).to(device)

    total_params = sum(p.numel() for p in model.parameters())
    print(f"  Device: {device}, GRU Parameters: {total_params:,}")

    optimizer = torch.optim.Adam(
        model.parameters(), lr=LEARNING_RATE, weight_decay=7e-4
    )
    criterion = HybridLoss(delta=0.5, smape_weight=0.3)

    scheduler = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(
        optimizer, T_0=30, T_mult=2, eta_min=1e-6
    )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name=f"gru_v4_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_params({
            "model_type": "GRU_v4_Attention",
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

        mask = y_test_actual > 0
        test_mape = float(
            np.mean(np.abs((y_test_actual[mask] - y_pred_actual[mask]) / y_test_actual[mask])) * 100
        ) if np.sum(mask) > 0 else 0.0

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

        print(f"\n  Train (GRU): RMSE={metrics['train_rmse']:.4f} MAE={metrics['train_mae']:.4f} R2={metrics['train_r2']:.4f}")
        print(f"  Test (GRU):  RMSE={metrics['test_rmse']:.4f} MAE={metrics['test_mae']:.4f} R2={metrics['test_r2']:.4f}")
        print(f"  MAPE(actual)={metrics['test_mape_actual']:.2f}%  SMAPE(log)={metrics['test_smape_log']:.2f}%")

        # Save Curves plot
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(train_losses, label='Train')
        ax.plot(val_losses, label='Val')
        ax.set_xlabel('Epoch'); ax.set_ylabel('Loss'); ax.set_title('GRU v4 Training Loss'); ax.legend()
        mlflow.log_figure(fig, "loss_curve.png"); plt.close()

        mlflow.pytorch.log_model(model, "model")

        with open("/tmp/scaler_gru.pkl", "wb") as f:
            pickle.dump(scaler, f)
        mlflow.log_artifact("/tmp/scaler_gru.pkl", "artifacts")

        with open("/tmp/feature_config_gru.json", "w") as f:
            json.dump({
                "all_features": ALL_FEATURES,
                "target": TARGET,
                "sequence_length": SEQUENCE_LENGTH,
                "scaler_type": "RobustScaler",
                "model_version": "gru_v4_volume"
            }, f, indent=2)
        mlflow.log_artifact("/tmp/feature_config_gru.json", "artifacts")

        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, MODEL_NAME)
        print(f"  Model registered: {MODEL_NAME}")

        return model, scaler, df_pd_scaled, device


# ============================================================
# Step 4: Forecast 12 months
# ============================================================

def _scale_value(raw, feat_idx, scaler):
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
        table_name="province_month_forecast_gru_next12",
        schema=schema, partition_by=["year", "month"],
        table_properties={"format-version": "2",
                          "write.format.default": "parquet",
                          "write.parquet.compression-codec": "snappy"},
         catalog="gold"
    )


def forecast_12_months(spark, model, scaler, df_pd, device):
    print("\n" + "=" * 80)
    print("STEP 4: FORECASTING 12 MONTHS (GRU)")
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
                'model_version': 'gru_v4_volume'
            })

            recent_volumes.append(pred_actual)

            new_row = last_seq[-1].copy()

            # Update temporal features
            new_row[fi["month_sin"]] = _scale_value(math.sin(2 * math.pi * tm / 12), fi["month_sin"], scaler)
            new_row[fi["month_cos"]] = _scale_value(math.cos(2 * math.pi * tm / 12), fi["month_cos"], scaler)

            # Update lag features
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

            # Update growth
            prev_vol = recent_volumes[-2] if len(recent_volumes) >= 2 else 0.0
            growth = 0.0
            if prev_vol > 0:
                growth = (pred_actual - prev_vol) / prev_vol
            growth = max(-1.0, min(5.0, growth))
            new_row[fi["hotel_vol_growth"]] = _scale_value(growth, fi["hotel_vol_growth"], scaler)

            # Update custom features
            idx_tc = fi["total_comments"]
            raw_tc = new_row[idx_tc] * scaler.scale_[idx_tc] + scaler.center_[idx_tc] if scaler.scale_[idx_tc] != 0 else scaler.center_[idx_tc]
            new_ratio = raw_tc / (pred_actual + 1.0)
            new_row[fi["social_to_booking_ratio"]] = _scale_value(new_ratio, fi["social_to_booking_ratio"], scaler)
            new_row[fi["sentiment_polarity_change"]] = _scale_value(0.0, fi["sentiment_polarity_change"], scaler)

            new_std = float(np.std(recent_volumes[-3:]))
            new_row[fi["hotel_vol_std_rolling_3m"]] = _scale_value(new_std, fi["hotel_vol_std_rolling_3m"], scaler)

            last_seq = np.vstack([last_seq[1:], new_row])

    print(f"  Generated {len(results)} predictions for GRU")

    forecast_df = spark.createDataFrame(results)
    forecast_df.write.format("iceberg").mode("overwrite") \
        .save("gold.gold.province_month_forecast_gru_next12")

    export_path = f"s3a://gold/dl_forecast/province_hotel_volume_forecast_gru_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    forecast_df.coalesce(1).write.mode("overwrite").parquet(export_path)
    print(f"  Exported GRU forecast to {export_path}")


def main():
    print("=" * 70)
    print("Gold Layer — Train GRU & Forecast Volume")
    print("=" * 70)
    print(f"Start: {datetime.now()}")

    spark = create_spark_session()
    logger = mlflow.pytorch  # Keep tracking reference
    start_time = datetime.now()

    try:
        df = load_features(spark)
        model, scaler, df_pd_scaled, device = train_model(df)
        forecast_12_months(spark, model, scaler, df_pd_scaled, device)

        execution_time = (datetime.now() - start_time).total_seconds()
        print(f"\nCompleted in {execution_time:.1f}s")

    except Exception as exc:
        print(f"\nFailed: {exc}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
