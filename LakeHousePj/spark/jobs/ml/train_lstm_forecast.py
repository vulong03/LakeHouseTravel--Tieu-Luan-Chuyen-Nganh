"""
ML Pipeline: Train GRU Deep Learning Model & Forecast Province Hotness
========================================================================

Reads directly from gold.gold.fact_province_month_dl_features (~40 features).
No inline hotness calculation — all features pre-computed in Gold layer.

Architecture:
- GRU + LayerNorm + Temporal Attention
- HuberLoss, weight_decay, early stopping
- Scaler fit on train only (no leakage)

Workflow:
1. Read fact_province_month_dl_features (ready-to-use)
2. Select feature columns, scale, build sequences
3. Train GRU with MLflow tracking
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
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import mlflow
import mlflow.pytorch
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
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

MLFLOW_TRACKING_URI = "postgresql://lakehouse_user:lakehouse_pass@postgres:5432/mlflow_db"
EXPERIMENT_NAME = "province_hotness_forecasting_lstm"
MODEL_NAME = "province_hotness_forecaster_lstm"

DL_FEATURES_TABLE = "gold.gold.fact_province_month_dl_features"

FORECAST_MONTHS = 12
TRAIN_TEST_SPLIT = 0.7

SEQUENCE_LENGTH = 4
HIDDEN_SIZE = 32
NUM_LAYERS = 1
DROPOUT = 0.3
LEARNING_RATE = 0.001
EPOCHS = 150
BATCH_SIZE = 32
PATIENCE = 20
TARGET = "hotness_score"

# Features read from DL fact table — no inline computation needed
TEMPORAL_FEATURES = ["month_sin", "month_cos"]

LAG_FEATURES = [
    "hotness_lag_1", "hotness_lag_2", "hotness_lag_3",
    "hotness_lag_12", "hotness_rolling_3m", "hotness_momentum",
]

VOLUME_FEATURES = [
    "total_posts", "total_comments", "total_hotel_reviews",
    "unique_authors", "comments_per_post",
]

ENGAGEMENT_FEATURES = [
    "avg_likes_per_post", "avg_saves_per_post", "avg_shares_per_post",
    "viral_post_ratio", "engagement_score",
]

NLP_FEATURES = [
    "avg_sentiment", "sentiment_std", "positive_ratio", "negative_ratio",
    "avg_word_count", "avg_unique_word_ratio",
    "emoji_sentiment_ratio", "reply_ratio",
]

HOTEL_FEATURES = [
    "avg_hotel_score", "hotel_score_std", "hotel_review_volume",
    "high_score_ratio", "domestic_review_ratio",
]

ALL_FEATURES = (
    TEMPORAL_FEATURES + LAG_FEATURES
    + VOLUME_FEATURES + ENGAGEMENT_FEATURES
    + NLP_FEATURES + HOTEL_FEATURES
)

# Features that should use LAGGED values during forecast (from previous step)
# Temporal + Lag features get updated each step; the rest keep last known values
FORECAST_UPDATABLE_FEATURES = set(TEMPORAL_FEATURES + LAG_FEATURES)


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
            nn.Linear(hidden_size, 16),
            nn.ReLU(),
            nn.Dropout(dropout),
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
    after = df.count()
    print(f"  Dropped {before - after} rows with NULL lags, remaining: {after}")
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

    # Time-based split BEFORE scaling
    split_ym = int(df_pd["year_month"].quantile(TRAIN_TEST_SPLIT))
    train_raw = df_pd[df_pd["year_month"] <= split_ym].copy()
    test_raw = df_pd[df_pd["year_month"] > split_ym].copy()

    print(f"  Train: {len(train_raw)} rows (up to {split_ym})")
    print(f"  Test:  {len(test_raw)} rows (from {split_ym + 1})")

    # Fit scaler on TRAIN only
    scaler = MinMaxScaler()
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

    model = GRUForecaster(
        input_size=len(ALL_FEATURES), hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS, dropout=DROPOUT
    ).to(device)

    total_params = sum(p.numel() for p in model.parameters())
    print(f"  Device: {device}, Parameters: {total_params:,}")

    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE, weight_decay=1e-4)
    criterion = nn.HuberLoss(delta=0.5)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5, patience=7, min_lr=1e-5
    )

    # --- MLflow ---
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run(run_name=f"gru_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_params({
            "model_type": "GRU_Attention",
            "source_table": DL_FEATURES_TABLE,
            "sequence_length": SEQUENCE_LENGTH,
            "hidden_size": HIDDEN_SIZE,
            "num_layers": NUM_LAYERS,
            "dropout": DROPOUT,
            "learning_rate": LEARNING_RATE,
            "loss": "HuberLoss_0.5",
            "epochs_max": EPOCHS,
            "batch_size": BATCH_SIZE,
            "patience": PATIENCE,
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
            scheduler.step(avg_val)

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
            y_pred = np.clip(y_pred, 0, 1)
            y_train_pred = model(torch.FloatTensor(X_train).to(device)).cpu().numpy()
            y_train_pred = np.clip(y_train_pred, 0, 1)

        metrics = {
            "train_rmse": float(np.sqrt(mean_squared_error(y_train, y_train_pred))),
            "train_mae": float(mean_absolute_error(y_train, y_train_pred)),
            "train_r2": float(r2_score(y_train, y_train_pred)),
            "test_rmse": float(np.sqrt(mean_squared_error(y_test, y_pred))),
            "test_mae": float(mean_absolute_error(y_test, y_pred)),
            "test_r2": float(r2_score(y_test, y_pred)),
            "best_val_loss": float(best_val_loss),
            "epochs_trained": len(train_losses),
        }
        mlflow.log_metrics(metrics)

        print(f"\n  Train: RMSE={metrics['train_rmse']:.4f} MAE={metrics['train_mae']:.4f} R2={metrics['train_r2']:.4f}")
        print(f"  Test:  RMSE={metrics['test_rmse']:.4f} MAE={metrics['test_mae']:.4f} R2={metrics['test_r2']:.4f}")

        # Plots
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(train_losses, label='Train')
        ax.plot(val_losses, label='Val')
        ax.set_xlabel('Epoch'); ax.set_ylabel('Loss'); ax.set_title('GRU Training Loss'); ax.legend()
        mlflow.log_figure(fig, "loss_curve.png"); plt.close()

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(y_test, y_pred, alpha=0.5, s=15)
        ax.plot([0, 1], [0, 1], 'r--')
        ax.set_xlabel('Actual'); ax.set_ylabel('Predicted')
        ax.set_title(f'GRU: Actual vs Predicted (R2={metrics["test_r2"]:.3f})')
        mlflow.log_figure(fig, "actual_vs_predicted.png"); plt.close()

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(y_pred, y_test - y_pred, alpha=0.5, s=15)
        ax.axhline(0, color='r', linestyle='--')
        ax.set_xlabel('Predicted'); ax.set_ylabel('Residual'); ax.set_title('Residuals')
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
            json.dump({"all_features": ALL_FEATURES, "target": TARGET,
                        "sequence_length": SEQUENCE_LENGTH}, f, indent=2)
        mlflow.log_artifact("/tmp/feature_config.json", "artifacts")

        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, MODEL_NAME)
        print(f"  Model registered: {MODEL_NAME}")

        return model, scaler, df_pd_scaled, device


# ============================================================
# Step 4: Forecast 12 months
# ============================================================

def _scale_value(raw, feat_idx, scaler):
    r = scaler.data_range_[feat_idx]
    if r == 0:
        return 0.0
    return (raw - scaler.data_min_[feat_idx]) / r


def create_forecast_table(spark):
    schema = StructType([
        StructField("province_sk", LongType(), False),
        StructField("province_name", StringType(), False),
        StructField("region", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("year_month", IntegerType(), False),
        StructField("horizon_month", IntegerType(), False),
        StructField("predicted_hotness", DoubleType(), False),
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

        recent_hotness = list(group[TARGET].values[-max(SEQUENCE_LENGTH, 12):])

        for horizon in range(1, FORECAST_MONTHS + 1):
            total_m = last_date.year * 12 + last_date.month + horizon
            ty = (total_m - 1) // 12
            tm = (total_m - 1) % 12 + 1
            tym = int(f"{ty:04d}{tm:02d}")

            model.eval()
            with torch.no_grad():
                pred = model(torch.FloatTensor(last_seq).unsqueeze(0).to(device)).cpu().item()
                pred = float(np.clip(pred, 0, 1))

            results.append({
                'province_sk': province_sk, 'province_name': province_name,
                'region': region, 'year': ty, 'month': tm,
                'year_month': tym, 'horizon_month': horizon,
                'predicted_hotness': pred,
                'forecast_date': datetime.now().strftime('%Y-%m-%d'),
                'model_version': 'gru_3.0'
            })

            recent_hotness.append(pred)

            new_row = last_seq[-1].copy()

            # Update temporal
            new_row[fi["month_sin"]] = _scale_value(math.sin(2 * math.pi * tm / 12), fi["month_sin"], scaler)
            new_row[fi["month_cos"]] = _scale_value(math.cos(2 * math.pi * tm / 12), fi["month_cos"], scaler)

            # Update lag features
            new_row[fi["hotness_lag_3"]] = new_row[fi["hotness_lag_2"]]
            new_row[fi["hotness_lag_2"]] = new_row[fi["hotness_lag_1"]]
            new_row[fi["hotness_lag_1"]] = _scale_value(pred, fi["hotness_lag_1"], scaler)

            if len(recent_hotness) >= 12:
                new_row[fi["hotness_lag_12"]] = _scale_value(
                    recent_hotness[-12], fi["hotness_lag_12"], scaler
                )
            if len(recent_hotness) >= 3:
                new_row[fi["hotness_rolling_3m"]] = _scale_value(
                    float(np.mean(recent_hotness[-3:])), fi["hotness_rolling_3m"], scaler
                )

            lag1 = recent_hotness[-1] if len(recent_hotness) >= 1 else 0
            lag3 = recent_hotness[-3] if len(recent_hotness) >= 3 else lag1
            new_row[fi["hotness_momentum"]] = _scale_value(
                lag1 - lag3, fi["hotness_momentum"], scaler
            )

            last_seq = np.vstack([last_seq[1:], new_row])

    print(f"  Generated {len(results)} predictions")

    forecast_df = spark.createDataFrame(results)

    forecast_df.write.format("iceberg").mode("overwrite") \
        .save("gold.gold.province_month_forecast_lstm_next12")

    export_path = f"s3a://gold/ml_forecast/province_hotness_forecast_lstm_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    forecast_df.coalesce(1).write.mode("overwrite").parquet(export_path)
    print(f"  Exported: {export_path}")

    forecast_df.orderBy("province_name", "year_month").show(10, truncate=False)
    return forecast_df


# ============================================================
# Main
# ============================================================

def main():
    print("\n" + "=" * 80)
    print("ML PIPELINE: PROVINCE HOTNESS FORECASTING (GRU DEEP LEARNING)")
    print("=" * 80)
    print(f"Source: {DL_FEATURES_TABLE}")
    print(f"Features: {len(ALL_FEATURES)} (pre-computed, no inline calculation)")
    print(f"Start: {datetime.now()}")

    spark = create_spark_session()

    try:
        df = load_features(spark)
        model, scaler, df_pd, device = train_model(df)
        forecast_12_months(spark, model, scaler, df_pd, device)

        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED")
        print("=" * 80)
        print(f"  Model: {MODEL_NAME}")
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
