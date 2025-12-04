"""
ML Pipeline: Train XGBoost Time-Series Model & Forecast Province Hotness
========================================================================

Workflow:
1. Calculate hotness_score từ 14 metrics (5 weight groups)
2. Tạo lag features (lag_1/2/3/12, rolling_avg_3m)
3. Train XGBoost model với MLflow tracking
4. Forecast 12 tháng tiếp theo (recursive autoregressive)

Output:
- Model: province_hotness_forecaster (MLflow registry)
- Table: gold.gold.province_month_forecast_next12
- Artifacts: MLflow (plots, metrics, model)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import xgboost as xgb
import mlflow
import mlflow.xgboost
import matplotlib.pyplot as plt
import seaborn as sns
import os

# MLflow configuration
MLFLOW_TRACKING_URI = "postgresql://lakehouse_user:lakehouse_pass@postgres:5432/mlflow_db"
MLFLOW_ARTIFACT_URI = "s3://gold/mlflow/"
EXPERIMENT_NAME = "province_hotness_forecasting"

# Model configuration
MODEL_NAME = "province_hotness_forecaster"
FORECAST_MONTHS = 12
TRAIN_TEST_SPLIT = 0.7

# XGBoost hyperparameters (conservative)
XGBOOST_PARAMS = {
    "n_estimators": 200,
    "max_depth": 4,
    "learning_rate": 0.05,
    "min_child_weight": 5,
    "subsample": 0.7,
    "colsample_bytree": 0.7,
    "gamma": 0.1,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "random_state": 42,
    "objective": "reg:squarederror",
    "tree_method": "hist"
}

# Feature columns (12 features)
TEMPORAL_FEATURES = ["month", "month_sin", "month_cos"]
LAG_FEATURES = ["hotness_lag_1", "hotness_lag_2", "hotness_lag_3", "hotness_lag_12", "hotness_rolling_avg_3m"]

CURRENT_FEATURES = [
    # === Volume (2) ===
    "total_comments",          # ✅ Có
    "total_posts",             # ✅ Có
    
    # === Post Engagement (3) ===
    "total_post_likes",        # ✅ Có
    "total_post_shares",       # ✅ Có
    "total_post_saves",        # ✅ Có
    
    # === Sentiment (2) ===
    "positive_ratio",          # ✅ Có
    "avg_sentiment_score",     # ✅ Có
    
    # === NLP Richness (3) ===
    "avg_words_per_comment",   # ✅ Có
    "avg_unique_word_ratio",   # ✅ Có
    "total_positive_emojis",   # ✅ Có
]

ALL_FEATURES = TEMPORAL_FEATURES + LAG_FEATURES + CURRENT_FEATURES


def create_spark_session():
    """Initialize Spark session với Iceberg catalog"""
    return SparkSession.builder \
        .appName("ML_Province_Hotness_Pipeline") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gold.type", "hive") \
        .config("spark.sql.catalog.gold.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold/") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()


def calculate_hotness_score(spark):
    """
    Step 1: Calculate hotness_score từ 14 normalized metrics
    
    Weights:
    - Volume: 20% (comments 15% + posts 5%)
    - Post engagement: 25% (likes 10% + shares 10% + saves 5%)
    - Comment engagement: 10%
    - Sentiment: 30% (positive 12% + avg 10% + negative 5% + emoji 3%)
    - NLP richness: 15% (words 6% + unique 6% + exclamation 3%)
    """
    print("\n" + "="*80)
    print("STEP 1: CALCULATING HOTNESS SCORE")
    print("="*80)
    
    df = spark.table("gold.gold.province_month_features")
    print(f"✓ Loaded province_month_features: {df.count()} rows")
    
    # Window for percentile ranking within same month
    window_month = Window.partitionBy("year", "month")
    
    # Normalize 14 metrics using percent_rank (0-1)
    df = df.withColumn("norm_total_comments", F.percent_rank().over(window_month.orderBy("total_comments")))
    df = df.withColumn("norm_total_posts", F.percent_rank().over(window_month.orderBy("total_posts")))
    df = df.withColumn("norm_total_post_likes", F.percent_rank().over(window_month.orderBy("total_post_likes")))
    df = df.withColumn("norm_total_post_shares", F.percent_rank().over(window_month.orderBy("total_post_shares")))
    df = df.withColumn("norm_total_post_saves", F.percent_rank().over(window_month.orderBy("total_post_saves")))
    df = df.withColumn("norm_total_comment_likes", F.percent_rank().over(window_month.orderBy("total_comment_likes")))
    df = df.withColumn("norm_positive_ratio", F.percent_rank().over(window_month.orderBy("positive_ratio")))
    df = df.withColumn("norm_negative_ratio", F.percent_rank().over(window_month.orderBy("negative_ratio")))
    df = df.withColumn("norm_avg_words_per_comment", F.percent_rank().over(window_month.orderBy("avg_words_per_comment")))
    df = df.withColumn("norm_avg_unique_word_ratio", F.percent_rank().over(window_month.orderBy("avg_unique_word_ratio")))
    
    # Calculate exclamation ratio on-the-fly
    df = df.withColumn("exclamation_ratio",
        F.when(F.col("total_comments") > 0,
            F.col("total_exclamations") / F.col("total_comments")
        ).otherwise(0.0)
    )
    df = df.withColumn("norm_exclamation_ratio", F.percent_rank().over(window_month.orderBy("exclamation_ratio")))
    
    # Sentiment: scale avg_sentiment_score from [-1, 1] to [0, 1]
    df = df.withColumn("sentiment_scaled", (F.col("avg_sentiment_score") + 1) / 2)
    df = df.withColumn("norm_avg_sentiment", F.percent_rank().over(window_month.orderBy("sentiment_scaled")))
    
    # Emoji sentiment: (positive - negative) / total, handle division by zero
    df = df.withColumn("emoji_sentiment", 
        F.when(F.col("total_emojis") > 0,
            (F.col("total_positive_emojis") - F.col("total_negative_emojis")) / F.col("total_emojis")
        ).otherwise(0)
    )
    df = df.withColumn("norm_emoji_sentiment", F.percent_rank().over(window_month.orderBy("emoji_sentiment")))
    
    # Calculate weighted hotness_score
    df = df.withColumn("hotness_score",
        # Volume: 20%
        (F.col("norm_total_comments") * 0.15) +
        (F.col("norm_total_posts") * 0.05) +
        # Post engagement: 25%
        (F.col("norm_total_post_likes") * 0.10) +
        (F.col("norm_total_post_shares") * 0.10) +
        (F.col("norm_total_post_saves") * 0.05) +
        # Comment engagement: 10%
        (F.col("norm_total_comment_likes") * 0.10) +
        # Sentiment: 30%
        (F.col("norm_positive_ratio") * 0.12) +
        (F.col("norm_avg_sentiment") * 0.10) +
        ((1 - F.col("norm_negative_ratio")) * 0.05) +
        (F.col("norm_emoji_sentiment") * 0.03) +
        # NLP richness: 15%
        (F.col("norm_avg_words_per_comment") * 0.06) +
        (F.col("norm_avg_unique_word_ratio") * 0.06) +
        (F.col("norm_exclamation_ratio") * 0.03)
    )
    
    # Clamp to [0, 1]
    df = df.withColumn("hotness_score", 
        F.when(F.col("hotness_score") < 0, 0)
        .when(F.col("hotness_score") > 1, 1)
        .otherwise(F.col("hotness_score"))
    )
    
    # Drop intermediate columns
    norm_cols = [c for c in df.columns if c.startswith("norm_")] + ["sentiment_scaled", "emoji_sentiment", "exclamation_ratio"]
    df = df.drop(*norm_cols)
    
    print(f"✓ Calculated hotness_score (min-max): {df.agg(F.min('hotness_score'), F.max('hotness_score')).first()}")
    
    return df


def create_lag_features(df):
    """
    Step 2: Create lag features for time-series modeling
    
    Features:
    - hotness_lag_1, lag_2, lag_3 (short-term trend)
    - hotness_lag_12 (seasonality)
    - hotness_rolling_avg_3m (smooth noise)
    """
    print("\n" + "="*80)
    print("STEP 2: CREATING LAG FEATURES")
    print("="*80)
    
    window_province = Window.partitionBy("province_sk").orderBy("year_month")
    
    # Create lag features
    df = df.withColumn("hotness_lag_1", F.lag("hotness_score", 1).over(window_province))
    df = df.withColumn("hotness_lag_2", F.lag("hotness_score", 2).over(window_province))
    df = df.withColumn("hotness_lag_3", F.lag("hotness_score", 3).over(window_province))
    df = df.withColumn("hotness_lag_12", F.lag("hotness_score", 12).over(window_province))
    
    # Rolling average of last 3 months
    window_rolling = Window.partitionBy("province_sk").orderBy("year_month").rowsBetween(-2, 0)
    df = df.withColumn("hotness_rolling_avg_3m", F.avg("hotness_score").over(window_rolling))
    
    # Drop rows with missing lags
    before_count = df.count()
    df = df.dropna(subset=["hotness_lag_1", "hotness_lag_2", "hotness_lag_3", "hotness_lag_12", "hotness_rolling_avg_3m"])
    after_count = df.count()
    
    print(f"✓ Created 5 lag features")
    print(f"✓ Dropped {before_count - after_count} rows with missing lags")
    print(f"✓ Final dataset: {after_count} rows")
    
    return df


def prepare_features(df):
    """Add temporal features (month_sin, month_cos)"""
    print("\n" + "="*80)
    print("PREPARING FEATURES FOR TRAINING")
    print("="*80)
    
    df = df.withColumn("month_sin", F.sin(2 * np.pi * F.col("month") / 12))
    df = df.withColumn("month_cos", F.cos(2 * np.pi * F.col("month") / 12))
    
    print(f"✓ Added temporal features: month_sin, month_cos")
    print(f"✓ Total features: {len(ALL_FEATURES)}")
    print(f"  - Temporal: {TEMPORAL_FEATURES}")
    print(f"  - Lag: {LAG_FEATURES}")
    print(f"  - Current: {CURRENT_FEATURES}")
    
    return df


def train_xgboost_model(df):
    """
    Step 3: Train XGBoost model with MLflow tracking
    
    Split: 70/30 time-based
    Features: 12 total (3 temporal + 5 lag + 4 current)
    """
    print("\n" + "="*80)
    print("STEP 3: TRAINING XGBOOST MODEL")
    print("="*80)
    
    # Convert to Pandas
    df_pd = df.select(ALL_FEATURES + ["hotness_score", "year_month", "province_sk", "province_name"]).toPandas()
    
    # Time-based split
    split_index = int(len(df_pd) * TRAIN_TEST_SPLIT)
    df_pd = df_pd.sort_values("year_month")
    
    train_df = df_pd.iloc[:split_index]
    test_df = df_pd.iloc[split_index:]
    
    X_train = train_df[ALL_FEATURES]
    y_train = train_df["hotness_score"]
    X_test = test_df[ALL_FEATURES]
    y_test = test_df["hotness_score"]
    
    print(f"✓ Train set: {len(train_df)} rows (до {train_df['year_month'].max()})")
    print(f"✓ Test set: {len(test_df)} rows (từ {test_df['year_month'].min()})")
    
    # Start MLflow run
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    with mlflow.start_run(run_name=f"xgboost_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        # Log parameters
        mlflow.log_params(XGBOOST_PARAMS)
        mlflow.log_param("train_test_split", TRAIN_TEST_SPLIT)
        mlflow.log_param("num_features", len(ALL_FEATURES))
        mlflow.log_param("train_size", len(train_df))
        mlflow.log_param("test_size", len(test_df))
        
        # Train model
        print("\n Training XGBoost...")
        model = xgb.XGBRegressor(**XGBOOST_PARAMS)
        model.fit(X_train, y_train)
        
        # Predictions
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)
        
        # Metrics
        train_rmse = np.sqrt(mean_squared_error(y_train, y_train_pred))
        train_mae = mean_absolute_error(y_train, y_train_pred)
        train_r2 = r2_score(y_train, y_train_pred)
        
        test_rmse = np.sqrt(mean_squared_error(y_test, y_test_pred))
        test_mae = mean_absolute_error(y_test, y_test_pred)
        test_r2 = r2_score(y_test, y_test_pred)
        
        mlflow.log_metrics({
            "train_rmse": train_rmse,
            "train_mae": train_mae,
            "train_r2": train_r2,
            "test_rmse": test_rmse,
            "test_mae": test_mae,
            "test_r2": test_r2
        })
        
        print(f"\n✓ Training Metrics:")
        print(f"  RMSE: {train_rmse:.4f} | MAE: {train_mae:.4f} | R²: {train_r2:.4f}")
        print(f"✓ Test Metrics:")
        print(f"  RMSE: {test_rmse:.4f} | MAE: {test_mae:.4f} | R²: {test_r2:.4f}")
        
        # Plot 1: Feature Importance
        fig, ax = plt.subplots(figsize=(10, 6))
        importance_df = pd.DataFrame({
            'feature': ALL_FEATURES,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        sns.barplot(data=importance_df, x='importance', y='feature', ax=ax)
        ax.set_title('Feature Importance')
        mlflow.log_figure(fig, "feature_importance.png")
        plt.close()
        
        # Plot 2: Actual vs Predicted (Test)
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.scatter(y_test, y_test_pred, alpha=0.5)
        ax.plot([0, 1], [0, 1], 'r--')
        ax.set_xlabel('Actual Hotness')
        ax.set_ylabel('Predicted Hotness')
        ax.set_title('Test Set: Actual vs Predicted')
        mlflow.log_figure(fig, "actual_vs_predicted.png")
        plt.close()
        
        # Plot 3: Residuals
        fig, ax = plt.subplots(figsize=(10, 6))
        residuals = y_test - y_test_pred
        ax.scatter(y_test_pred, residuals, alpha=0.5)
        ax.axhline(y=0, color='r', linestyle='--')
        ax.set_xlabel('Predicted Hotness')
        ax.set_ylabel('Residuals')
        ax.set_title('Residual Plot')
        mlflow.log_figure(fig, "residuals.png")
        plt.close()
        
        # Plot 4: Time series comparison
        fig, ax = plt.subplots(figsize=(14, 6))
        test_df_plot = test_df.copy()
        test_df_plot['predicted'] = y_test_pred
        # Show first province as example
        province_sample = test_df_plot['province_sk'].iloc[0]
        sample_data = test_df_plot[test_df_plot['province_sk'] == province_sample].sort_values('year_month')
        ax.plot(sample_data['year_month'], sample_data['hotness_score'], label='Actual', marker='o')
        ax.plot(sample_data['year_month'], sample_data['predicted'], label='Predicted', marker='x')
        ax.set_title(f'Time Series: {sample_data["province_name"].iloc[0]}')
        ax.set_xlabel('Year-Month')
        ax.set_ylabel('Hotness Score')
        ax.legend()
        plt.xticks(rotation=45)
        mlflow.log_figure(fig, "time_series_sample.png")
        plt.close()
        
        # Log model
        mlflow.xgboost.log_model(model, "model")
        
        # Register model
        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, MODEL_NAME)
        
        print(f"✓ Model registered: {MODEL_NAME}")
        print(f"✓ MLflow Run ID: {mlflow.active_run().info.run_id}")
        
        return model, test_df


def forecast_12_months(spark, model, df_with_lags):
    """
    Step 4: Forecast 12 months ahead using recursive autoregressive
    
    Strategy:
    - Month 1: Use actual lags
    - Month 2+: Use previous predictions as lags
    - Current metrics: Use last known values (2025-11)
    - Rolling avg: Recalculate each step
    """
    print("\n" + "="*80)
    print("STEP 4: FORECASTING 12 MONTHS AHEAD")
    print("="*80)
    
    # Get latest data for each province
    latest_df = df_with_lags.groupBy("province_sk").agg(F.max("year_month").alias("max_year_month"))
    latest_data = df_with_lags.join(latest_df, 
        (df_with_lags.province_sk == latest_df.province_sk) & 
        (df_with_lags.year_month == latest_df.max_year_month)
    ).select(df_with_lags["*"]).toPandas()
    
    print(f"✓ Loaded latest data for {len(latest_data)} provinces (as of {latest_data['year_month'].max()})")
    
    # Prepare forecast DataFrame
    forecast_results = []
    
    for _, row in latest_data.iterrows():
        province_sk = row['province_sk']
        province_name = row['province_name']
        region = row['region']
        
        # Initialize with last known values
        last_hotness = row['hotness_score']
        lag_history = [
            row['hotness_lag_12'],  # 12 months ago
            row['hotness_lag_3'],   # 3 months ago
            row['hotness_lag_2'],   # 2 months ago
            row['hotness_lag_1'],   # 1 month ago
            last_hotness            # current (will become lag_1 for next prediction)
        ]
        
        # Current metrics (last known values) - all 10 features
        current_metrics = {
            'total_comments': row['total_comments'],
            'total_posts': row['total_posts'],
            'total_post_likes': row['total_post_likes'],
            'total_post_shares': row['total_post_shares'],
            'total_post_saves': row['total_post_saves'],
            'positive_ratio': row['positive_ratio'],
            'avg_sentiment_score': row['avg_sentiment_score'],
            'avg_words_per_comment': row['avg_words_per_comment'],
            'avg_unique_word_ratio': row['avg_unique_word_ratio'],
            'total_positive_emojis': row['total_positive_emojis']
        }
        
        # Forecast 12 months
        for horizon in range(1, FORECAST_MONTHS + 1):
            # Calculate target month
            last_date = datetime.strptime(row['year_month'], '%Y-%m')
            target_date = last_date + timedelta(days=30 * horizon)
            target_year = target_date.year
            target_month = target_date.month
            target_year_month = target_date.strftime('%Y-%m')
            
            # Prepare features
            features = {
                'month': target_month,
                'month_sin': np.sin(2 * np.pi * target_month / 12),
                'month_cos': np.cos(2 * np.pi * target_month / 12),
                'hotness_lag_1': lag_history[-1],
                'hotness_lag_2': lag_history[-2],
                'hotness_lag_3': lag_history[-3],
                'hotness_lag_12': lag_history[-12] if len(lag_history) >= 12 else lag_history[0],
                'hotness_rolling_avg_3m': np.mean(lag_history[-3:]),
                **current_metrics
            }
            
            # Predict
            X = pd.DataFrame([features])[ALL_FEATURES]
            predicted_hotness = model.predict(X)[0]
            predicted_hotness = np.clip(predicted_hotness, 0, 1)
            
            # Store result
            forecast_results.append({
                'province_sk': province_sk,
                'province_name': province_name,
                'region': region,
                'year': target_year,
                'month': target_month,
                'year_month': target_year_month,
                'horizon_month': horizon,
                'predicted_hotness': float(predicted_hotness),
                'forecast_date': datetime.now().strftime('%Y-%m-%d'),
                'model_version': '1.0'
            })
            
            # Update lag history for next iteration
            lag_history.append(predicted_hotness)
    
    print(f"✓ Generated {len(forecast_results)} predictions ({len(latest_data)} provinces × {FORECAST_MONTHS} months)")
    
    # Convert to Spark DataFrame
    forecast_df = spark.createDataFrame(forecast_results)
    
    # Save to Iceberg table
    forecast_df.writeTo("gold.gold.province_month_forecast_next12") \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .partitionedBy("year", "month") \
        .createOrReplace()
    
    print(f"✓ Saved forecast to table: gold.gold.province_month_forecast_next12")
    
    # Show sample
    print("\nSample forecast (first 10 rows):")
    forecast_df.orderBy("province_name", "year_month").show(10, truncate=False)
    
    return forecast_df


def main():
    """Main pipeline execution"""
    print("\n" + "="*80)
    print("ML PIPELINE: PROVINCE HOTNESS FORECASTING")
    print("="*80)
    print(f"Start time: {datetime.now()}")
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Step 1: Calculate hotness score
        df = calculate_hotness_score(spark)
        
        # Step 2: Create lag features
        df = create_lag_features(df)
        
        # Prepare features
        df = prepare_features(df)
        
        # Step 3: Train model
        model, test_df = train_xgboost_model(df)
        
        # Step 4: Forecast 12 months
        forecast_df = forecast_12_months(spark, model, df)
        
        print("\n" + "="*80)
        print("✓ PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*80)
        print(f"End time: {datetime.now()}")
        print(f"\nOutputs:")
        print(f"  - Model: {MODEL_NAME} (MLflow Registry)")
        print(f"  - Forecast Table: gold.gold.province_month_forecast_next12")
        print(f"  - MLflow URI: {MLFLOW_TRACKING_URI}")
        print(f"  - Experiment: {EXPERIMENT_NAME}")
        
    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
