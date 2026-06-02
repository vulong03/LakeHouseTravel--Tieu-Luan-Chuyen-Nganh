"""
Gold Layer — fact_province_month_dl_features
=============================================
ML-optimized fact table: aggregate TikTok engagement, NLP sentiment,
and Booking.com hotel reviews into ~40 curated features at province-month grain.

Designed for Deep Learning (GRU/LSTM) but usable by any model.

Source:  3 existing Gold fact tables + dimensions
Target:  gold.gold.fact_province_month_dl_features
Grain:   1 row = 1 province x 1 month
Output:  ~40 features + hotness_score + lag features (ready for ML)
"""

import sys
import math
from datetime import datetime

sys.path.append("/opt/spark/jobs")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, DoubleType,
    StringType, TimestampType, BooleanType,
)
import numpy as np

from config import (
    FACT_COMMENT_NLP_TABLE, FACT_COMMENT_NLP_V2_TABLE,
    FACT_PROVINCE_CONTENT_TABLE, FACT_HOTEL_REVIEW_TABLE,
    DIM_PROVINCE_TABLE, DIM_DATE_TABLE, DIM_POST_TABLE,
    DIM_HOTEL_TABLE, DIM_COUNTRY_TABLE, DIM_TRAVEL_TYPE_TABLE,
    GOLD_CATALOG, GOLD_DATABASE, GOLD_TABLE, GOLD_TABLE_FULL,
    PARQUET_EXPORT_PATH, SOURCE_DESCRIPTION,
    HOTNESS_WEIGHTS, VIETNAM_COUNTRY_NAME,
    PEAK_SEASON_MONTHS, LOW_SCORE_THRESHOLD, HIGH_SCORE_THRESHOLD,
)
from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.gold_job_logger import get_gold_logger


# ============================================================
# Table Setup
# ============================================================

def create_gold_database(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")


def create_output_table(spark: SparkSession) -> None:
    schema = StructType([
        # Keys
        StructField("province_sk", IntegerType(), False),
        StructField("province_name", StringType(), False),
        StructField("region", StringType(), True),
        StructField("year_month", IntegerType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),

        # Group 1: Volume & Activity (6)
        StructField("total_posts", LongType(), True),
        StructField("total_comments", LongType(), True),
        StructField("total_hotel_reviews", LongType(), True),
        StructField("unique_authors", LongType(), True),
        StructField("comments_per_post", DoubleType(), True),
        StructField("post_frequency", DoubleType(), True),

        # Group 2: TikTok Engagement (7)
        StructField("avg_likes_per_post", DoubleType(), True),
        StructField("avg_saves_per_post", DoubleType(), True),
        StructField("avg_shares_per_post", DoubleType(), True),
        StructField("median_likes_per_post", DoubleType(), True),
        StructField("p90_likes_per_post", DoubleType(), True),
        StructField("viral_post_ratio", DoubleType(), True),
        StructField("engagement_score", LongType(), True),

        # Group 3: NLP & Sentiment (10)
        StructField("avg_sentiment", DoubleType(), True),
        StructField("sentiment_std", DoubleType(), True),
        StructField("positive_ratio", DoubleType(), True),
        StructField("negative_ratio", DoubleType(), True),
        StructField("sentiment_polarity", DoubleType(), True),
        StructField("avg_word_count", DoubleType(), True),
        StructField("word_count_std", DoubleType(), True),
        StructField("avg_unique_word_ratio", DoubleType(), True),
        StructField("emoji_sentiment_ratio", DoubleType(), True),
        StructField("reply_ratio", DoubleType(), True),

        # Group 3b: Aspect scores (from NLP v2, 0 if not available) (6)
        StructField("avg_aspect_scenery", DoubleType(), True),
        StructField("avg_aspect_food", DoubleType(), True),
        StructField("avg_aspect_price", DoubleType(), True),
        StructField("avg_aspect_service", DoubleType(), True),
        StructField("avg_aspect_transport", DoubleType(), True),
        StructField("avg_aspect_accommodation", DoubleType(), True),

        # Group 4: Hotel / Booking (12)
        StructField("avg_hotel_score", DoubleType(), True),
        StructField("hotel_score_std", DoubleType(), True),
        StructField("hotel_review_volume", LongType(), True),
        StructField("high_score_ratio", DoubleType(), True),
        StructField("low_score_ratio", DoubleType(), True),
        StructField("unique_reviewer_countries", LongType(), True),
        StructField("domestic_review_ratio", DoubleType(), True),
        StructField("couple_ratio", DoubleType(), True),
        StructField("family_ratio", DoubleType(), True),
        StructField("business_ratio", DoubleType(), True),
        StructField("solo_ratio", DoubleType(), True),
        StructField("hotel_vol_growth", DoubleType(), True),

        # Group 5: Temporal (4)
        StructField("month_sin", DoubleType(), True),
        StructField("month_cos", DoubleType(), True),
        StructField("is_peak_season", BooleanType(), True),
        StructField("hotness_score", DoubleType(), True),

        # Group 6: Lags (12)
        StructField("hotness_lag_1", DoubleType(), True),
        StructField("hotness_lag_2", DoubleType(), True),
        StructField("hotness_lag_3", DoubleType(), True),
        StructField("hotness_lag_12", DoubleType(), True),
        StructField("hotness_rolling_3m", DoubleType(), True),
        StructField("hotness_momentum", DoubleType(), True),
        StructField("hotel_vol_lag_1", LongType(), True),
        StructField("hotel_vol_lag_2", LongType(), True),
        StructField("hotel_vol_lag_3", LongType(), True),
        StructField("hotel_vol_lag_12", LongType(), True),
        StructField("hotel_vol_rolling_3m", DoubleType(), True),
        StructField("hotel_vol_momentum", LongType(), True),

        # Metadata
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ])

    create_iceberg_table_if_not_exists(
        spark=spark,
        database=GOLD_DATABASE,
        table_name=GOLD_TABLE,
        schema=schema,
        partition_by=["year"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
        catalog=GOLD_CATALOG,
    )


# ============================================================
# Step 1: Aggregate TikTok post engagement → province-month
# ============================================================

def aggregate_post_engagement(spark: SparkSession) -> DataFrame:
    """
    From fact_province_content_engagement (grain: 1 post),
    aggregate to province x month.
    """
    print("\n[1/5] Aggregating TikTok post engagement...")

    df_posts = spark.table(FACT_PROVINCE_CONTENT_TABLE)
    df_dim_post = spark.table(DIM_POST_TABLE).select("post_sk", "post_date_sk")
    df_dates = spark.table(DIM_DATE_TABLE).select("date_sk", "year", "month", "year_month")

    df = (
        df_posts
        .join(df_dim_post, "post_sk", "inner")
        .join(df_dates, df_dim_post.post_date_sk == df_dates.date_sk, "inner")
        .where(F.col("province_sk").isNotNull())
    )

    # p90 threshold per province-month for viral detection
    df_with_p90 = df.withColumn(
        "_p90_likes",
        F.expr("percentile_approx(likes, 0.9)").over(
            Window.partitionBy("province_sk", "year_month")
        )
    )

    agg = df_with_p90.groupBy("province_sk", "year_month", "year", "month").agg(
        F.countDistinct("post_sk").alias("total_posts"),
        F.countDistinct("author_sk").alias("unique_authors"),

        F.avg("likes").alias("avg_likes_per_post"),
        F.avg("saves").alias("avg_saves_per_post"),
        F.avg("shares").alias("avg_shares_per_post"),
        F.expr("percentile_approx(likes, 0.5)").cast("double").alias("median_likes_per_post"),
        F.expr("percentile_approx(likes, 0.9)").cast("double").alias("p90_likes_per_post"),

        F.sum(
            F.col("likes") + F.col("comments") + F.col("saves") + F.col("shares")
        ).alias("engagement_score"),

        F.avg(
            F.when(F.col("likes") > F.col("_p90_likes"), 1).otherwise(0)
        ).alias("viral_post_ratio"),
    )

    row_count = agg.count()
    print(f"   Post engagement: {row_count} province-months")
    return agg


# ============================================================
# Step 2: Aggregate NLP comment features → province-month
# ============================================================

def _try_nlp_v2(spark: SparkSession) -> bool:
    """Check if NLP v2 table exists and has data."""
    try:
        if spark.catalog.tableExists(FACT_COMMENT_NLP_V2_TABLE):
            count = spark.table(FACT_COMMENT_NLP_V2_TABLE).limit(1).count()
            return count > 0
    except Exception:
        pass
    return False


def aggregate_nlp_comments(spark: SparkSession) -> DataFrame:
    """
    Aggregate NLP features to province x month.
    Prefers NLP v2 (PhoBERT continuous sentiment + aspects) if available,
    falls back to v1 (underthesea 3-level).
    """
    print("\n[2/5] Aggregating NLP comment features...")

    use_v2 = _try_nlp_v2(spark)
    df_dim_post = spark.table(DIM_POST_TABLE).select("post_sk", "post_date_sk")
    df_dates = spark.table(DIM_DATE_TABLE).select("date_sk", "year_month")

    if use_v2:
        print("   Using NLP v2 (PhoBERT: continuous sentiment + aspects)")
        df_comments = spark.table(FACT_COMMENT_NLP_V2_TABLE)

        df = (
            df_comments
            .join(df_dim_post, "post_sk", "inner")
            .join(df_dates, df_dim_post.post_date_sk == df_dates.date_sk, "inner")
            .where(F.col("province_sk").isNotNull())
        )

        total_col = F.count("comment_sk")

        agg = df.groupBy("province_sk", "year_month").agg(
            F.count("comment_sk").alias("total_comments"),

            F.avg("sentiment_score").alias("avg_sentiment"),
            F.stddev("sentiment_score").alias("sentiment_std"),
            (F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0))
             / total_col).alias("positive_ratio"),
            (F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0))
             / total_col).alias("negative_ratio"),

            F.avg("word_count").alias("avg_word_count"),
            F.stddev(F.col("word_count").cast("double")).alias("word_count_std"),
            F.lit(0.0).alias("avg_unique_word_ratio"),

            F.lit(0.0).alias("emoji_sentiment_ratio"),

            (F.sum(F.when(F.col("comment_level") == 2, 1).otherwise(0))
             / total_col).alias("reply_ratio"),

            F.sum("comment_likes").alias("_total_comment_likes"),

            # Aspect scores (v2 only)
            F.avg("aspect_scenery").alias("avg_aspect_scenery"),
            F.avg("aspect_food").alias("avg_aspect_food"),
            F.avg("aspect_price").alias("avg_aspect_price"),
            F.avg("aspect_service").alias("avg_aspect_service"),
            F.avg("aspect_transport").alias("avg_aspect_transport"),
            F.avg("aspect_accommodation").alias("avg_aspect_accommodation"),
        )
    else:
        print("   Using NLP v1 (underthesea: 3-level sentiment, no aspects)")
        df_comments = spark.table(FACT_COMMENT_NLP_TABLE)

        df = (
            df_comments
            .join(df_dim_post, "post_sk", "inner")
            .join(df_dates, df_dim_post.post_date_sk == df_dates.date_sk, "inner")
            .where(F.col("province_sk").isNotNull())
        )

        total_col = F.count("comment_sk")

        agg = df.groupBy("province_sk", "year_month").agg(
            F.count("comment_sk").alias("total_comments"),

            F.avg("sentiment_score").alias("avg_sentiment"),
            F.stddev("sentiment_score").alias("sentiment_std"),
            (F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0))
             / total_col).alias("positive_ratio"),
            (F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0))
             / total_col).alias("negative_ratio"),

            F.avg("word_count").alias("avg_word_count"),
            F.stddev(F.col("word_count").cast("double")).alias("word_count_std"),
            F.avg("unique_word_ratio").alias("avg_unique_word_ratio"),

            F.avg(
                F.when(
                    F.col("emoji_count") > 0,
                    (F.col("positive_emoji_count") - F.col("negative_emoji_count"))
                    / F.col("emoji_count")
                ).otherwise(0)
            ).alias("emoji_sentiment_ratio"),

            (F.sum(F.when(F.col("comment_level") == 2, 1).otherwise(0))
             / total_col).alias("reply_ratio"),

            F.sum("comment_likes").alias("_total_comment_likes"),

            # No aspect data in v1
            F.lit(0.0).alias("avg_aspect_scenery"),
            F.lit(0.0).alias("avg_aspect_food"),
            F.lit(0.0).alias("avg_aspect_price"),
            F.lit(0.0).alias("avg_aspect_service"),
            F.lit(0.0).alias("avg_aspect_transport"),
            F.lit(0.0).alias("avg_aspect_accommodation"),
        )

    agg = agg.withColumn("sentiment_polarity",
        F.col("positive_ratio") - F.col("negative_ratio")
    )

    row_count = agg.count()
    print(f"   NLP comments: {row_count} province-months")
    return agg


# ============================================================
# Step 3: Aggregate hotel reviews → province-month
# ============================================================

def aggregate_hotel_reviews(spark: SparkSession) -> DataFrame:
    """
    From fact_hotel_review_daily (grain: 1 review),
    aggregate hotel quality features to province x month.
    """
    print("\n[3/5] Aggregating hotel review features...")

    df_reviews = spark.table(FACT_HOTEL_REVIEW_TABLE)
    df_hotels = spark.table(DIM_HOTEL_TABLE).select("hotel_sk", "province_sk")
    df_dates = spark.table(DIM_DATE_TABLE).select("date_sk", "year_month")
    df_countries = spark.table(DIM_COUNTRY_TABLE).select("country_sk", "country_name")
    df_travel = spark.table(DIM_TRAVEL_TYPE_TABLE).select("traveler_type_sk", "traveler_type_name")

    df = (
        df_reviews
        .join(df_hotels, "hotel_sk", "inner")
        .join(df_dates, df_reviews.stay_date_sk == df_dates.date_sk, "left")
        .join(df_countries, "country_sk", "left")
        .join(df_travel, "traveler_type_sk", "left")
        .where(F.col("province_sk").isNotNull())
    )

    total_col = F.count("fact_id")

    agg = df.groupBy("province_sk", "year_month").agg(
        F.count("fact_id").alias("hotel_review_volume"),
        F.avg("review_score").alias("avg_hotel_score"),
        F.stddev("review_score").alias("hotel_score_std"),

        (F.sum(F.when(F.col("review_score") >= HIGH_SCORE_THRESHOLD, 1).otherwise(0))
         / total_col).alias("high_score_ratio"),
        (F.sum(F.when(F.col("review_score") < LOW_SCORE_THRESHOLD, 1).otherwise(0))
         / total_col).alias("low_score_ratio"),

        F.countDistinct("country_sk").alias("unique_reviewer_countries"),

        (F.sum(F.when(F.col("country_name") == VIETNAM_COUNTRY_NAME, 1).otherwise(0))
         / total_col).alias("domestic_review_ratio"),

        # Traveler type ratios
        (F.sum(F.when(F.col("traveler_type_name").contains("Cặp đôi"), 1).otherwise(0))
         / total_col).alias("couple_ratio"),
        (F.sum(F.when(F.col("traveler_type_name").contains("Gia đình"), 1).otherwise(0))
         / total_col).alias("family_ratio"),
        (F.sum(F.when(F.col("traveler_type_name").contains("Công tác"), 1).otherwise(0))
         / total_col).alias("business_ratio"),
        (F.sum(F.when(F.col("traveler_type_name").contains("Một mình"), 1).otherwise(0))
         / total_col).alias("solo_ratio"),
    )

    row_count = agg.count()
    print(f"   Hotel reviews: {row_count} province-months")
    return agg


# ============================================================
# Step 4: Join all aggregations + compute derived features
# ============================================================

def build_combined_features(
    spark: SparkSession,
    df_posts: DataFrame,
    df_comments: DataFrame,
    df_hotels: DataFrame,
) -> DataFrame:
    """
    Join 3 aggregation DataFrames on (province_sk, year_month),
    add province info, temporal features, hotness score, and lags.
    """
    print("\n[4/5] Joining aggregations and computing derived features...")

    # --- Join ---
    df = df_posts.join(df_comments, on=["province_sk", "year_month"], how="left")
    df = df.join(df_hotels, on=["province_sk", "year_month"], how="left")
    df = df.withColumn("total_hotel_reviews", F.col("hotel_review_volume"))

    # --- Province info ---
    df_province = spark.table(DIM_PROVINCE_TABLE).select(
        "province_sk", "province_name", "region"
    )
    df = df.join(df_province, "province_sk", "left")

    # --- Fill nulls for provinces without hotel/comment data ---
    fill_zero_cols = [
        "total_comments", "total_hotel_reviews",
        "avg_sentiment", "sentiment_std", "positive_ratio", "negative_ratio",
        "sentiment_polarity", "avg_word_count", "word_count_std",
        "avg_unique_word_ratio", "emoji_sentiment_ratio", "reply_ratio",
        "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price",
        "avg_aspect_service", "avg_aspect_transport", "avg_aspect_accommodation",
        "_total_comment_likes",
        "hotel_review_volume", "avg_hotel_score", "hotel_score_std",
        "high_score_ratio", "low_score_ratio",
        "unique_reviewer_countries", "domestic_review_ratio",
        "couple_ratio", "family_ratio", "business_ratio", "solo_ratio",
    ]
    df = df.fillna(0, subset=fill_zero_cols)

    # --- Derived: comments_per_post, post_frequency ---
    df = df.withColumn("comments_per_post",
        F.when(F.col("total_posts") > 0,
               F.col("total_comments") / F.col("total_posts")
        ).otherwise(0)
    )

    days_in_month_expr = F.dayofmonth(
        F.last_day(F.to_date(F.concat(
            (F.col("year_month") / 100).cast("int").cast("string"),
            F.lit("-"),
            F.lpad((F.col("year_month") % 100).cast("string"), 2, "0"),
            F.lit("-01")
        )))
    )
    df = df.withColumn("post_frequency",
        F.when(days_in_month_expr > 0,
               F.col("total_posts") / days_in_month_expr
        ).otherwise(0)
    )

    # --- Temporal features ---
    df = df.withColumn("month_sin", F.sin(2 * math.pi * F.col("month") / 12))
    df = df.withColumn("month_cos", F.cos(2 * math.pi * F.col("month") / 12))

    peak_months = F.array(*[F.lit(m) for m in PEAK_SEASON_MONTHS])
    df = df.withColumn("is_peak_season", F.array_contains(peak_months, F.col("month")))

    # --- Hotness score (same weighted formula as XGBoost/RF) ---
    window_month = Window.partitionBy("year", "month")

    norm_volume = (
        F.percent_rank().over(window_month.orderBy("total_posts")) * 0.6
        + F.percent_rank().over(window_month.orderBy("total_comments")) * 0.4
    )
    norm_engagement = (
        F.percent_rank().over(window_month.orderBy("avg_likes_per_post")) * 0.45
        + F.percent_rank().over(window_month.orderBy("avg_saves_per_post")) * 0.35
        + F.percent_rank().over(window_month.orderBy("_total_comment_likes")) * 0.20
    )
    norm_sentiment = (
        F.percent_rank().over(window_month.orderBy("positive_ratio")) * 0.5
        + F.percent_rank().over(window_month.orderBy("avg_sentiment")) * 0.3
        + (1 - F.percent_rank().over(window_month.orderBy("negative_ratio"))) * 0.2
    )
    norm_emoji = (
        F.percent_rank().over(window_month.orderBy("emoji_sentiment_ratio"))
    )
    norm_nlp = (
        F.percent_rank().over(window_month.orderBy("avg_word_count")) * 0.45
        + F.percent_rank().over(window_month.orderBy("avg_unique_word_ratio")) * 0.45
        + F.percent_rank().over(window_month.orderBy(
            F.when(F.col("avg_word_count") > 0,
                   F.col("word_count_std") / F.col("avg_word_count")
            ).otherwise(0)
        )) * 0.10
    )

    w = HOTNESS_WEIGHTS
    df = df.withColumn("hotness_score",
        F.greatest(F.lit(0.0), F.least(F.lit(1.0),
            norm_volume * w["volume"]
            + norm_engagement * w["engagement"]
            + norm_sentiment * w["sentiment"]
            + norm_emoji * w["emoji_vibe"]
            + norm_nlp * w["nlp_richness"]
        ))
    )

    # --- Lag features ---
    window_province = Window.partitionBy("province_sk").orderBy("year_month")

    df = df.withColumn("hotness_lag_1", F.lag("hotness_score", 1).over(window_province))
    df = df.withColumn("hotness_lag_2", F.lag("hotness_score", 2).over(window_province))
    df = df.withColumn("hotness_lag_3", F.lag("hotness_score", 3).over(window_province))
    df = df.withColumn("hotness_lag_12", F.lag("hotness_score", 12).over(window_province))

    window_rolling = Window.partitionBy("province_sk").orderBy("year_month").rowsBetween(-3, -1)
    df = df.withColumn("hotness_rolling_3m", F.avg("hotness_score").over(window_rolling))

    df = df.withColumn("hotness_momentum",
        F.col("hotness_lag_1") - F.col("hotness_lag_3")
    )

    # Hotel review volume lags
    df = df.withColumn("hotel_vol_lag_1", F.lag("hotel_review_volume", 1).over(window_province))
    df = df.withColumn("hotel_vol_lag_2", F.lag("hotel_review_volume", 2).over(window_province))
    df = df.withColumn("hotel_vol_lag_3", F.lag("hotel_review_volume", 3).over(window_province))
    df = df.withColumn("hotel_vol_lag_12", F.lag("hotel_review_volume", 12).over(window_province))

    df = df.withColumn("hotel_vol_rolling_3m", F.avg("hotel_review_volume").over(window_rolling))

    df = df.withColumn("hotel_vol_momentum",
        F.col("hotel_vol_lag_1") - F.col("hotel_vol_lag_3")
    )

    df = df.withColumn("hotel_vol_growth",
        F.when(F.col("hotel_vol_lag_1") > 0,
               (F.col("hotel_review_volume") - F.col("hotel_vol_lag_1")) / F.col("hotel_vol_lag_1")
        ).otherwise(F.lit(0.0))
    )

    # --- Drop internal columns ---
    df = df.drop("_total_comment_likes")

    # --- Metadata ---
    current_ts = F.current_timestamp()
    df = df.withColumn("created_at", current_ts)
    df = df.withColumn("updated_at", current_ts)

    # --- Final column order ---
    output_columns = [
        "province_sk", "province_name", "region", "year_month", "year", "month",
        # Volume
        "total_posts", "total_comments", "total_hotel_reviews",
        "unique_authors", "comments_per_post", "post_frequency",
        # Engagement
        "avg_likes_per_post", "avg_saves_per_post", "avg_shares_per_post",
        "median_likes_per_post", "p90_likes_per_post",
        "viral_post_ratio", "engagement_score",
        # NLP
        "avg_sentiment", "sentiment_std", "positive_ratio", "negative_ratio",
        "sentiment_polarity",
        "avg_word_count", "word_count_std", "avg_unique_word_ratio",
        "emoji_sentiment_ratio", "reply_ratio",
        # Aspects (from NLP v2, 0.0 if v1)
        "avg_aspect_scenery", "avg_aspect_food", "avg_aspect_price",
        "avg_aspect_service", "avg_aspect_transport", "avg_aspect_accommodation",
        # Hotel
        "avg_hotel_score", "hotel_score_std", "hotel_review_volume",
        "high_score_ratio", "low_score_ratio",
        "unique_reviewer_countries", "domestic_review_ratio",
        "couple_ratio", "family_ratio", "business_ratio", "solo_ratio",
        "hotel_vol_growth",
        # Temporal
        "month_sin", "month_cos", "is_peak_season", "hotness_score",
        # Lags
        "hotness_lag_1", "hotness_lag_2", "hotness_lag_3", "hotness_lag_12",
        "hotness_rolling_3m", "hotness_momentum",
        "hotel_vol_lag_1", "hotel_vol_lag_2", "hotel_vol_lag_3", "hotel_vol_lag_12",
        "hotel_vol_rolling_3m", "hotel_vol_momentum",
        # Metadata
        "created_at", "updated_at",
    ]

    df = df.select(*output_columns)

    row_count = df.count()
    print(f"   Combined features: {row_count} province-months, {len(output_columns) - 2} features")
    return df


# ============================================================
# Step 5: Write + Export
# ============================================================

def write_and_export(spark: SparkSession, df: DataFrame) -> int:
    """Write to Iceberg table and export Parquet for ML training."""
    print("\n[5/5] Writing to Iceberg + exporting Parquet...")

    # Filter incomplete current month
    current_ym = int(datetime.now().strftime("%Y%m"))
    df = df.filter(F.col("year_month") < current_ym)

    row_count = df.count()

    df.write.format("iceberg").mode("overwrite").saveAsTable(GOLD_TABLE_FULL)
    print(f"   Wrote {row_count} rows to {GOLD_TABLE_FULL}")

    df.coalesce(1).write.mode("overwrite").parquet(PARQUET_EXPORT_PATH)
    print(f"   Exported to {PARQUET_EXPORT_PATH}")

    return row_count


# ============================================================
# Validation
# ============================================================

def print_summary(spark: SparkSession) -> None:
    df = spark.table(GOLD_TABLE_FULL)
    total = df.count()
    provinces = df.select("province_sk").distinct().count()
    months = df.select("year_month").distinct().count()
    ym_range = df.agg(F.min("year_month"), F.max("year_month")).collect()[0]
    nulls_hotness = df.filter(F.col("hotness_score").isNull()).count()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total rows:         {total:,}")
    print(f"  Provinces:          {provinces}")
    print(f"  Months:             {months}")
    print(f"  Range:              {ym_range[0]} — {ym_range[1]}")
    print(f"  NULL hotness_score: {nulls_hotness}")

    # Feature stats sample
    df.select(
        "hotness_score", "avg_sentiment", "engagement_score",
        "avg_hotel_score", "viral_post_ratio", "reply_ratio",
    ).describe().show()


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 70)
    print("Gold Layer — fact_province_month_dl_features")
    print("=" * 70)
    print(f"Start: {datetime.now()}")

    spark = get_spark_session("Gold_Fact_DL_Features")
    logger = get_gold_logger(spark)
    start_time = datetime.now()

    try:
        create_gold_database(spark)
        create_output_table(spark)

        df_posts = aggregate_post_engagement(spark)
        df_comments = aggregate_nlp_comments(spark)
        df_hotels = aggregate_hotel_reviews(spark)

        df_final = build_combined_features(spark, df_posts, df_comments, df_hotels)
        row_count = write_and_export(spark, df_final)

        print_summary(spark)

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.log_job_success(
            source_path=SOURCE_DESCRIPTION,
            table_name=GOLD_TABLE_FULL,
            records_processed=row_count,
            job_details={
                "job_type": "fact_dl_features",
                "execution_time_seconds": execution_time,
                "feature_count": 40,
                "grain": "province_x_month",
            },
        )

        print(f"\nCompleted in {execution_time:.1f}s")

    except Exception as exc:
        logger.log_job_failure(
            source_path=SOURCE_DESCRIPTION,
            table_name=GOLD_TABLE_FULL,
            error_message=str(exc),
        )
        print(f"\nFailed: {exc}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
