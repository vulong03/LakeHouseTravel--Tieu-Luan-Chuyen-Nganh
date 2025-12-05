"""
Gold Layer - Training Model: Province-Month Aggregation
========================================================
Aggregates comment-level fact table to province-month grain for ML training.

Input:  gold.gold.fact_comment_nlp_engagement (465K comments)
Output: gold.gold.province_month_features (3K province-months)

Features:
  - 28 aggregated features including:
    * Volume metrics (total_comments, total_posts)
    * NLP aggregates (word counts, sentiment scores, emoji counts)
    * Engagement metrics (likes, saves, shares)
    * Sentiment distribution (positive/neutral/negative ratios)

Business Use Case:
  Recommend best provinces to visit by month using XGBoost model
"""

import sys
from datetime import datetime
from typing import Tuple, Dict, Any

sys.path.append("/opt/spark/jobs")

from config import (  # type: ignore
    FACT_COMMENT_NLP_TABLE,
    DIM_DATE_TABLE,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    SOURCE_DESCRIPTION,
)

DIM_POST_TABLE = f"{GOLD_CATALOG}.{GOLD_DATABASE}.dim_post"

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    TimestampType,
)

from utils.spark_session import get_spark_session  # type: ignore
from utils.iceberg_utils import create_iceberg_table_if_not_exists  # type: ignore


def create_gold_database(spark: SparkSession) -> None:
    """Create Gold database if not exists"""
    print("\n[*] Ensuring Gold database exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
    print(f"[OK] Database {GOLD_CATALOG}.{GOLD_DATABASE} ready")


def create_output_table(spark: SparkSession) -> None:
    """Create province_month_features Iceberg table if not exists"""
    print("\n[*] Ensuring province_month_features Iceberg table exists...")
    
    schema = StructType([
        # Primary Keys
        StructField("province_sk", IntegerType(), False),
        StructField("year_month", IntegerType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), True),
        
        # Comment Volume
        StructField("total_comments", LongType(), True),
        StructField("total_posts", LongType(), True),
        
        # NLP Aggregates (SUM)
        StructField("total_words", LongType(), True),
        StructField("total_exclamations", LongType(), True),
        StructField("total_emojis", LongType(), True),
        StructField("total_positive_emojis", LongType(), True),
        StructField("total_negative_emojis", LongType(), True),
        
        # NLP Aggregates (AVG)
        StructField("avg_unique_word_ratio", DoubleType(), True),
        StructField("avg_sentiment_score", DoubleType(), True),
        StructField("avg_words_per_comment", DoubleType(), True),
        
        # Sentiment Distribution (COUNT)
        StructField("positive_comments", LongType(), True),
        StructField("neutral_comments", LongType(), True),
        StructField("negative_comments", LongType(), True),
        
        # Sentiment Ratios (COMPUTED)
        StructField("positive_ratio", DoubleType(), True),
        StructField("negative_ratio", DoubleType(), True),
        
        # Engagement Metrics (Comment)
        StructField("total_comment_likes", LongType(), True),
        StructField("avg_comment_likes", DoubleType(), True),
        
        # Post Engagement (Deduplicated)
        StructField("total_post_likes", LongType(), True),
        StructField("total_post_saves", LongType(), True),
        StructField("total_post_shares", LongType(), True),
        StructField("avg_post_likes", DoubleType(), True),
        StructField("avg_post_saves", DoubleType(), True),
        StructField("avg_post_shares", DoubleType(), True),
        
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
    print(f"[OK] Table {GOLD_TABLE_FULL} ready")


def load_dim_date(spark: SparkSession) -> DataFrame:
    """Load dim_date from Gold catalog"""
    print(f"\n📅 Loading {DIM_DATE_TABLE}...")
    
    df = spark.table(DIM_DATE_TABLE)
    
    # Select only needed columns
    df = df.select(
        "date_sk",
        "year",
        "month",
        "month_name",
        "year_month"
    )
    
    row_count = df.count()
    print(f"   ✅ Loaded {row_count:,} dates")
    
    return df


def load_fact_comments(spark: SparkSession) -> Tuple[DataFrame, Dict[str, Any]]:
    """Load fact_comment_nlp_engagement and join with dim_post to get post_date_sk"""
    print(f"\n📥 Loading {FACT_COMMENT_NLP_TABLE}...")
    
    df_comments = spark.table(FACT_COMMENT_NLP_TABLE)
    
    print(f"\n🔗 Joining with {DIM_POST_TABLE} to get post_date_sk...")
    df_posts = spark.table(DIM_POST_TABLE).select("post_sk", "post_date_sk")
    
    df = df_comments.join(
        df_posts,
        on="post_sk",
        how="inner"
    )
    
    total_count = df.count()
    provinces = df.select("province_sk").distinct().count()
    posts = df.select("post_sk").distinct().count()
    
    print(f"   ✅ Loaded {total_count:,} comments (with post_date_sk)")
    print(f"      - Distinct provinces: {provinces}")
    print(f"      - Distinct posts: {posts}")
    
    stats = {
        "total_comments": total_count,
        "distinct_provinces": provinces,
        "distinct_posts": posts,
    }
    
    return df, stats


def join_with_date_dimension(
    df_comments: DataFrame, 
    df_dates: DataFrame
) -> DataFrame:
    """Join with date dimension to get POST year_month (not comment date)"""
    print("\n🔗 Joining with dim_date (by POST date)...")
    
    # Join by POST_DATE_SK to aggregate by month post was created
    df_joined = df_comments.join(
        df_dates,
        df_comments.post_date_sk == df_dates.date_sk,
        how="inner"
    )
    
    # Drop duplicate date_sk column
    df_joined = df_joined.drop(df_dates.date_sk)
    
    count_after = df_joined.count()
    print(f"   ✅ Joined result: {count_after:,} rows (grouped by POST date)")
    
    return df_joined


def aggregate_comment_metrics(df: DataFrame) -> DataFrame:
    """Aggregate comment-level metrics to province-month grain"""
    print("\n📊 Aggregating comment metrics...")
    
    df_agg = df.groupBy(
        "province_sk", 
        "year", 
        "month", 
        "month_name", 
        "year_month"
    ).agg(
        # Volume
        F.count("comment_sk").alias("total_comments"),
        F.countDistinct("post_sk").alias("total_posts"),
        
        # NLP - SUM
        F.sum("word_count").alias("total_words"),
        F.sum("exclamation_count").alias("total_exclamations"),
        F.sum("emoji_count").alias("total_emojis"),
        F.sum("positive_emoji_count").alias("total_positive_emojis"),
        F.sum("negative_emoji_count").alias("total_negative_emojis"),
        
        # NLP - AVG
        F.avg("unique_word_ratio").alias("avg_unique_word_ratio"),
        F.avg("sentiment_score").alias("avg_sentiment_score"),
        F.avg("word_count").alias("avg_words_per_comment"),
        
        # Sentiment Distribution
        F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_comments"),
        F.sum(F.when(F.col("sentiment_label") == "neutral", 1).otherwise(0)).alias("neutral_comments"),
        F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_comments"),
        
        # Comment Engagement
        F.sum("comment_likes").alias("total_comment_likes"),
        F.avg("comment_likes").alias("avg_comment_likes"),
    )
    
    row_count = df_agg.count()
    print(f"   ✅ Aggregated to {row_count:,} province-months")
    
    return df_agg


def aggregate_post_metrics(spark: SparkSession) -> DataFrame:
    """
    Aggregate post-level metrics from fact_province_content_engagement.
    
    This fact table has grain = 1 post, so no deduplication needed.
    Join with dim_post and dim_date to get year_month, then aggregate.
    """
    print("\n📱 Aggregating post metrics from fact_province_content_engagement...")
    
    # Load post-level fact table
    fact_post_table = f"{GOLD_CATALOG}.{GOLD_DATABASE}.fact_province_content_engagement"
    df_posts = spark.table(fact_post_table)
    
    # Load dim_post to get post_date_sk
    df_post_dates = spark.table(DIM_POST_TABLE).select("post_sk", "post_date_sk")
    
    # Load dim_date to get year_month
    df_dates = spark.table(f"{GOLD_CATALOG}.{GOLD_DATABASE}.dim_date").select(
        "date_sk", "year_month"
    )
    
    # Join to get year_month for each post
    df_posts_with_month = df_posts \
        .join(df_post_dates, "post_sk", "inner") \
        .join(df_dates, df_post_dates.post_date_sk == df_dates.date_sk, "inner") \
        .select(
            "post_sk",
            "province_sk", 
            df_dates.year_month,
            df_posts.likes.alias("post_likes"),
            df_posts.saves.alias("post_saves"),
            df_posts.shares.alias("post_shares")
        )
    
    unique_posts = df_posts_with_month.count()
    print(f"   ✅ Loaded {unique_posts:,} posts from fact table")
    
    # Aggregate to province-month (each post counted once - no deduplication needed)
    df_post_agg = df_posts_with_month.groupBy("province_sk", "year_month").agg(
        F.sum("post_likes").alias("total_post_likes"),
        F.sum("post_saves").alias("total_post_saves"),
        F.sum("post_shares").alias("total_post_shares"),
        F.avg("post_likes").alias("avg_post_likes"),
        F.avg("post_saves").alias("avg_post_saves"),
        F.avg("post_shares").alias("avg_post_shares"),
    )
    
    row_count = df_post_agg.count()
    print(f"   ✅ Aggregated to {row_count:,} province-months")
    
    return df_post_agg


def join_comment_and_post_aggregations(
    df_comment_agg: DataFrame,
    df_post_agg: DataFrame
) -> DataFrame:
    """Join comment and post aggregations"""
    print("\n🔗 Joining comment + post aggregations...")
    
    df_final = df_comment_agg.join(
        df_post_agg,
        on=["province_sk", "year_month"],
        how="left"
    )
    
    return df_final


def calculate_derived_features(df: DataFrame) -> DataFrame:
    """Calculate derived features (ratios, percentages)"""
    print("\n🧮 Calculating derived features...")
    
    df = df.withColumn(
        "positive_ratio",
        F.col("positive_comments") / F.col("total_comments")
    ).withColumn(
        "negative_ratio",
        F.col("negative_comments") / F.col("total_comments")
    )
    
    return df


def apply_filters(df: DataFrame) -> Tuple[DataFrame, int]:
    """Apply data quality filters"""
    print("\n🔍 Applying data quality filters...")
    
    initial_count = df.count()
    
    # Filter: Exclude current month (incomplete data)
    current_year_month = int(datetime.now().strftime("%Y%m"))
    df = df.filter(F.col("year_month") < current_year_month)
    print(f"   ✅ Filter: year_month < {current_year_month} (exclude current month)")
    
    final_count = df.count()
    filtered_out = initial_count - final_count
    
    print(f"   📊 Result: {final_count:,} rows ({filtered_out:,} filtered out)")
    
    return df, filtered_out


def add_metadata(df: DataFrame) -> DataFrame:
    """Add created_at and updated_at timestamps"""
    print("\n⏰ Adding metadata timestamps...")
    
    df = df \
        .withColumn("created_at", F.current_timestamp()) \
        .withColumn("updated_at", F.current_timestamp())
    
    return df


def reorder_columns(df: DataFrame) -> DataFrame:
    """Reorder columns to match schema"""
    column_order = [
        # Keys
        "province_sk", "year_month", "year", "month", "month_name",
        # Volume
        "total_comments", "total_posts",
        # NLP - SUM
        "total_words", "total_exclamations", "total_emojis",
        "total_positive_emojis", "total_negative_emojis",
        # NLP - AVG
        "avg_unique_word_ratio", "avg_sentiment_score", "avg_words_per_comment",
        # Sentiment
        "positive_comments", "neutral_comments", "negative_comments",
        "positive_ratio", "negative_ratio",
        # Comment Engagement
        "total_comment_likes", "avg_comment_likes",
        # Post Engagement
        "total_post_likes", "total_post_saves", "total_post_shares",
        "avg_post_likes", "avg_post_saves", "avg_post_shares",
        # Metadata
        "created_at", "updated_at",
    ]
    
    return df.select(*column_order)


def write_to_iceberg(spark: SparkSession, df: DataFrame) -> int:
    """Write aggregated data to Iceberg table with full overwrite"""
    print(f"\n💾 Writing to {GOLD_TABLE_FULL}...")
    
    row_count = df.count()
    print(f"   📊 Total rows to write: {row_count:,}")
    
    # Full overwrite entire table (vì thay đổi logic từ comment_date -> post_date)
    df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(GOLD_TABLE_FULL)
    
    print(f"   ✅ Successfully overwrote entire table with {row_count:,} rows")
    
    return row_count


def export_to_parquet(df: DataFrame) -> None:
    """Export to Parquet for ML training"""
    parquet_path = "s3a://gold/ml_training/province_month_features.parquet"
    
    print(f"\n📦 Exporting to Parquet: {parquet_path}")
    
    # Write as single file (coalesce) for easier ML loading
    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(parquet_path)
    
    print(f"   ✅ Exported successfully")


def print_summary_stats(spark: SparkSession) -> None:
    """Print summary statistics"""
    print("\n" + "=" * 60)
    print("📊 SUMMARY STATISTICS")
    print("=" * 60)
    
    df = spark.table(GOLD_TABLE_FULL)
    
    # Basic counts
    total_rows = df.count()
    distinct_provinces = df.select("province_sk").distinct().count()
    distinct_months = df.select("year_month").distinct().count()
    
    print(f"Total rows:           {total_rows:,}")
    print(f"Distinct provinces:   {distinct_provinces}")
    print(f"Distinct year_months: {distinct_months}")
    
    # Date range
    date_range = df.agg(
        F.min("year_month").alias("min_ym"),
        F.max("year_month").alias("max_ym")
    ).collect()[0]
    
    print(f"Year_month range:     {date_range['min_ym']} to {date_range['max_ym']}")
    
    # Aggregates
    aggregates = df.agg(
        F.sum("total_comments").alias("total_comments"),
        F.avg("total_comments").alias("avg_comments_per_month"),
        F.avg("positive_ratio").alias("avg_positive_ratio"),
        F.avg("avg_sentiment_score").alias("overall_sentiment"),
    ).collect()[0]
    
    print(f"\nTotal comments:       {aggregates['total_comments']:,}")
    print(f"Avg comments/month:   {aggregates['avg_comments_per_month']:.1f}")
    print(f"Avg positive ratio:   {aggregates['avg_positive_ratio']:.3f}")
    print(f"Overall sentiment:    {aggregates['overall_sentiment']:.3f}")
    
    print("=" * 60)


def main():
    """Main execution flow"""
    print("\n" + "=" * 60)
    print("Gold Layer - Province-Month Aggregation Job")
    print("=" * 60)
    print(f"Job: {SOURCE_DESCRIPTION}")
    print(f"Target: {GOLD_TABLE_FULL}")
    print("=" * 60)
    
    # Initialize Spark
    spark = get_spark_session("ProvinceMonthAggregation")
    
    try:
        # Setup
        create_gold_database(spark)
        create_output_table(spark)
        
        # Load data
        df_dates = load_dim_date(spark)
        df_comments, load_stats = load_fact_comments(spark)
        
        # Transform
        df_with_dates = join_with_date_dimension(df_comments, df_dates)
        df_comment_agg = aggregate_comment_metrics(df_with_dates)
        df_post_agg = aggregate_post_metrics(spark)  # Now takes spark instead of df
        df_joined = join_comment_and_post_aggregations(df_comment_agg, df_post_agg)
        df_final = calculate_derived_features(df_joined)
        df_filtered, filtered_count = apply_filters(df_final)
        df_with_metadata = add_metadata(df_filtered)
        df_output = reorder_columns(df_with_metadata)
        
        # Load
        result_count = write_to_iceberg(spark, df_output)
        export_to_parquet(df_output)
        
        # Summary
        print_summary_stats(spark)
        
        print("\n" + "=" * 60)
        print("✅ Job completed successfully!")
        print("=" * 60)
        print(f"Input:  {load_stats['total_comments']:,} comments")
        print(f"Output: {result_count:,} province-months")
        print(f"Filtered: {filtered_count:,} rows")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Job failed: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
