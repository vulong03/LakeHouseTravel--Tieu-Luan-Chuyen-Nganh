"""
Gold Layer - Fact: Province Content Engagement

This job aggregates TikTok post metrics by province and post date to power
analytics around author activity and engagement at the provincial level.
"""

import sys
from typing import Tuple, Dict, Any

from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    LongType,
    TimestampType,
)

# Make sure the shared utils are importable inside the Spark container
sys.path.append("/opt/spark/jobs")

from config import (  # type: ignore
    DIM_POST_TABLE,
    POST_METRICS_TABLE,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    SOURCE_DESCRIPTION,
)
from utils.spark_session import get_spark_session  # type: ignore
from utils.iceberg_utils import create_iceberg_table_if_not_exists  # type: ignore
from utils.gold_job_logger import get_gold_logger  # type: ignore


def create_gold_database(spark: SparkSession) -> None:
    """Ensure the target Gold database exists."""
    print("\n[*] Ensuring Gold database exists...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
    print(f"[OK] Database {GOLD_CATALOG}.{GOLD_DATABASE} ready")


def create_fact_table(spark: SparkSession) -> None:
    """Create the Iceberg table for the fact if it doesn't already exist."""
    print("\n[*] Ensuring fact table exists...")

    schema = StructType(
        [
            StructField("province_sk", IntegerType(), False),
            StructField("date_sk", IntegerType(), False),
            StructField("posts_cnt", LongType(), False),
            StructField("likes_cnt", LongType(), False),
            StructField("comments_cnt", LongType(), False),
            StructField("comments_crawled_cnt", LongType(), False),
            StructField("saves_cnt", LongType(), False),
            StructField("shares_cnt", LongType(), False),
            StructField("level1_comments_cnt", LongType(), False),
            StructField("level2_comments_cnt", LongType(), False),
            StructField("unique_authors_cnt", LongType(), False),
            StructField("engagement_score", LongType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )

    create_iceberg_table_if_not_exists(
        spark=spark,
        database=GOLD_DATABASE,
        table_name=GOLD_TABLE,
        schema=schema,
        partition_by=["province_sk"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
        catalog=GOLD_CATALOG,
    )


def load_dim_post_data(spark: SparkSession) -> Tuple[DataFrame, Dict[str, Any]]:
    """Load the subset of dim_post required for aggregation and report stats."""
    print("\n📥 Loading dim_post data...")
    df = (
        spark.table(DIM_POST_TABLE)
        .select(
            "post_sk",
            "post_url",
            "province_sk",
            "post_date_sk",
            "author_sk",
        )
        .where(F.col("province_sk").isNotNull() & F.col("post_date_sk").isNotNull())
    )

    total = df.count()
    provinces = df.select("province_sk").distinct().count()
    dates = df.select("post_date_sk").distinct().count()
    print(f"   - Loaded {total:,} posts with province & post_date")
    print(f"     · Distinct provinces: {provinces}, distinct dates: {dates}")

    stats = {
        "total_records": total,
        "distinct_provinces": provinces,
        "distinct_dates": dates,
    }
    return df, stats


def load_post_metrics_data(spark: SparkSession) -> Tuple[DataFrame, Dict[str, Any]]:
    """Load latest metrics per post from the silver table and log details."""
    print("\n📥 Loading post metrics from silver...")
    raw_df = spark.table(POST_METRICS_TABLE).select(
        "post_url",
        "likes",
        "comments_count",
        "comments_displayed_tiktok",
        "comments_loaded",
        "comments_level1",
        "comments_level2",
        "saves",
        "shares",
        "crawl_time",
        "scrape_timestamp",
        "ingestion_timestamp",
    )

    window_spec = Window.partitionBy("post_url").orderBy(
        F.coalesce(
            F.col("crawl_time"),
            F.to_timestamp("scrape_timestamp"),
            F.col("ingestion_timestamp"),
        ).desc()
    )

    df = (
        raw_df.withColumn("row_num", F.row_number().over(window_spec))
        .where(F.col("row_num") == 1)
        .drop("row_num")
    )

    raw_count = raw_df.count()
    latest_count = df.count()
    with_metrics = df.select("post_url").distinct().count()
    print(f"   - Raw metric rows: {raw_count:,}")
    print(f"   - Prepared latest metrics for {latest_count:,} rows")
    print(f"     · Distinct posts with metrics: {with_metrics:,}")

    stats = {
        "raw_rows": raw_count,
        "prepared_rows": latest_count,
        "distinct_posts": with_metrics,
    }
    return df, stats


def prepare_aggregated_fact(
    dim_post_df: DataFrame,
    metrics_df: DataFrame,
    dim_stats: Dict[str, Any],
    metric_stats: Dict[str, Any],
) -> Tuple[DataFrame, int]:
    """Join inputs, aggregate by province & date, and return fact DataFrame."""
    print("\n🔄 Joining and aggregating data...")

    joined = (
        dim_post_df.alias("dp")
        .join(metrics_df.alias("pm"), on="post_url", how="inner")
    )

    matched_posts = joined.select("dp.post_sk").distinct().count()
    unmatched_posts = dim_stats["total_records"] - matched_posts
    print(f"   - Joined posts with metrics: {matched_posts:,}")
    print(f"   - Posts without metrics: {unmatched_posts:,}")

    prepared = (
        joined.select(
            F.col("dp.province_sk").alias("province_sk"),
            F.col("dp.post_date_sk").alias("date_sk"),
            F.col("dp.post_sk").alias("post_sk"),
            F.col("dp.author_sk").alias("author_sk"),
            F.coalesce(F.col("pm.likes").cast("long"), F.lit(0)).alias("likes"),
            F.coalesce(
                F.coalesce(F.col("pm.comments_displayed_tiktok"), F.col("pm.comments_count")).cast("long"),
                F.lit(0),
            ).alias("comments"),
            F.coalesce(F.col("pm.comments_loaded").cast("long"), F.lit(0)).alias("comments_crawled"),
            F.coalesce(F.col("pm.saves").cast("long"), F.lit(0)).alias("saves"),
            F.coalesce(F.col("pm.shares").cast("long"), F.lit(0)).alias("shares"),
            F.coalesce(F.col("pm.comments_level1").cast("long"), F.lit(0)).alias("level1"),
            F.coalesce(F.col("pm.comments_level2").cast("long"), F.lit(0)).alias("level2"),
        )
        .where(F.col("province_sk").isNotNull() & F.col("date_sk").isNotNull())
    )

    aggregated = (
        prepared.groupBy("province_sk", "date_sk")
        .agg(
            F.countDistinct("post_sk").alias("posts_cnt"),
            F.sum("likes").alias("likes_cnt"),
            F.sum("comments").alias("comments_cnt"),
            F.sum("comments_crawled").alias("comments_crawled_cnt"),
            F.sum("saves").alias("saves_cnt"),
            F.sum("shares").alias("shares_cnt"),
            F.sum("level1").alias("level1_comments_cnt"),
            F.sum("level2").alias("level2_comments_cnt"),
            F.countDistinct("author_sk").alias("unique_authors_cnt"),
        )
    )

    current_ts = F.current_timestamp()
    fact_df = (
        aggregated.withColumn(
            "engagement_score",
            F.col("likes_cnt")
            + F.col("comments_cnt")
            + F.col("saves_cnt")
            + F.col("shares_cnt"),
        )
        .withColumn("created_at", current_ts)
        .withColumn("updated_at", current_ts)
        .select(
            "province_sk",
            "date_sk",
            "posts_cnt",
            "likes_cnt",
            "comments_cnt",
            "comments_crawled_cnt",
            "saves_cnt",
            "shares_cnt",
            "level1_comments_cnt",
            "level2_comments_cnt",
            "unique_authors_cnt",
            "engagement_score",
            "created_at",
            "updated_at",
        )
    )

    fact_df = fact_df.cache()
    record_count = fact_df.count()
    print(f"   - Aggregated {record_count:,} fact rows")
    return fact_df, record_count


def write_to_gold_table(fact_df: DataFrame) -> None:
    """Persist the fact data to the Iceberg table."""
    print("\n💾 Writing fact data to Gold table...")
    (
        fact_df.coalesce(1)
        .writeTo(GOLD_TABLE_FULL)
        .using("iceberg")
        .overwritePartitions()
    )
    print(f"[OK] Data written to {GOLD_TABLE_FULL}")


def validate_results(fact_df: DataFrame) -> None:
    """Display basic statistics for sanity checking."""
    print("\n✅ Validation sample (top provinces by engagement):")
    (
        fact_df.orderBy(F.desc("engagement_score"))
        .select(
            "province_sk",
            "date_sk",
            "posts_cnt",
            "likes_cnt",
            "comments_cnt",
            "saves_cnt",
            "shares_cnt",
            "unique_authors_cnt",
            "engagement_score",
        )
        .show(10, truncate=False)
    )


def main():
    spark = get_spark_session("Gold_Fact_Province_Content_Engagement")
    logger = get_gold_logger(spark)
    fact_df = None

    try:
        create_gold_database(spark)
        create_fact_table(spark)

        dim_post_df, dim_stats = load_dim_post_data(spark)
        metrics_df, metric_stats = load_post_metrics_data(spark)

        fact_df, record_count = prepare_aggregated_fact(
            dim_post_df, metrics_df, dim_stats, metric_stats
        )

        write_to_gold_table(fact_df)
        validate_results(fact_df)

        logger.log_job_success(
            source_path=SOURCE_DESCRIPTION,
            table_name=GOLD_TABLE_FULL,
            records_processed=record_count,
            job_details={
                "job_type": "fact",
                "distinct_provinces": fact_df.select("province_sk").distinct().count(),
                "distinct_dates": fact_df.select("date_sk").distinct().count(),
            },
        )

        fact_df.unpersist()

        print("\n============================================")
        print("✅ fact_province_content_engagement completed!")
        print(f"   Rows written: {record_count:,}")
        print("============================================")

    except Exception as exc:
        logger.log_job_failure(
            source_path=SOURCE_DESCRIPTION,
            table_name=GOLD_TABLE_FULL,
            error_message=str(exc),
        )
        print("\n❌ Fact job failed!")
        raise
    finally:
        if fact_df is not None:
            fact_df.unpersist(blocking=False)
        spark.stop()


if __name__ == "__main__":
    main()


