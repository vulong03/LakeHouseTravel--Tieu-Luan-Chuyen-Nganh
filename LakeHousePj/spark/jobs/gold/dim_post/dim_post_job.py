"""
Gold Layer - Dimension Post Job
================================
Builds `gold.dim_post` from Silver layer tables.

Key responsibilities:
  1. Read from silver.silver.tiktok_post_metadata (main source)
  2. Join with silver.silver.tiktok_videos (for keyword, target_type, has_sub)
  3. Join with dim_author (by normalized author_tag)
  4. Join with dim_province (by extracted province from keyword)
  5. Join with dim_date (by crawl_time date)
  6. Transform and write to Iceberg table
  7. Log execution to PostgreSQL via GoldJobLogger
"""

import sys
from datetime import datetime

sys.path.append("/opt/spark/jobs")

from config import (  # type: ignore
    SOURCE_POSTS_TABLE,
    SOURCE_VIDEOS_TABLE,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    BUSINESS_KEY,
    DIM_AUTHOR_TABLE,
    DIM_PROVINCE_TABLE,
    DIM_DATE_TABLE,
)
from utils.spark_session import get_spark_session  # type: ignore
from utils.iceberg_utils import create_iceberg_table_if_not_exists  # type: ignore
from utils.gold_job_logger import get_gold_logger  # type: ignore

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    BooleanType,
    TimestampType,
)
from pyspark.sql.window import Window


def create_gold_database(spark):
    """Create Gold database if not exists"""
    print("\n[*] Ensuring Gold database exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
    print(f"[OK] Database {GOLD_CATALOG}.{GOLD_DATABASE} ready")


def create_dim_post_table(spark):
    """Create dim_post Iceberg table if not exists"""
    print("\n[*] Ensuring dim_post Iceberg table exists...")
    schema = StructType(
        [
            StructField("post_sk", IntegerType(), False),
            StructField("post_url", StringType(), False),  # Business key
            StructField("author_sk", IntegerType(), True),  # FK to dim_author
            StructField("province_sk", IntegerType(), True),  # FK to dim_province
            StructField("crawl_date_sk", IntegerType(), True),  # FK to dim_date
            StructField("post_date_sk", IntegerType(), True),  # FK to dim_date
            StructField("post_description", StringType(), True),
            StructField("keyword", StringType(), True),
            StructField("target_type", StringType(), True),
            StructField("has_sub", BooleanType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("is_active", BooleanType(), False),
        ]
    )

    create_iceberg_table_if_not_exists(
        spark=spark,
        database=GOLD_DATABASE,
        table_name=GOLD_TABLE,
        schema=schema,
        partition_by=[],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
        catalog=GOLD_CATALOG,
    )
    print(f"[OK] Table {GOLD_TABLE_FULL} ready")


def load_source_data(spark) -> DataFrame:
    """Load post data from Silver layer"""
    print(f"\nLoading source data from: {SOURCE_POSTS_TABLE}")
    df_posts = spark.table(SOURCE_POSTS_TABLE)
    
    posts_count = df_posts.count()
    print(f"Loaded {posts_count:,} records from {SOURCE_POSTS_TABLE}")
    
    return df_posts


def join_with_videos_table(spark, df_posts: DataFrame) -> DataFrame:
    """Join with tiktok_videos to get keyword, target_type, has_sub"""
    print("\nJoining with tiktok_videos table...")
    
    df_videos = spark.table(SOURCE_VIDEOS_TABLE).select(
        "url",
        "keyword",
        "target_type",
        "has_sub",
        "read_status"
    ).filter(F.col("read_status") == 1)  # Only get links with read_status = 1
    
    # INNER JOIN: Only keep posts that exist in tiktok_videos
    df_joined = df_posts.join(
        df_videos,
        df_posts["post_url"] == df_videos["url"],
        how="inner"
    ).drop("url")
    
    print(f"Kept {df_joined.count():,} posts that exist in tiktok_videos")
    
    return df_joined


def extract_province_from_keyword(df: DataFrame) -> DataFrame:
    """Extract province name from keyword (remove 'du lịch ' prefix)"""
    print("\nExtracting province from keyword...")
    
    df = df.withColumn(
        "_province_name",
        F.when(
            F.col("keyword").isNotNull(),
            F.regexp_replace(F.trim(F.col("keyword")), "^du lịch ", "")
        ).otherwise(F.lit(None))
    )
    
    return df


def join_with_dim_author(spark, df: DataFrame) -> DataFrame:
    """Join with dim_author to get author_sk"""
    print("\nJoining with dim_author...")
    
    # Load dim_author
    df_author = spark.table(DIM_AUTHOR_TABLE).select(
        "author_sk",
        "author_tag"
    )
    
    # Normalize author_tag (trim + remove @ prefix) for join
    df = df.withColumn(
        "_author_tag_normalized",
        F.when(
            F.col("author_tag").isNotNull(),
            F.regexp_replace(F.trim(F.col("author_tag")), "^@", "")
        ).otherwise(F.lit(None))
    )
    
    # Left join with dim_author
    df_joined = df.join(
        df_author,
        df["_author_tag_normalized"] == df_author["author_tag"],
        how="left"
    ).drop("author_tag", "_author_tag_normalized")
    
    missing_count = df_joined.filter(F.col("author_sk").isNull()).count()
    if missing_count > 0:
        print(f"Warning: {missing_count} posts have no matching author")
    
    return df_joined


def join_with_dim_province(spark, df: DataFrame) -> DataFrame:
    """Join with dim_province to get province_sk"""
    print("\nJoining with dim_province...")
    
    # Load dim_province
    df_province = spark.table(DIM_PROVINCE_TABLE).select(
        "province_sk",
        "province_name"
    )
    
    # Left join with dim_province
    df_joined = df.join(
        df_province,
        df["_province_name"] == df_province["province_name"],
        how="left"
    ).drop("province_name", "_province_name")
    
    missing_count = df_joined.filter(
        F.col("keyword").isNotNull() & F.col("province_sk").isNull()
    ).count()
    if missing_count > 0:
        print(f"Warning: {missing_count} posts have keyword but no matching province")
    
    return df_joined


def join_with_dim_date(spark, df: DataFrame) -> DataFrame:
    """Join with dim_date to get crawl_date_sk & post_date_sk"""
    print("\nJoining with dim_date...")
    
    # Load dim_date
    df_date = spark.table(DIM_DATE_TABLE).select(
        "date_sk",
        "full_date"
    )
    
    # Extract date from crawl_time
    df = df.withColumn(
        "_crawl_date",
        F.when(
            F.col("crawl_time").isNotNull(),
            F.to_date(F.col("crawl_time"))
        ).otherwise(F.lit(None))
    ).withColumn(
        "_post_date",
        F.when(
            F.col("post_date").isNotNull(),
            F.to_date(F.col("post_date"))
        ).otherwise(F.lit(None))
    )
    
    crawl_alias = df_date.alias("crawl")
    df_joined = df.join(
        crawl_alias,
        df["_crawl_date"] == crawl_alias["full_date"],
        how="left"
    ).withColumnRenamed("date_sk", "crawl_date_sk") \
     .drop("full_date")
    
    post_alias = df_date.alias("post")
    df_joined = df_joined.join(
        post_alias,
        df_joined["_post_date"] == post_alias["full_date"],
        how="left"
    ).withColumnRenamed("date_sk", "post_date_sk") \
     .drop("full_date") \
     .drop("_crawl_date", "_post_date")
    
    missing_crawl = df_joined.filter(
        F.col("crawl_time").isNotNull() & F.col("crawl_date_sk").isNull()
    ).count()
    if missing_crawl > 0:
        print(f"Warning: {missing_crawl} posts have crawl_time but no matching date in dim_date")
    
    missing_post = df_joined.filter(
        F.col("post_date").isNotNull() & F.col("post_date_sk").isNull()
    ).count()
    if missing_post > 0:
        print(f"Warning: {missing_post} posts have post_date but no matching date in dim_date")
    
    return df_joined


def transform_has_sub(df: DataFrame) -> DataFrame:
    """Convert has_sub from string 'no'/'yes' to boolean"""
    print("\nConverting has_sub to boolean...")
    
    df = df.withColumn(
        "has_sub",
        F.when(
            F.lower(F.trim(F.col("has_sub"))) == "yes",
            True
        ).when(
            F.lower(F.trim(F.col("has_sub"))) == "no",
            False
        ).otherwise(F.lit(None))
    )
    
    return df


def deduplicate_by_post_url(df: DataFrame) -> DataFrame:
    """
    Deduplicate by post_url, keeping the latest record based on crawl_time or ingestion_timestamp
    
    This ensures each post_url appears only once in the dimension table.
    """
    print("\nDeduplicating by post_url (keep latest)...")
    
    original_count = df.count()
    print(f"Original records: {original_count:,}")
    
    # Window function to rank by post_url, ordered by latest crawl_time/ingestion_timestamp
    window_spec = Window.partitionBy("post_url").orderBy(
        F.coalesce(F.col("crawl_time"), F.col("ingestion_timestamp")).desc()
    )
    
    df = df.withColumn("_rank", F.row_number().over(window_spec)) \
           .filter(F.col("_rank") == 1) \
           .drop("_rank")
    
    final_count = df.count()
    duplicates_removed = original_count - final_count
    print(f"Removed duplicates: {duplicates_removed:,} records")
    print(f"Final unique posts: {final_count:,}")
    
    return df


def transform_to_dimension(df: DataFrame) -> DataFrame:
    """
    Transform to dimension table format
    
    Transformations:
    1. Select required columns
    2. Normalize post_description (trim)
    3. Add metadata columns
    4. Generate surrogate key
    """
    print("\nTransforming to dimension format...")
    
    current_timestamp = F.current_timestamp()
    
    # Step 1: Select and normalize columns
    df = df.select(
        "post_url",
        "author_sk",
        "province_sk",
        "crawl_date_sk",
        "post_date_sk",
        F.when(
            F.col("post_description").isNotNull(),
            F.trim(F.col("post_description"))
        ).otherwise(F.lit(None)).alias("post_description"),
        "keyword",
        "target_type",
        "has_sub"
    )
    
    # Step 2: Add metadata columns
    print("Adding SCD metadata...")
    df = df.withColumn("created_at", current_timestamp)
    df = df.withColumn("updated_at", current_timestamp)
    df = df.withColumn("is_active", F.lit(True))
    
    # Step 3: Generate surrogate key
    print("Generating surrogate keys...")
    window_spec = Window.orderBy("post_url")
    df = df.withColumn("post_sk", F.row_number().over(window_spec))
    
    # Reorder columns to match schema
    df = df.select(
        "post_sk",
        "post_url",
        "author_sk",
        "province_sk",
        "crawl_date_sk",
        "post_date_sk",
        "post_description",
        "keyword",
        "target_type",
        "has_sub",
        "created_at",
        "updated_at",
        "is_active"
    )
    
    print("Transformation complete")
    df.printSchema()
    df.show(10, truncate=False)
    
    return df


def write_to_gold_table(spark, df: DataFrame, record_count: int):
    """Write dimension data to Gold Iceberg table"""
    print(f"\nWriting to Gold table: {GOLD_TABLE_FULL}")
    print(f"Records to write: {record_count:,}")
    
    df.writeTo(GOLD_TABLE_FULL) \
        .using("iceberg") \
        .overwritePartitions()
    
    print(f"Successfully wrote {record_count:,} records to {GOLD_TABLE_FULL}")


def validate_results(spark):
    """Validate the created dimension table"""
    print("\nValidating results...")
    
    df = spark.table(GOLD_TABLE_FULL)
    
    total_count = df.count()
    with_author = df.filter(F.col("author_sk").isNotNull()).count()
    with_province = df.filter(F.col("province_sk").isNotNull()).count()
    with_crawl_date = df.filter(F.col("crawl_date_sk").isNotNull()).count()
    with_post_date = df.filter(F.col("post_date_sk").isNotNull()).count()
    with_description = df.filter(F.col("post_description").isNotNull()).count()
    
    print("\nValidation Summary:")
    print(f"Total posts: {total_count:,}")
    print(f"Posts with author_sk: {with_author:,} ({with_author/total_count*100:.2f}%)")
    print(f"Posts with province_sk: {with_province:,} ({with_province/total_count*100:.2f}%)")
    print(f"Posts with crawl_date_sk: {with_crawl_date:,} ({with_crawl_date/total_count*100:.2f}%)")
    print(f"Posts with post_date_sk: {with_post_date:,} ({with_post_date/total_count*100:.2f}%)")
    print(f"Posts with description: {with_description:,} ({with_description/total_count*100:.2f}%)")
    
    print("\nSample posts:")
    df.select("post_sk", "post_url", "author_sk", "province_sk", "crawl_date_sk", "post_date_sk", "keyword") \
        .orderBy("post_sk") \
        .show(20, truncate=False)


def main():
    """Main execution flow for dim_post job"""
    print("=" * 80)
    print("Gold Layer - Dimension Post Job")
    print("=" * 80)
    
    spark = get_spark_session(app_name="Gold_Dim_Post")
    logger = get_gold_logger(spark)
    
    job_start_time = datetime.now()
    record_count = 0
    
    try:
        # Step 1: Create Gold database
        create_gold_database(spark)
        
        # Step 2: Create dimension table
        create_dim_post_table(spark)
        
        # Step 3: Load source data
        df_posts = load_source_data(spark)
        
        # Step 4: Join with videos table
        df_joined = join_with_videos_table(spark, df_posts)
        
        # Step 5: Extract province from keyword
        df_joined = extract_province_from_keyword(df_joined)
        
        # Step 6: Join with dim_author
        df_joined = join_with_dim_author(spark, df_joined)
        
        # Step 7: Join with dim_province
        df_joined = join_with_dim_province(spark, df_joined)
        
        # Step 8: Join with dim_date
        df_joined = join_with_dim_date(spark, df_joined)
        
        # Step 9: Transform has_sub
        df_joined = transform_has_sub(df_joined)
        
        # Step 10: Deduplicate by post_url (keep latest)
        df_joined = deduplicate_by_post_url(df_joined)
        
        # Step 11: Transform to dimension format
        dim_df = transform_to_dimension(df_joined)
        record_count = dim_df.count()
        
        # Step 12: Write to Gold table
        write_to_gold_table(spark, dim_df, record_count)
        
        # Step 13: Validate results
        validate_results(spark)
        
        # Log success
        job_end_time = datetime.now()
        execution_time = (job_end_time - job_start_time).total_seconds()
        
        logger.log_job_success(
            source_path=f"{SOURCE_POSTS_TABLE} + {SOURCE_VIDEOS_TABLE}",
            table_name=GOLD_TABLE_FULL,
            records_processed=record_count,
            job_details={
                "job_type": "dimension",
                "execution_time_seconds": execution_time,
                "source_type": "silver_tables",
                "posts_with_author": dim_df.filter(F.col("author_sk").isNotNull()).count(),
                "posts_with_province": dim_df.filter(F.col("province_sk").isNotNull()).count(),
                "posts_with_date": dim_df.filter(F.col("crawl_date_sk").isNotNull()).count(),
            }
        )
        
        print("\n" + "=" * 80)
        print("dim_post job completed successfully!")
        print(f"Records: {record_count:,}")
        print(f"Execution time: {execution_time:.2f}s")
        print("=" * 80)
        
    except Exception as e:
        logger.log_job_failure(
            source_path=f"{SOURCE_POSTS_TABLE} + {SOURCE_VIDEOS_TABLE}",
            table_name=GOLD_TABLE_FULL,
            error_message=str(e)
        )
        
        print("\n" + "=" * 80)
        print(f"dim_post job failed: {e}")
        print("=" * 80)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

