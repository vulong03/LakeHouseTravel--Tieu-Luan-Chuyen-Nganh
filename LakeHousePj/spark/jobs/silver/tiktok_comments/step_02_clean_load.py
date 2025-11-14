"""
Step 2: Clean & Load - TikTok Comments (Scratch → Silver) - BATCH PROCESSING

Purpose: Apply data cleaning, type conversion, and load to Silver using batch processing
Strategy:
  - Phase 1: PREPARE
    * List partitions from Scratch
    * Build mapping: partition → source file metadata
    * Filter unprocessed partitions (check PostgreSQL tracking)
  
  - Phase 2: BATCH PROCESSING (30 partitions per batch)
    * Posts: Read 30 partitions → Union → Clean → Single APPEND
    * Comments: Read 30 partitions → Union → Clean → Single APPEND
    * Log each file individually to PostgreSQL
    * Repeat for next batch
  
  - Phase 3: EXCEPTION HANDLING
    * Per-batch error handling (continue on failure)
    * Failed files logged with status='failed'
    * Summary statistics reported

Benefits:
  - Reduce Iceberg append operations: 2758 → 92 (96.7% reduction)
  - Reduce Hive Metastore snapshots and memory pressure
  - Maintain granular file-level tracking in PostgreSQL

Input: Scratch Parquet files (2 partitioned folders by post_url)
Output: 2 Silver Iceberg tables:
  - silver.silver.tiktok_post_metadata
  - silver.silver.tiktok_post_comments
"""

import sys
import os
import re

sys.path.append('/opt/spark/jobs')

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DateType, TimestampType, LongType
)

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum
from utils.file_tracker import check_if_file_ingested, log_ingestion_to_postgres
from silver.tiktok_comments.config import (
    SILVER_DATABASE,
    TABLE_NAME_POSTS,
    TABLE_NAME_COMMENTS,
    SILVER_TABLE_POSTS,
    SILVER_TABLE_COMMENTS,
    SCRATCH_BASE_PATH_POSTS,
    SCRATCH_BASE_PATH_COMMENTS,
    BRONZE_BASE_PATH,
    BUSINESS_COLUMNS_POSTS,
    BUSINESS_COLUMNS_COMMENTS,
    PARTITION_COLUMNS_POSTS,
    PARTITION_COLUMNS_COMMENTS,
    POSTGRES_CONN,
    BATCH_SIZE,
    CONTINUE_ON_BATCH_FAILURE
)
from silver.tiktok_comments.partition_utils import (
    list_scratch_partitions,
    decode_partition_value,
    build_partition_to_file_mapping,
    filter_unprocessed_partitions
)
from silver.tiktok_comments.file_processor import (
    create_batches,
    process_single_post_url
)


def get_latest_scratch_run(spark, scratch_base_path):
    """Get the latest run folder from Scratch bucket"""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(scratch_base_path),
            hadoop_conf
        )
        
        base_path = spark._jvm.org.apache.hadoop.fs.Path(scratch_base_path)
        
        if not fs.exists(base_path):
            raise Exception(f"Scratch path does not exist: {scratch_base_path}")
        
        # List all run directories
        file_statuses = fs.listStatus(base_path)
        run_dirs = [
            status.getPath().getName()
            for status in file_statuses
            if status.isDirectory() and status.getPath().getName().startswith("run_")
        ]
        
        if not run_dirs:
            raise Exception(f"No run directories found in {scratch_base_path}")
        
        # Sort by timestamp (run_YYYYMMDD_HHMMSS) and get latest
        run_dirs.sort(reverse=True)
        latest_run = run_dirs[0]
        
        latest_path = f"{scratch_base_path}/{latest_run}"
        print(f"📂 Latest Scratch run: {latest_run}")
        print(f"   Path: {latest_path}")
        
        return latest_path, latest_run
        
    except Exception as e:
        print(f"❌ Error finding latest Scratch run: {e}")
        raise


def parse_post_date(df):
    """
    Parse post_date from DD-MM-YYYY string format to DateType
    
    IMPROVED: Debug unparseable dates to improve parser
    
    Args:
        df: DataFrame with 'post_date' column (StringType)
    
    Returns:
        DataFrame with 'post_date' as DateType
    """
    print("🔧 Parsing post_date (DD-MM-YYYY → DateType)...")
    
    total_records = df.count()
    null_before = df.filter(F.col("post_date").isNull()).count()
    
    print(f"   Total records: {total_records:,}")
    print(f"   NULL before parsing: {null_before}")
    
    # DEBUG: Sample raw post_date values before parsing (commented to speed up)
    # print("\n   🔍 DEBUG: Sample raw post_date values:")
    # df.select("post_url", "post_date") \
    #     .filter(F.col("post_date").isNotNull()) \
    #     .show(10, truncate=False)
    
    # Split DD-MM-YYYY into components
    df_split = df.withColumn("_day", F.split(F.col("post_date"), "-").getItem(0).cast(IntegerType())) \
                 .withColumn("_month", F.split(F.col("post_date"), "-").getItem(1).cast(IntegerType())) \
                 .withColumn("_year", F.split(F.col("post_date"), "-").getItem(2).cast(IntegerType()))
    
    # Create date from components
    df_parsed = df_split.withColumn(
        "post_date_parsed",
        F.when(
            (F.col("_day").isNotNull()) & 
            (F.col("_month").isNotNull()) & 
            (F.col("_year").isNotNull()),
            F.make_date(F.col("_year"), F.col("_month"), F.col("_day"))
        ).otherwise(F.lit(None).cast(DateType()))
    )
    
    # DEBUG: Analyze unparseable dates (commented to speed up)
    # print("\n   🔍 DEBUG: Analyzing unparseable dates...")
    # df_unparseable = df_parsed.filter(
    #     (F.col("post_date").isNotNull()) &  # Has original value
    #     (F.col("post_date_parsed").isNull())  # But failed to parse
    # )
    # 
    # unparseable_count = df_unparseable.count()
    # print(f"   Unparseable dates found: {unparseable_count}")
    # 
    # if unparseable_count > 0:
    #     print("\n   📋 Sample unparseable date formats:")
    #     df_unparseable.select(
    #         "post_url",
    #         "post_date",
    #         "_day", "_month", "_year"
    #     ).show(20, truncate=False)
    #     
    #     # Count by pattern
    #     print("\n   📊 Unparseable date patterns:")
    #     df_unparseable.groupBy("post_date") \
    #         .count() \
    #         .orderBy(F.desc("count")) \
    #         .show(20, truncate=False)
    
    # Drop intermediate columns
    df_final = df_parsed.drop("_day", "_month", "_year", "post_date") \
                        .withColumnRenamed("post_date_parsed", "post_date")
    
    # Final stats
    parsed_count = df_final.filter(F.col("post_date").isNotNull()).count()
    parse_rate = (parsed_count / total_records * 100) if total_records > 0 else 0
    
    print(f"   Successfully parsed: {parsed_count:,}")
    print(f"   Parse rate: {parse_rate:.2f}%")
    
    return df_final


def parse_crawl_time(df):
    """
    Parse crawl_time from string to TimestampType
    
    Format: "Sat Sep 27 2025 00:52:04 GMT+0700 (Indochina Time)"
    
    Strategy: Remove GMT timezone part, then parse with explicit pattern
    """
    print(f"🔧 Parsing crawl_time (String → TimestampType)...")
    
    # Remove " GMT+0700 (Indochina Time)" part, keep only "Sat Sep 27 2025 00:52:04"
    df_with_timestamp = df \
        .withColumn("crawl_time_cleaned",
            F.regexp_replace(F.col("crawl_time"), r" GMT[+-]\d{4}.*$", "")
        ) \
        .withColumn("crawl_time_parsed",
            F.to_timestamp(F.col("crawl_time_cleaned"), "EEE MMM dd yyyy HH:mm:ss")
        ) \
        .drop("crawl_time", "crawl_time_cleaned") \
        .withColumnRenamed("crawl_time_parsed", "crawl_time")
    
    return df_with_timestamp


def parse_comment_time(df):
    """
    Parse comment time from mixed format to DateType
    
    Format 1: "DD-MM-YYYY" (absolute date)
    Format 2: "X ngày trước", "X tuần trước", "X tháng trước" (relative date)
    
    Strategy:
    1. Try absolute format first (DD-MM-YYYY)
    2. If fails, parse relative format using scrape_timestamp
    3. Keep as NULL if both fail
    
    Args:
        df: Input DataFrame with time as StringType, scrape_timestamp as StringType
    
    Returns:
        DataFrame with comment_date as DateType
    """
    print(f"🔧 Parsing comment time (Mixed format → DateType)...")
    
    # Parse scrape_timestamp (YYYY-MM-DDTHH-MM-SS) to date
    df = df.withColumn("_scrape_date",
        F.to_date(
            F.regexp_replace(F.col("scrape_timestamp"), "T", " ").substr(1, 10),
            "yyyy-MM-dd"
        )
    )
    
    # Try absolute format first: DD-MM-YYYY
    df = df \
        .withColumn("_parts", F.split(F.col("time"), "-")) \
        .withColumn("_day", F.col("_parts")[0].cast("int")) \
        .withColumn("_month", F.col("_parts")[1].cast("int")) \
        .withColumn("_year", F.col("_parts")[2].cast("int")) \
        .withColumn("_absolute_date",
            F.when(
                (F.col("_day").isNotNull()) & 
                (F.col("_month").isNotNull()) & 
                (F.col("_year").isNotNull()) &
                (F.size(F.col("_parts")) == 3),
                F.make_date(F.col("_year"), F.col("_month"), F.col("_day"))
            ).otherwise(F.lit(None).cast(DateType()))
        )
    
    # Parse relative format: "X ngày trước", "X tuần trước", "X tháng trước"
    df = df \
        .withColumn("_relative_num",
            F.regexp_extract(F.col("time"), r"^(\d+)\s+(ngày|tuần|tháng)", 1).cast("int")
        ) \
        .withColumn("_relative_unit",
            F.regexp_extract(F.col("time"), r"^(\d+)\s+(ngày|tuần|tháng)", 2)
        )
    
    # Calculate relative date
    df = df.withColumn("_relative_date",
        F.when(F.col("_relative_unit") == "ngày",
            F.date_sub(F.col("_scrape_date"), F.col("_relative_num"))
        ).when(F.col("_relative_unit") == "tuần",
            F.date_sub(F.col("_scrape_date"), F.col("_relative_num") * 7)
        ).when(F.col("_relative_unit") == "tháng",
            F.add_months(F.col("_scrape_date"), -F.col("_relative_num"))
        ).otherwise(F.lit(None).cast(DateType()))
    )
    
    # Combine: Use absolute if available, else use relative
    df_with_date = df \
        .withColumn("comment_date",
            F.coalesce(F.col("_absolute_date"), F.col("_relative_date"))
        ) \
        .drop("time", "_parts", "_day", "_month", "_year", "_absolute_date",
              "_relative_num", "_relative_unit", "_relative_date", "_scrape_date")
    
    # Validate parsing results
    total_count = df.count()
    parsed_count = df_with_date.filter(F.col("comment_date").isNotNull()).count()
    
    print(f"   Total comments: {total_count:,}")
    print(f"   Successfully parsed: {parsed_count:,}")
    print(f"   Parse rate: {(parsed_count / total_count * 100):.2f}%")
    
    return df_with_date


def clean_and_transform_posts(df):
    """
    Apply data cleaning and type conversions for posts
    
    Transformations:
    1. Parse post_date: String (DD-MM-YYYY) → DateType
    2. Parse crawl_time: String → TimestampType
    3. Convert metrics to INT: likes, comments_count, saves, shares
    4. Keep descriptions as String
    5. Update ingestion_timestamp to current datetime
    
    Args:
        df: Input DataFrame from Scratch
    
    Returns:
        Cleaned DataFrame with proper types
    """
    print(f"\n🧹 Applying data cleaning and transformations for POSTS...")
    
    # 1. Parse post_date (DD-MM-YYYY → DateType)
    df_cleaned = parse_post_date(df)
    
    # 2. Parse crawl_time (String → TimestampType)
    df_cleaned = parse_crawl_time(df_cleaned)
    
    # 3. Convert metrics to INT (safe casting - NULL if invalid)
    print(f"🔧 Converting metrics (String → Int)...")
    metric_columns = ['likes', 'comments_count', 'saves', 'shares',
                     'comments_level1', 'comments_level2', 'comments_loaded',
                     'comments_displayed_tiktok', 'comments_difference']
    
    for col in metric_columns:
        df_cleaned = df_cleaned.withColumn(col,
            F.when(F.col(col).isNotNull() & (F.trim(F.col(col)) != ""),
                   F.col(col).cast(IntegerType())
            ).otherwise(F.lit(None).cast(IntegerType()))
        )
    
    # 4. Update ingestion_timestamp to current datetime
    print(f"🔧 Updating ingestion_timestamp (TimestampType)...")
    df_cleaned = df_cleaned.withColumn("ingestion_timestamp", F.lit(datetime.now()))
    
    # Show sample (commented out to speed up processing)
    # print(f"\n📋 Sample cleaned posts data:")
    # df_cleaned.select(
    #     "post_url", "author", "post_date", "likes", "comments_count"
    # ).show(5, truncate=False)
    
    return df_cleaned


def clean_and_transform_comments(df):
    """
    Apply data cleaning and type conversions for comments
    
    Transformations:
    1. Parse time (mixed format) → DateType as comment_date
    2. Convert likes, number_of_replies to INT
    3. Convert stt to INT
    4. Trim whitespace from text fields
    5. Update ingestion_timestamp to current datetime
    
    Args:
        df: Input DataFrame from Scratch
    
    Returns:
        Cleaned DataFrame with proper types
    """
    print(f"\n🧹 Applying data cleaning and transformations for COMMENTS...")
    
    # 1. Parse comment time (mixed format → DateType)
    df_cleaned = parse_comment_time(df)
    
    # 2. Convert stt to INT (comment sequence number)
    print(f"🔧 Converting stt (String → Int)...")
    df_cleaned = df_cleaned.withColumn("stt",
        F.when(F.col("stt").isNotNull() & (F.trim(F.col("stt")) != ""),
               F.col("stt").cast(IntegerType())
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    
    # 3. Convert likes, number_of_replies to INT
    print(f"🔧 Converting likes and number_of_replies (String → Int)...")
    for col in ['likes', 'number_of_replies']:
        df_cleaned = df_cleaned.withColumn(col,
            F.when(F.col(col).isNotNull() & (F.trim(F.col(col)) != ""),
                   F.col(col).cast(IntegerType())
            ).otherwise(F.lit(None).cast(IntegerType()))
        )
    
    # 4. Trim whitespace from text fields
    print(f"🔧 Trimming whitespace from text fields...")
    text_columns = ['ten', 'tag_ten', 'comment', 'replied_to_tag_name']
    for col in text_columns:
        df_cleaned = df_cleaned.withColumn(col, F.trim(F.col(col)))
    
    # 5. Filter out invalid comments (empty or NULL comment text)
    print(f"🔧 Filtering out invalid comments...")
    count_before = df_cleaned.count()
    df_filtered = df_cleaned.filter(
        (F.col("comment").isNotNull()) & 
        (F.col("comment") != "")
    )
    count_after = df_filtered.count()
    removed_count = count_before - count_after
    removed_pct = (removed_count / count_before * 100) if count_before > 0 else 0
    print(f"   Removed {removed_count:,} comments with empty/NULL text ({removed_pct:.2f}%)")
    print(f"   Valid comments remaining: {count_after:,}")
    
    # 6. Update ingestion_timestamp to current datetime
    print(f"🔧 Updating ingestion_timestamp (TimestampType)...")
    df_final = df_filtered.withColumn("ingestion_timestamp", F.lit(datetime.now()))
    
    # Show sample (commented out to speed up processing - would run 1,380+ times)
    # print(f"\n📋 Sample cleaned comments data:")
    # df_final.select(
    #     "post_url", "ten", "comment", "comment_date", "likes"
    # ).show(5, truncate=False)
    
    return df_final


def create_silver_posts_table(spark):
    """Create Silver table for posts metadata if not exists"""
    
    schema = StructType([
        # Primary key
        StructField("post_url", StringType(), False),
        
        # Author info
        StructField("author", StringType(), True),
        StructField("author_tag", StringType(), True),
        StructField("author_url", StringType(), True),
        
        # Post info
        StructField("post_date", DateType(), True),  # ✅ DateType (cleaned)
        StructField("post_description", StringType(), True),
        
        # Engagement metrics
        StructField("likes", IntegerType(), True),  # ✅ IntegerType (cleaned)
        StructField("comments_count", IntegerType(), True),
        StructField("saves", IntegerType(), True),
        StructField("shares", IntegerType(), True),
        
        # Comment statistics
        StructField("comments_level1", IntegerType(), True),
        StructField("comments_level2", IntegerType(), True),
        StructField("comments_loaded", IntegerType(), True),
        StructField("comments_displayed_tiktok", IntegerType(), True),
        StructField("comments_difference", IntegerType(), True),
        
        # Crawl metadata
        StructField("crawl_time", TimestampType(), True),  # ✅ TimestampType (cleaned)
        StructField("scrape_timestamp", StringType(), True),
        
        # Checksum for deduplication
        StructField("row_checksum", StringType(), False),
        
        # Ingestion metadata
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False),
        StructField("source_file_size_bytes", LongType(), False)
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        catalog="silver",
        database=SILVER_DATABASE,
        table_name=TABLE_NAME_POSTS,
        schema=schema,
        partition_by=PARTITION_COLUMNS_POSTS,
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def create_silver_comments_table(spark):
    """Create Silver table for comments if not exists"""
    
    schema = StructType([
        # Link to post
        StructField("post_url", StringType(), False),
        
        # Comment identifiers
        StructField("stt", IntegerType(), True),  # ✅ IntegerType (cleaned)
        
        # Commenter info
        StructField("ten", StringType(), True),
        StructField("tag_ten", StringType(), True),
        StructField("url", StringType(), True),
        
        # Comment data
        StructField("comment", StringType(), False),
        StructField("comment_date", DateType(), True),  # ✅ DateType (cleaned from time)
        StructField("likes", IntegerType(), True),  # ✅ IntegerType (cleaned)
        
        # Reply info
        StructField("level_comment", StringType(), True),
        StructField("replied_to_tag_name", StringType(), True),
        StructField("number_of_replies", IntegerType(), True),  # ✅ IntegerType (cleaned)
        
        # Scrape metadata
        StructField("scrape_timestamp", StringType(), True),
        
        # Checksum for deduplication
        StructField("row_checksum", StringType(), False),
        
        # Ingestion metadata
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False),
        StructField("source_file_size_bytes", LongType(), False)
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        catalog="silver",
        database=SILVER_DATABASE,
        table_name=TABLE_NAME_COMMENTS,
        schema=schema,
        partition_by=PARTITION_COLUMNS_COMMENTS,
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


# =============================================================================
# BATCH PROCESSING APPROACH
# =============================================================================
# Strategy: Process 30 partitions at a time to reduce Iceberg append operations
# Benefits:
#   - 1379 appends → 46 appends (96.7% reduction)
#   - Less pressure on Hive Metastore
#   - Maintain file-level tracking in PostgreSQL


# REMOVED: prepare_unprocessed_partitions() - No longer needed with new approach


def process_batches(spark, unprocessed_items, scratch_path_posts, scratch_path_comments):
    """
    PHASE 2: PER-FILE PROCESSING - Process each file completely (posts + comments).
    
    Strategy: For each item (metadata dict) in batch:
      1. Read partition for posts
      2. Clean & append posts (with duplicate check)
      3. Read partition for comments
      4. Clean & append comments
      5. Log 1 entry (atomic - success only if BOTH OK)
    
    Args:
        spark: SparkSession instance
        unprocessed_items: List of metadata dicts (each has post_url, checksum, etc.)
        scratch_path_posts: Scratch path for posts
        scratch_path_comments: Scratch path for comments
    
    Returns:
        Dict with statistics: posts_loaded, comments_loaded, files_success, files_failed
    """
    print(f"\n{'='*80}")
    print(f"📦 PHASE 2: PER-FILE PROCESSING - Processing {len(unprocessed_items)} files")
    print(f"{'='*80}")
    
    # Create batches (for organized display, not for bulk processing)
    batches = create_batches(unprocessed_items, BATCH_SIZE)
    
    stats = {
        "posts_loaded": 0,
        "comments_loaded": 0,
        "files_success": 0,
        "files_failed": 0,
        "batches_completed": 0
    }
    
    # Process each batch (but process files individually within batch)
    for batch_id, batch_urls in enumerate(batches, 1):
        print(f"\n{'='*80}")
        print(f"📦 BATCH {batch_id}/{len(batches)}: Processing {len(batch_urls)} files")
        print(f"{'='*80}")
        
        batch_success = 0
        batch_failed = 0
        
        # Process each file in batch
        for idx, item_metadata in enumerate(batch_urls, 1):
            print(f"\n[{idx}/{len(batch_urls)}]", end=" ")
            
            # Check if SparkContext is still alive before processing
            if spark.sparkContext._jsc is None or spark.sparkContext._jsc.sc().isStopped():
                print(f"❌ SparkContext stopped - aborting remaining files")
                print(f"   Already processed: {batch_success} files")
                print(f"   Remaining in batch: {len(batch_urls) - idx + 1} files")
                stats["files_failed"] += len(batch_urls) - idx + 1
                break
            
            try:
                # Extract metadata from item
                post_url = item_metadata["post_url"]
                file_meta = {
                    "source_file": item_metadata["source_file"],
                    "source_file_checksum": item_metadata["source_file_checksum"],
                    "source_file_size_bytes": item_metadata["source_file_size_bytes"]
                }
                
                posts_count, comments_count, status, error_msg = process_single_post_url(
                    spark=spark,
                    post_url=post_url,
                    file_meta=file_meta,
                    scratch_path_posts=scratch_path_posts,
                    scratch_path_comments=scratch_path_comments,
                    silver_table_posts=SILVER_TABLE_POSTS,
                    silver_table_comments=SILVER_TABLE_COMMENTS,
                    business_columns_posts=BUSINESS_COLUMNS_POSTS,
                    business_columns_comments=BUSINESS_COLUMNS_COMMENTS,
                    cleaning_func_posts=clean_and_transform_posts,
                    cleaning_func_comments=clean_and_transform_comments,
                    postgres_conn=POSTGRES_CONN,
                    bronze_base_path=BRONZE_BASE_PATH,
                    batch_id=batch_id
                )
                
                if status == 'success':
                    stats["posts_loaded"] += posts_count
                    stats["comments_loaded"] += comments_count
                    stats["files_success"] += 1
                    batch_success += 1
                else:
                    stats["files_failed"] += 1
                    batch_failed += 1
                    
            except Exception as e:
                print(f"   ❌ Unexpected error: {e}")
                stats["files_failed"] += 1
                batch_failed += 1
                
                if not CONTINUE_ON_BATCH_FAILURE:
                    print(f"   Stopping execution (CONTINUE_ON_BATCH_FAILURE=False)")
                    return stats
        
        stats["batches_completed"] += 1
        
        print(f"\n{'='*40}")
        print(f"📊 Batch {batch_id}/{len(batches)} summary:")
        print(f"   Success: {batch_success}/{len(batch_urls)}")
        print(f"   Failed: {batch_failed}/{len(batch_urls)}")
        print(f"{'='*40}")
    
    return stats


def print_summary(stats, total_files):
    """
    PHASE 3: SUMMARY - Print final statistics.
    
    Args:
        stats: Statistics dict from process_batches()
        total_files: Total number of files attempted
    """
    print(f"\n{'='*80}")
    print(f"📊 FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"   Total files: {total_files}")
    print(f"   Files success: {stats['files_success']}")
    print(f"   Files failed: {stats['files_failed']}")
    print(f"   Posts loaded: {stats['posts_loaded']:,}")
    print(f"   Comments loaded: {stats['comments_loaded']:,}")
    print(f"   Batches completed: {stats['batches_completed']}")
    
    if stats['files_failed'] > 0:
        success_rate = (stats['files_success'] / total_files * 100) if total_files > 0 else 0
        print(f"\n⚠️  {stats['files_failed']} files failed ({success_rate:.1f}% success rate)")
        print(f"   Check PostgreSQL file_ingestion_log WHERE status='failed' for details")
    else:
        print(f"\n✅ All files completed successfully!")


def clean_and_load_to_silver(spark):
    """
    Main ETL: Read Scratch Parquet → Clean (Per-File) → Load to Silver
    
    PER-FILE PROCESSING APPROACH:
    1. Phase 1: PREPARE - List partitions, build mapping, filter unprocessed
    2. Phase 2: PER-FILE PROCESSING - Process each file completely (posts + comments)
    3. Phase 3: SUMMARY - Report statistics
    
    Returns:
        tuple: (posts_loaded, comments_loaded)
    """
    print(f"🚀 STEP 2: Clean & Load (Scratch → Silver) - PER-FILE PROCESSING")
    print(f"   Source 1 (Posts): {SCRATCH_BASE_PATH_POSTS}")
    print(f"   Source 2 (Comments): {SCRATCH_BASE_PATH_COMMENTS}")
    print(f"   Target 1: {SILVER_TABLE_POSTS}")
    print(f"   Target 2: {SILVER_TABLE_COMMENTS}")
    print(f"   Batch size: {BATCH_SIZE} files (for display organization)")
    
    # Get latest Scratch runs
    scratch_path_posts, run_id_posts = get_latest_scratch_run(spark, SCRATCH_BASE_PATH_POSTS)
    scratch_path_comments, run_id_comments = get_latest_scratch_run(spark, SCRATCH_BASE_PATH_COMMENTS)
    
    print(f"\n   Latest runs:")
    print(f"   - Posts: {run_id_posts}")
    print(f"   - Comments: {run_id_comments}")
    
    # Phase 1: PREPARE (with enhanced debugging)
    print(f"\n{'='*80}")
    print(f"📋 PHASE 1: PREPARE - List partitions & build mapping")
    print(f"{'='*80}")
    
    # List partitions from posts
    # posts_partitions = list_scratch_partitions(spark, scratch_path_posts)
    file_combinations = list_scratch_partitions(spark, scratch_path_posts)

    # Build mapping (includes debug info for 1502 vs 1400 issue)
    # mapping = build_partition_to_file_mapping(spark, scratch_path_posts, posts_partitions)
    mapping = build_partition_to_file_mapping(spark, scratch_path_posts, file_combinations)
    # Filter unprocessed
    unprocessed_items = filter_unprocessed_partitions(spark, mapping, POSTGRES_CONN, layer='silver')
    
    if not unprocessed_items:
        print(f"\n✅ All files already processed!")
        return 0, 0
    
    # Phase 2: PER-FILE PROCESSING
    stats = process_batches(
        spark,
        unprocessed_items,
        scratch_path_posts,
        scratch_path_comments
    )
    
    # Phase 3: SUMMARY
    # print_summary(stats, len(unprocessed_urls))
    print_summary(stats, len(unprocessed_items))

    
    return stats["posts_loaded"], stats["comments_loaded"]


def main():
    print("=" * 80)
    print("🔄 SILVER TIKTOK COMMENTS - STEP 2: CLEAN & LOAD (Scratch → Silver)")
    print("=" * 80)
    
    spark = get_spark_session(app_name="Silver_TikTok_Comments_Step2_Clean_Load")
    # Use legacy time parser policy to preserve pre-Spark-3.0 datetime parsing behavior
    # This helps recognize patterns like 'EEE MMM dd yyyy HH:mm:ss' when parsing textual
    # timestamps that include short weekday/month names. See Spark docs and upgrade notes.
    try:
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        print("⚙️  Set spark.sql.legacy.timeParserPolicy = LEGACY")
    except Exception:
        # Fail-open: if we cannot set the config for some reason, continue and let
        # the job fail later so the error is visible in logs.
        print("⚠️  Could not set spark.sql.legacy.timeParserPolicy; proceeding without it")
    
    try:
        print("\n1️⃣  Creating Silver table schemas...")
        create_silver_posts_table(spark)
        create_silver_comments_table(spark)
        
        print("\n2️⃣  Cleaning and loading to Silver...")
        posts_count, comments_count = clean_and_load_to_silver(spark)
        
        print("\n" + "=" * 80)
        if posts_count > 0 or comments_count > 0:
            print(f"✅ STEP 2 COMPLETED:")
            print(f"   Posts loaded: {posts_count:,}")
            print(f"   Comments loaded: {comments_count:,}")
        else:
            print(f"✅ STEP 2 COMPLETED: No new records to load")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
