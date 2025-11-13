"""
Step 2: Clean & Load - TikTok Comments (Scratch → Silver)

Purpose: Apply data cleaning, type conversion, deduplication, and load to Silver
Strategy:
  - Read from 2 Scratch Parquet folders (posts + comments)
  - Apply data cleaning:
    * Posts: Parse dates (post_date, crawl_time), convert metrics to INT
    * Comments: Parse time (mixed format: DD-MM-YYYY or relative), convert likes to INT
  - Calculate row_checksum:
    * Posts: post_url only (primary key)
    * Comments: post_url + stt + ten + comment + time (unique identifier)
  - Deduplicate:
    * Posts: LEFT ANTI JOIN on post_url (already exists?)
    * Comments: LEFT ANTI JOIN on row_checksum
  - APPEND to 2 Silver Iceberg tables
  - Log to PostgreSQL tracking table

Input: Scratch Parquet files (2 folders)
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
    POSTGRES_CONN
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


# NO DEDUPLICATION NEEDED!
# Strategy: Crawler tool only scrapes NEW videos (1 video = 1 file)
# Each file is unique (never re-scrapes same video)
# Therefore: Direct APPEND without deduplication


def clean_and_load_to_silver(spark):
    """
    Main ETL: Read Scratch Parquet → Clean → Deduplicate → Load to Silver
    
    Process 2 tables separately:
    1. Posts metadata
    2. Comments data
    
    Returns:
        tuple: (posts_loaded, comments_loaded)
    """
    print(f"🚀 STEP 2: Clean & Load (Scratch → Silver)")
    print(f"   Source 1: {SCRATCH_BASE_PATH_POSTS}")
    print(f"   Source 2: {SCRATCH_BASE_PATH_COMMENTS}")
    print(f"   Target 1: {SILVER_TABLE_POSTS}")
    print(f"   Target 2: {SILVER_TABLE_COMMENTS}")
    
    # =========================================================================
    # PART 1: POSTS METADATA
    # =========================================================================
    print(f"\n" + "=" * 80)
    print(f"📋 PART 1: PROCESSING POSTS METADATA")
    print(f"=" * 80)
    
    # Get latest Scratch run for posts
    scratch_path_posts, run_id_posts = get_latest_scratch_run(spark, SCRATCH_BASE_PATH_POSTS)
    
    # Read from Scratch Parquet
    print(f"\n📖 Reading posts from Scratch bucket...")
    df_posts_scratch = spark.read.parquet(scratch_path_posts)
    
    posts_scratch_count = df_posts_scratch.count()
    print(f"📝 Posts from Scratch: {posts_scratch_count:,}")
    
    posts_loaded = 0
    posts_skipped = 0
    
    if posts_scratch_count > 0:
        # Get source file metadata (for Bronze file path)
        source_metadata_posts = df_posts_scratch.select(
            "source_file", 
            "source_file_checksum", 
            "source_file_size_bytes"
        ).first()
        
        bronze_file_path_posts = f"{BRONZE_BASE_PATH}/{source_metadata_posts['source_file']}"
        file_checksum_posts = source_metadata_posts['source_file_checksum']
        file_size_bytes_posts = source_metadata_posts['source_file_size_bytes']
        
        # Check if already processed
        if check_if_file_ingested(file_checksum_posts, POSTGRES_CONN, layer='silver'):
            print(f"\n⏭️  Posts already processed (found in PostgreSQL tracking) - skipping")
            posts_skipped = posts_scratch_count
        else:
            # Apply cleaning and transformations
            df_posts_cleaned = clean_and_transform_posts(df_posts_scratch)
            
            # Calculate row_checksum (post_url only - for tracking)
            print(f"\n🔐 Calculating row_checksum for posts (post_url)...")
            df_posts_with_checksum = calculate_row_checksum(df_posts_cleaned, BUSINESS_COLUMNS_POSTS)
            
            # NO DEDUPLICATION - Direct APPEND
            # Reason: Crawler never re-scrapes same video (1 video = 1 file = unique)
            print(f"\n📝 Strategy: Direct APPEND (no deduplication needed)")
            print(f"   Reason: Tool only scrapes NEW videos, never re-scrapes old ones")
            
            posts_new_count = df_posts_with_checksum.count()
            
            # Write to Silver table (APPEND mode)
            print(f"\n💾 Appending {posts_new_count:,} posts to Silver table...")
            df_posts_with_checksum.writeTo(SILVER_TABLE_POSTS) \
                .using("iceberg") \
                .append()
            
            print(f"   ✅ Successfully appended {posts_new_count:,} posts")
            posts_loaded = posts_new_count
            
            # Log to PostgreSQL
            print(f"\n📝 Logging posts to PostgreSQL...")
            ingestion_details = {
                "strategy": "Direct APPEND (no deduplication - crawler never re-scrapes same video)",
                "records_appended": posts_loaded,
                "transformations": {
                    "post_date": "DD-MM-YYYY → DateType",
                    "crawl_time": "String → TimestampType",
                    "metrics": "String → IntegerType"
                }
            }
            
            log_ingestion_to_postgres(
                file_path=bronze_file_path_posts,
                file_checksum=file_checksum_posts,
                records_ingested=posts_loaded,
                table_name=f"silver.{TABLE_NAME_POSTS}",
                status="success",
                postgres_conn_params=POSTGRES_CONN,
                layer='silver',
                ingestion_details=ingestion_details,
                file_size_bytes=file_size_bytes_posts
            )
            print(f"   ✅ Posts logged successfully")
    
    # =========================================================================
    # PART 2: COMMENTS DATA (PARTITION-BY-PARTITION PROCESSING)
    # =========================================================================
    print(f"\n" + "=" * 80)
    print(f"💬 PART 2: PROCESSING COMMENTS DATA")
    print(f"=" * 80)
    
    # Get latest Scratch run for comments
    scratch_path_comments, run_id_comments = get_latest_scratch_run(spark, SCRATCH_BASE_PATH_COMMENTS)
    
    # Get list of post_url partitions
    print(f"\n📖 Reading post_url partitions from Scratch bucket...")
    df_temp = spark.read.parquet(scratch_path_comments)
    
    post_urls = [row.post_url for row in df_temp.select("post_url").distinct().collect()]
    total_posts = len(post_urls)
    
    print(f"📝 Found {total_posts:,} post_url partitions in Scratch")
    
    # Build mapping: post_url -> source file metadata (for Bronze file path and size)
    print(f"\n📋 Building source file metadata mapping...")
    source_metadata_map = {}
    
    for row in df_temp.select("post_url", "source_file", "source_file_checksum", "source_file_size_bytes").distinct().collect():
        source_metadata_map[row.post_url] = {
            "source_file": row.source_file,
            "source_file_checksum": row.source_file_checksum,
            "source_file_size_bytes": row.source_file_size_bytes
        }
    
    print(f"   ✅ Mapped {len(source_metadata_map)} posts to source files")
    
    comments_loaded = 0
    files_skipped = 0
    
    if total_posts > 0:
        print(f"\n📝 Strategy: Process each post_url partition separately")
        print(f"   Reason: Avoid OOM with 195k comments - process in small batches")
        print(f"\n{'='*80}")
        print(f"🔄 PROCESSING {total_posts:,} POSTS (one at a time)")
        print(f"{'='*80}")
        
        # Process each post_url partition
        for idx, post_url in enumerate(post_urls, 1):
            print(f"\n📊 [{idx}/{total_posts}] Processing post: {post_url}")
            
            # Get source file metadata for this post (from Bronze)
            source_metadata = source_metadata_map.get(post_url)
            
            if not source_metadata:
                print(f"   ⚠️  Warning: No source metadata found for this post - skipping")
                continue
            
            bronze_file_path = f"{BRONZE_BASE_PATH}/{source_metadata['source_file']}"
            file_checksum = source_metadata['source_file_checksum']
            file_size_bytes = source_metadata['source_file_size_bytes']
            
            # Check if this file already processed (logged in PostgreSQL)
            if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
                print(f"   ⏭️  Already processed (found in PostgreSQL tracking) - skipping")
                continue
            
            # Read only this post_url partition
            df_post_comments = spark.read.parquet(scratch_path_comments) \
                .filter(F.col("post_url") == post_url)
            
            comment_count = df_post_comments.count()
            print(f"   💬 Comments in this post: {comment_count:,}")
            
            if comment_count == 0:
                print(f"   ⏭️  Skipping - no comments in partition")
                # Log as skipped (0 comments)
                ingestion_details = {
                    "strategy": "Partition-by-partition processing to avoid OOM",
                    "skip_reason": "No comments found in partition",
                    "post_url": post_url
                }
                
                log_ingestion_to_postgres(
                    file_path=bronze_file_path,
                    file_checksum=file_checksum,
                    records_ingested=0,
                    table_name=f"silver.{TABLE_NAME_COMMENTS}",
                    status="skipped",
                    postgres_conn_params=POSTGRES_CONN,
                    layer='silver',
                    ingestion_details=ingestion_details,
                    file_size_bytes=file_size_bytes
                )
                print(f"   📝 Logged as skipped (0 comments)")
                files_skipped += 1
                continue
            
            # Clean and transform this partition
            df_cleaned = clean_and_transform_comments(df_post_comments)
            
            # Check if any valid comments remain after filtering
            valid_comment_count = df_cleaned.count()
            
            if valid_comment_count == 0:
                print(f"   ⏭️  Skipping - no valid comments after filtering (all comments empty)")
                # Log as skipped (all comments filtered out)
                ingestion_details = {
                    "strategy": "Partition-by-partition processing to avoid OOM",
                    "skip_reason": "All comments filtered out (empty/NULL text)",
                    "post_url": post_url,
                    "original_comments": comment_count,
                    "valid_comments": 0,
                    "filtered_out": comment_count
                }
                
                log_ingestion_to_postgres(
                    file_path=bronze_file_path,
                    file_checksum=file_checksum,
                    records_ingested=0,
                    table_name=f"silver.{TABLE_NAME_COMMENTS}",
                    status="skipped",
                    postgres_conn_params=POSTGRES_CONN,
                    layer='silver',
                    ingestion_details=ingestion_details,
                    file_size_bytes=file_size_bytes
                )
                print(f"   📝 Logged as skipped (all comments empty)")
                files_skipped += 1
                continue
            
            # Calculate row_checksum
            df_with_checksum = calculate_row_checksum(df_cleaned, BUSINESS_COLUMNS_COMMENTS)
            
            # Write to Silver (direct append, small batch)
            df_with_checksum \
                .coalesce(1) \
                .writeTo(SILVER_TABLE_COMMENTS) \
                .using("iceberg") \
                .append()
            
            comments_loaded += valid_comment_count
            print(f"   ✅ Appended {valid_comment_count:,} comments to Silver")
            
            # Log this file to PostgreSQL immediately (SUCCESS)
            ingestion_details = {
                "strategy": "Partition-by-partition processing to avoid OOM",
                "post_url": post_url,
                "records_appended": {
                    "original_comments": comment_count,
                    "valid_comments": valid_comment_count,
                    "filtered_out": comment_count - valid_comment_count
                },
                "transformations": {
                    "comment_date": "Mixed format → DateType",
                    "stt_likes_replies": "String → IntegerType",
                    "text_fields": "Trimmed whitespace",
                    "filtering": "Removed empty/NULL comments"
                }
            }
            
            log_ingestion_to_postgres(
                file_path=bronze_file_path,
                file_checksum=file_checksum,
                records_ingested=valid_comment_count,
                table_name=f"silver.{TABLE_NAME_COMMENTS}",
                status="success",
                postgres_conn_params=POSTGRES_CONN,
                layer='silver',
                ingestion_details=ingestion_details,
                file_size_bytes=file_size_bytes
            )
            print(f"   📝 Logged to PostgreSQL")
        
        print(f"\n{'='*80}")
        print(f"✅ ALL PARTITIONS PROCESSED")
        print(f"   Posts loaded: {comments_loaded:,} comments")
        print(f"   Files skipped: {files_skipped:,} (logged with status='skipped')")
        print(f"{'='*80}")
        
        # Show final distribution
        if comments_loaded > 0:
            print(f"\n📊 Final Silver comments stats:")
            silver_comments = spark.table(SILVER_TABLE_COMMENTS)
            total_silver_comments = silver_comments.count()
            print(f"   Total comments in Silver: {total_silver_comments:,}")
            
            print(f"\n   Top 10 posts by comment count:")
            silver_comments.groupBy("post_url") \
                .count() \
                .orderBy(F.desc("count")) \
                .show(10, truncate=False)
    
    # Note: Posts and comments both logged per-file during processing loop
    print(f"\n📝 Logging summary:")
    print(f"   All files logged to PostgreSQL during processing")
    print(f"   - Success: Files with valid data appended to Silver")
    print(f"   - Skipped: Files with no/empty comments (for future skip check)")
    
    return posts_loaded, comments_loaded


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
