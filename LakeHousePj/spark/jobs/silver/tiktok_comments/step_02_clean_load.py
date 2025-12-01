"""
Step 2: Clean & Load - TikTok Comments (Scratch → Silver) - BATCH PROCESSING

Purpose: Apply data cleaning, type conversion, and load to Silver using batch processing
Strategy:
  - Phase 1: PREPARE
    * List partitions from Scratch
    * Build mapping: partition → source file metadata
    * Filter unprocessed partitions (check PostgreSQL tracking)
  
  - Phase 2: BATCH PROCESSING (30 files per batch)
    * Posts: Read 30 files → Union → Clean → Single APPEND
    * Comments: Read 30 files → Union → Clean → Single APPEND
    * Log each file individually to PostgreSQL (calculate counts from final DataFrame)
    * Repeat for next batch
  
  - Phase 3: EXCEPTION HANDLING
    * Per-batch error handling (continue on failure)
    * Failed files logged with status='failed'
    * Summary statistics reported

Benefits:
  - Reduce Iceberg append operations: ~2,830 → ~92 (96.7% reduction)
  - Reduce Hive Metastore snapshots and memory pressure
  - Maintain granular file-level tracking in PostgreSQL
  - Partition by crawl_date/scrape_date instead of post_url for better performance

Input: Scratch Parquet files (unpartitioned folders)
Output: 2 Silver Iceberg tables:
  - silver.silver.tiktok_post_metadata (partitioned by crawl_date)
  - silver.silver.tiktok_post_comments (partitioned by scrape_date)
"""

import sys
import os
import re

sys.path.append('/opt/spark/jobs')

from datetime import datetime, timedelta
from functools import reduce
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


def parse_tiktok_number(value_col):
    """
    Parse TikTok number format to integer
    
    Handles:
    - Plain numbers: "1234" → 1234
    - K format: "36.6K" → 36600, "22K" → 22000
    - M format: "1.5M" → 1500000, "1M" → 1000000
    - N/A or invalid → NULL
    
    Args:
        value_col: Column with string values
    
    Returns:
        Column expression that parses to IntegerType
    """
    from pyspark.sql.types import IntegerType
    
    # Extract number and unit
    # Pattern: optional digits, optional decimal point, digits, optional K/M
    # Examples: "36.6K", "22K", "1.5M", "1M", "1234"
    trimmed = F.trim(value_col)
    
    # Case 1: Plain number (all digits)
    plain_number = F.when(
        trimmed.rlike("^\\d+$"),
        trimmed.cast(IntegerType())
    )
    
    # Case 2: K format (thousands) - e.g., "36.6K" → 36600, "22K" → 22000
    k_pattern = F.regexp_extract(trimmed, r"^([\d.]+)K$", 1)
    k_value = F.when(
        trimmed.rlike("^[\\d.]+K$"),
        (k_pattern.cast("double") * 1000).cast(IntegerType())
    )
    
    # Case 3: M format (millions) - e.g., "1.5M" → 1500000, "1M" → 1000000
    m_pattern = F.regexp_extract(trimmed, r"^([\d.]+)M$", 1)
    m_value = F.when(
        trimmed.rlike("^[\\d.]+M$"),
        (m_pattern.cast("double") * 1000000).cast(IntegerType())
    )
    
    # Combine: try plain number first, then K, then M, else NULL
    return F.when(
        value_col.isNotNull() & (trimmed != "") & (trimmed != "N/A"),
        F.coalesce(plain_number, k_value, m_value)
    ).otherwise(F.lit(None).cast(IntegerType()))


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
    3. Extract crawl_date from crawl_time (for partitioning)
    4. Convert metrics to INT: likes, comments_count, saves, shares
    5. Keep descriptions as String
    6. Update ingestion_timestamp to current datetime
    
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
    
    # 3. Extract crawl_date from crawl_time (for partitioning)
    print(f"🔧 Extracting crawl_date from crawl_time...")
    df_cleaned = df_cleaned.withColumn("crawl_date", F.to_date(F.col("crawl_time")))
    
    # 4. Convert metrics to INT (parse TikTok format: K, M, plain numbers)
    print(f"🔧 Converting metrics (String → Int, parsing TikTok format: K/M)...")
    # likes: số lượng like của bài đăng
    # comments_count: số lượng comment hiển thị trên TikTok
    # saves: số lượt lưu (bookmark)
    # shares: số lượt chia sẻ
    # comments_level1: số comment cấp 1 (bình luận trực tiếp)
    # comments_level2: số comment cấp 2 (trả lời comment cấp 1)
    # comments_loaded: số comment tool crawl được
    # comments_displayed_tiktok: số comment thực của video (ground truth)
    # comments_difference: chênh lệch (comments_displayed_tiktok - comments_loaded)
    metric_columns = ['likes', 'comments_count', 'saves', 'shares',
                     'comments_level1', 'comments_level2', 'comments_loaded',
                     'comments_displayed_tiktok', 'comments_difference']
    
    for col in metric_columns:
        # Use parse_tiktok_number to handle: plain numbers, K format, M format, N/A
        df_cleaned = df_cleaned.withColumn(col, parse_tiktok_number(F.col(col)))
    
    # 5. Update ingestion_timestamp to current datetime
    print(f"🔧 Updating ingestion_timestamp (TimestampType)...")
    df_cleaned = df_cleaned.withColumn("ingestion_timestamp", F.lit(datetime.now()))
    
    # 6. Filter out posts with shares > 5M (invalid crawl data)
    print(f"🔧 Filtering out posts with shares > 5M (invalid crawl data)...")
    count_before = df_cleaned.count()
    df_filtered = df_cleaned.filter(
        F.col("shares").isNull() | (F.col("shares") <= 5000000)
    )
    count_after = df_filtered.count()
    removed_count = count_before - count_after
    if removed_count > 0:
        removed_pct = (removed_count / count_before * 100) if count_before > 0 else 0
        print(f"   Removed {removed_count:,} posts with shares > 5M ({removed_pct:.2f}%)")
        print(f"   Valid posts remaining: {count_after:,}")
    
    # Show sample (commented out to speed up processing)
    # print(f"\n📋 Sample cleaned posts data:")
    # df_filtered.select(
    #     "post_url", "author", "post_date", "likes", "comments_count"
    # ).show(5, truncate=False)
    
    return df_filtered


def clean_and_transform_comments(df):
    """
    Apply data cleaning and type conversions for comments
    
    Transformations:
    1. Parse time (mixed format) → DateType as comment_date
    2. Extract scrape_date from scrape_timestamp (for partitioning)
    3. Convert likes, number_of_replies to INT
    4. Convert stt to INT
    5. Trim whitespace from text fields
    6. Validate level_comment (filter column shift errors)
    7. Filter out invalid comments (empty or NULL comment text)
    8. Update ingestion_timestamp to current datetime
    
    Args:
        df: Input DataFrame from Scratch
    
    Returns:
        Cleaned DataFrame with proper types
    """
    print(f"\n🧹 Applying data cleaning and transformations for COMMENTS...")
    
    # 1. Parse comment time (mixed format → DateType)
    df_cleaned = parse_comment_time(df)
    
    # 2. Extract scrape_date from scrape_timestamp (for partitioning)
    print(f"🔧 Extracting scrape_date from scrape_timestamp...")
    df_cleaned = df_cleaned.withColumn("scrape_date",
        F.to_date(
            F.regexp_replace(F.col("scrape_timestamp"), "T", " ").substr(1, 10),
            "yyyy-MM-dd"
        )
    )
    
    # 3. Convert stt to INT (comment sequence number) - plain number only
    print(f"🔧 Converting stt (String → Int)...")
    df_cleaned = df_cleaned.withColumn("stt",
        F.when(
            F.col("stt").isNotNull() & 
            (F.trim(F.col("stt")) != "") &
            (F.trim(F.col("stt")) != "N/A") &
            (F.trim(F.col("stt")).rlike("^\\d+$")),  # Only digits (no K/M for sequence numbers)
            F.trim(F.col("stt")).cast(IntegerType())
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    
    # 4. Convert likes, number_of_replies to INT (parse TikTok format: K, M)
    print(f"🔧 Converting likes and number_of_replies (String → Int, parsing TikTok format: K/M)...")
    for col in ['likes', 'number_of_replies']:
        df_cleaned = df_cleaned.withColumn(col, parse_tiktok_number(F.col(col)))
    
    # 5. Trim whitespace from text fields
    print(f"🔧 Trimming whitespace from text fields...")
    text_columns = ['ten', 'tag_ten', 'comment', 'replied_to_tag_name']
    for col in text_columns:
        df_cleaned = df_cleaned.withColumn(col, F.trim(F.col(col)))
    
    # 6. Validate level_comment (filter column shift errors)
    print(f"🔧 Validating level_comment (filter column shift errors)...")
    count_before_validation = df_cleaned.count()
    
    df_validated = df_cleaned.filter(
        F.col("level_comment").isNull() |
        F.col("level_comment").isin("Yes", "No", "yes", "no", "YES", "NO")
    )
    
    count_after_validation = df_validated.count()
    invalid_count = count_before_validation - count_after_validation
    invalid_pct = (invalid_count / count_before_validation * 100) if count_before_validation > 0 else 0
    
    print(f"   Removed {invalid_count:,} comments with invalid level_comment ({invalid_pct:.2f}%)")
    print(f"   ℹ️  Invalid values indicate column shift from Bronze parser (csv.DictReader)")
    print(f"   Valid comments remaining: {count_after_validation:,}")
    
    df_cleaned = df_validated
    
    # 7. Filter out invalid comments (empty or NULL comment text)
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
    
    # 8. Update ingestion_timestamp to current datetime
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
        StructField("post_url", StringType(), False),  # Link bài viết TikTok (duy nhất)
        
        # Author info
        StructField("author", StringType(), True),  # Tên hiển thị của tác giả
        StructField("author_tag", StringType(), True),  # Username/handle (@author)
        StructField("author_url", StringType(), True),  # URL trang cá nhân tác giả
        
        # Post info
        StructField("post_date", DateType(), True),  # Ngày video được đăng
        StructField("post_description", StringType(), True),  # Mô tả/caption bài đăng
        
        # Engagement metrics
        StructField("likes", IntegerType(), True),  # Số lượng like
        StructField("comments_count", IntegerType(), True),  # Số lượng comment hiển thị
        StructField("saves", IntegerType(), True),  # Số lượt lưu (bookmark)
        StructField("shares", IntegerType(), True),  # Số lượt chia sẻ
        
        # Comment statistics
        StructField("comments_level1", IntegerType(), True),  # Comment cấp 1 (trực tiếp)
        StructField("comments_level2", IntegerType(), True),  # Comment cấp 2 (reply)
        StructField("comments_loaded", IntegerType(), True),  # Số comment tool crawl được
        StructField("comments_displayed_tiktok", IntegerType(), True),  # Số comment thực (ground truth)
        StructField("comments_difference", IntegerType(), True),  # Chênh lệch (displayed - loaded)
        
        # Crawl metadata
        StructField("crawl_time", TimestampType(), True),  # Timestamp lúc crawl
        StructField("crawl_date", DateType(), True),  # Date extracted từ crawl_time (for partitioning)
        StructField("scrape_timestamp", StringType(), True),  # Timestamp phiên scrape (ISO8601)
        
        # Checksum for deduplication
        StructField("row_checksum", StringType(), False),  # Hash toàn bộ row (phát hiện thay đổi)
        
        # Ingestion metadata
        StructField("ingestion_timestamp", TimestampType(), False),  # Thời điểm ghi vào Lakehouse
        StructField("source_file", StringType(), False),  # Tên file gốc (truy vết)
        StructField("source_file_checksum", StringType(), False),  # Checksum file nguồn (đảm bảo toàn vẹn)
        StructField("source_file_size_bytes", LongType(), False)  # Kích thước file nguồn (bytes)
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
        StructField("post_url", StringType(), False),  # URL bài đăng (khóa ngoại tới bảng metadata)
        
        # Comment identifiers
        StructField("stt", IntegerType(), True),  # Số thứ tự comment trong video (1 to n per video)
        
        # Commenter info
        StructField("ten", StringType(), True),  # Tên hiển thị người bình luận
        StructField("tag_ten", StringType(), True),  # Username/handle TikTok người bình luận
        StructField("url", StringType(), True),  # Link tới trang cá nhân người bình luận
        
        # Comment data
        StructField("comment", StringType(), False),  # Nội dung bình luận
        StructField("comment_date", DateType(), True),  # Ngày bình luận được đăng
        StructField("likes", IntegerType(), True),  # Số lượt thích của bình luận
        
        # Reply info
        StructField("level_comment", StringType(), True),  # Cấp độ: "Yes"=level2 (reply), "No"=level1 (trực tiếp)
        StructField("replied_to_tag_name", StringType(), True),  # Username người được reply (chỉ với level2)
        StructField("number_of_replies", IntegerType(), True),  # Số reply con (nếu level1)
        
        # Scrape metadata
        StructField("scrape_timestamp", StringType(), True),  # Timestamp chuỗi lúc crawl comment
        StructField("scrape_date", DateType(), True),  # Date extracted từ scrape_timestamp (for partitioning)
        
        # Checksum for deduplication
        StructField("row_checksum", StringType(), False),  # Hash toàn bộ row (phát hiện thay đổi)
        
        # Ingestion metadata
        StructField("ingestion_timestamp", TimestampType(), False),  # Thời điểm ghi vào Lakehouse
        StructField("source_file", StringType(), False),  # Tên file gốc (truy vết)
        StructField("source_file_checksum", StringType(), False),  # Checksum file nguồn (đảm bảo toàn vẹn)
        StructField("source_file_size_bytes", LongType(), False)  # Kích thước file nguồn (bytes)
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
# Strategy: Process 30 files at a time, write once per batch to reduce Iceberg append operations
# Benefits:
#   - ~2,830 appends → ~92 appends (96.7% reduction)
#   - Less pressure on Hive Metastore
#   - Maintain file-level tracking in PostgreSQL
#   - Partition by crawl_date/scrape_date instead of post_url for better performance


# REMOVED: prepare_unprocessed_partitions() - No longer needed with new approach


def process_batches(spark, unprocessed_items, scratch_path_posts, scratch_path_comments):
    """
    PHASE 2: BATCH PROCESSING - Process 30 files per batch, write once per batch.
    
    Strategy: For each batch (30 files):
      1. Read all posts from 30 files → Union → Clean → Single APPEND
      2. Read all comments from 30 files → Union → Clean → Single APPEND
      3. Log each file individually to PostgreSQL (calculate counts from final DataFrame)
    
    Args:
        spark: SparkSession instance
        unprocessed_items: List of metadata dicts (each has post_url, checksum, etc.)
        scratch_path_posts: Scratch path for posts
        scratch_path_comments: Scratch path for comments
    
    Returns:
        Dict with statistics: posts_loaded, comments_loaded, files_success, files_failed
    """
    print(f"\n{'='*80}")
    print(f"📦 PHASE 2: BATCH PROCESSING - Processing {len(unprocessed_items)} files in batches of {BATCH_SIZE}")
    print(f"{'='*80}")
    
    # Create batches
    batches = create_batches(unprocessed_items, BATCH_SIZE)
    
    stats = {
        "posts_loaded": 0,
        "comments_loaded": 0,
        "files_success": 0,
        "files_failed": 0,
        "batches_completed": 0
    }
    
    # Process each batch
    for batch_id, batch_items in enumerate(batches, 1):
        print(f"\n{'='*80}")
        print(f"📦 BATCH {batch_id}/{len(batches)}: Processing {len(batch_items)} files")
        print(f"{'='*80}")
        
        batch_posts_count = 0
        batch_comments_count = 0
        batch_files_success = 0
        batch_files_failed = 0
        
        # Initialize DataFrames for logging
        df_posts_new = None
        df_posts_cleaned = None  # Keep for logging (to check if post_url was processed)
        df_comments_final = None
        df_comments_cleaned = None  # Keep for logging (to check if post_url was processed)
        
        # Track append success status
        posts_append_success = True  # Default: no data to append is OK
        comments_append_success = True  # Default: no data to append is OK
        
        try:
            # ============================================================
            # STEP 1: COLLECT POST_URLS FROM BATCH
            # ============================================================
            batch_post_urls = [item["post_url"] for item in batch_items]
            print(f"\n📋 Batch contains {len(batch_post_urls)} post URLs")
            
            # ============================================================
            # STEP 2: PROCESS POSTS (Read all → Union → Clean → Append once)
            # ============================================================
            print(f"\n📝 Processing POSTS for batch {batch_id}...")
            
            # Read all posts from batch (using filter for all post_urls at once)
            try:
                # Read all posts matching batch post_urls in one go (more efficient)
                df_all_posts = spark.read.parquet(scratch_path_posts) \
                    .filter(F.col("post_url").isin(batch_post_urls))
                
                # Check if we have any data
                if df_all_posts.count() > 0:
                    all_posts_data = [df_all_posts]
                else:
                    all_posts_data = []
            except Exception as e:
                print(f"   ⚠️  Error reading posts: {e}")
                all_posts_data = []
                batch_files_failed += len(batch_post_urls)
            
            if not all_posts_data:
                print(f"   ⏭️  No posts data in this batch")
            else:
                # Use the single DataFrame (already filtered)
                print(f"   📊 Processing posts data...")
                df_posts_raw = all_posts_data[0]
                
                # Clean & Transform
                print(f"   🧹 Cleaning and transforming posts...")
                df_posts_cleaned = clean_and_transform_posts(df_posts_raw)
                
                # Check duplicates with Silver (anti-join)
                print(f"   🔍 Checking duplicates with Silver table...")
                df_silver_posts = spark.table(SILVER_TABLE_POSTS).select("post_url")
                df_posts_new = df_posts_cleaned.join(
                    df_silver_posts,
                    on="post_url",
                    how="left_anti"
                )
                posts_new_count = df_posts_new.count()
                posts_skipped = df_posts_cleaned.count() - posts_new_count
                print(f"   New posts to append: {posts_new_count:,}")
                print(f"   Posts already in Silver (skipped): {posts_skipped:,}")
                
                if posts_new_count > 0:
                    # Calculate row checksum
                    print(f"   🔐 Calculating row checksum...")
                    df_posts_final = calculate_row_checksum(
                        df_posts_new,
                        BUSINESS_COLUMNS_POSTS
                    )
                    
                    # Append to Silver (1 time for entire batch) with error handling
                    print(f"   💾 Appending {posts_new_count:,} posts to Silver...")
                    try:
                        df_posts_final.writeTo(SILVER_TABLE_POSTS) \
                            .using("iceberg") \
                            .append()
                        batch_posts_count = posts_new_count
                        posts_append_success = True
                        print(f"   ✅ Posts appended successfully!")
                    except Exception as append_error:
                        print(f"   ❌ Posts append failed: {append_error}")
                        batch_posts_count = 0
                        posts_append_success = False
                        # Re-raise to be caught by outer exception handler
                        raise
                else:
                    print(f"   ⏭️  No new posts to append")
                    posts_append_success = True  # No data to append is OK
            
            # ============================================================
            # STEP 3: PROCESS COMMENTS (Read all → Union → Clean → Append once)
            # ============================================================
            print(f"\n💬 Processing COMMENTS for batch {batch_id}...")
            
            # Read all comments from batch (using filter for all post_urls at once)
            try:
                # Read all comments matching batch post_urls in one go (more efficient)
                df_all_comments = spark.read.parquet(scratch_path_comments) \
                    .filter(F.col("post_url").isin(batch_post_urls))
                
                # Check if we have any data
                if df_all_comments.count() > 0:
                    all_comments_data = [df_all_comments]
                else:
                    all_comments_data = []
            except Exception as e:
                print(f"   ⚠️  Error reading comments: {e}")
                all_comments_data = []
            
            if not all_comments_data:
                print(f"   ⏭️  No comments data in this batch")
            else:
                # Use the single DataFrame (already filtered)
                print(f"   📊 Processing comments data...")
                df_comments_raw = all_comments_data[0]
                
                # Clean & Transform
                print(f"   🧹 Cleaning and transforming comments...")
                df_comments_cleaned = clean_and_transform_comments(df_comments_raw)
                
                # Calculate row checksum
                print(f"   🔐 Calculating row checksum...")
                df_comments_final = calculate_row_checksum(
                    df_comments_cleaned,
                    BUSINESS_COLUMNS_COMMENTS
                )
                
                # Append to Silver (1 time for entire batch) with error handling
                comments_count = df_comments_final.count()
                print(f"   💾 Appending {comments_count:,} comments to Silver...")
                try:
                    df_comments_final.writeTo(SILVER_TABLE_COMMENTS) \
                        .using("iceberg") \
                        .append()
                    batch_comments_count = comments_count
                    comments_append_success = True
                    print(f"   ✅ Comments appended successfully!")
                except Exception as append_error:
                    print(f"   ❌ Comments append failed: {append_error}")
                    batch_comments_count = 0
                    comments_append_success = False
                    # Re-raise to be caught by outer exception handler
                    raise
            
            # ============================================================
            # STEP 4: LOG TO POSTGRESQL (per file, after batch append)
            # ============================================================
            print(f"\n📝 Logging batch files to PostgreSQL...")
            
            # Calculate record counts per file from final DataFrames
            for item in batch_items:
                post_url = item["post_url"]
                file_checksum = item["source_file_checksum"]
                file_name = item["source_file"]
                file_size = item["source_file_size_bytes"]
                
                try:
                    # Count posts for this file
                    # Check if post_url was PROCESSED (in cleaned data), not just appended
                    posts_for_file = 0
                    if df_posts_cleaned is not None:
                        # Check if this post_url was processed (even if deduplicated)
                        posts_check = df_posts_cleaned.filter(F.col("post_url") == post_url).count()
                        if posts_check > 0:
                            posts_for_file = 1  # Each post_url = 1 post record
                    
                    # Count comments for this file
                    # Check if post_url was PROCESSED (in cleaned data)
                    comments_for_file = 0
                    if df_comments_cleaned is not None:
                        comments_for_file = df_comments_cleaned \
                            .filter(F.col("post_url") == post_url) \
                            .count()
                    
                    total_records = posts_for_file + comments_for_file
                    
                    # Determine status (check both append success and data existence)
                    if not posts_append_success or not comments_append_success:
                        # Append operation failed
                        status = 'failed'
                        batch_files_failed += 1
                        if not posts_append_success and not comments_append_success:
                            error_msg = "Both posts and comments append operations failed"
                        elif not posts_append_success:
                            error_msg = "Posts append operation failed"
                        else:
                            error_msg = "Comments append operation failed"
                    elif posts_for_file > 0 or comments_for_file > 0:
                        # Append succeeded and data was processed (even if deduplicated)
                        status = 'success'
                        batch_files_success += 1
                        error_msg = None
                    else:
                        # Append succeeded but post_url was not found in processed data at all
                        status = 'failed'
                        batch_files_failed += 1
                        error_msg = "Post URL not found in processed data"
                    
                    # Log to PostgreSQL
                    ingestion_details = {
                        "tables": [
                            {"name": TABLE_NAME_POSTS, "status": "success" if posts_for_file > 0 else "skipped", "records": posts_for_file},
                            {"name": TABLE_NAME_COMMENTS, "status": "success" if comments_for_file > 0 else "skipped", "records": comments_for_file}
                        ],
                        "post_url": post_url,
                        "source_size_bytes": file_size,
                        "batch_id": batch_id
                    }
                    
                    log_ingestion_to_postgres(
                        file_path=f"{BRONZE_BASE_PATH}/{file_name}",
                        file_checksum=file_checksum,
                        records_ingested=total_records,
                        table_name=f"{TABLE_NAME_POSTS} + {TABLE_NAME_COMMENTS}",
                        status=status,
                        layer='silver',
                        ingestion_details=ingestion_details,
                        file_size_bytes=file_size,
                        postgres_conn_params=POSTGRES_CONN,
                        error_message=error_msg if status == 'failed' else None
                    )
                    
                except Exception as log_error:
                    print(f"   ⚠️  Failed to log file {file_name}: {log_error}")
                    batch_files_failed += 1
            
            # Update stats
            stats["posts_loaded"] += batch_posts_count
            stats["comments_loaded"] += batch_comments_count
            stats["files_success"] += batch_files_success
            stats["files_failed"] += batch_files_failed
            stats["batches_completed"] += 1
            
            print(f"\n{'='*40}")
            print(f"📊 Batch {batch_id}/{len(batches)} summary:")
            print(f"   Posts loaded: {batch_posts_count:,}")
            print(f"   Comments loaded: {batch_comments_count:,}")
            print(f"   Files success: {batch_files_success}/{len(batch_items)}")
            print(f"   Files failed: {batch_files_failed}/{len(batch_items)}")
            print(f"{'='*40}")
            
        except Exception as e:
            print(f"\n❌ ERROR in batch {batch_id}: {e}")
            import traceback
            traceback.print_exc()
            
            # Mark all files in batch as failed
            stats["files_failed"] += len(batch_items)
            
            # Log all files as failed
            for item in batch_items:
                try:
                    log_ingestion_to_postgres(
                        file_path=f"{BRONZE_BASE_PATH}/{item['source_file']}",
                        file_checksum=item["source_file_checksum"],
                        records_ingested=0,
                        table_name=f"{TABLE_NAME_POSTS} + {TABLE_NAME_COMMENTS}",
                        status='failed',
                        layer='silver',
                        error_message=str(e),
                        ingestion_details={"batch_id": batch_id, "post_url": item["post_url"]},
                        file_size_bytes=item["source_file_size_bytes"],
                        postgres_conn_params=POSTGRES_CONN
                    )
                except:
                    pass
            
            if not CONTINUE_ON_BATCH_FAILURE:
                print(f"   Stopping execution (CONTINUE_ON_BATCH_FAILURE=False)")
                break
    
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
    Main ETL: Read Scratch Parquet → Clean (Batch) → Load to Silver
    
    BATCH PROCESSING APPROACH:
    1. Phase 1: PREPARE - List partitions, build mapping, filter unprocessed
    2. Phase 2: BATCH PROCESSING - Process 30 files per batch, write once per batch
    3. Phase 3: SUMMARY - Report statistics
    
    Returns:
        tuple: (posts_loaded, comments_loaded)
    """
    print(f"🚀 STEP 2: Clean & Load (Scratch → Silver) - BATCH PROCESSING")
    print(f"   Source 1 (Posts): {SCRATCH_BASE_PATH_POSTS}")
    print(f"   Source 2 (Comments): {SCRATCH_BASE_PATH_COMMENTS}")
    print(f"   Target 1: {SILVER_TABLE_POSTS}")
    print(f"   Target 2: {SILVER_TABLE_COMMENTS}")
    print(f"   Batch size: {BATCH_SIZE} files per batch (write once per batch)")
    
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
    # Filter unprocessed (check both checksum in PostgreSQL and post_url in Silver)
    unprocessed_items = filter_unprocessed_partitions(
        spark, 
        mapping, 
        POSTGRES_CONN, 
        layer='silver',
        silver_table_posts=SILVER_TABLE_POSTS
    )
    
    if not unprocessed_items:
        print(f"\n✅ All files already processed!")
        return 0, 0
    
    # Phase 2: BATCH PROCESSING
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
