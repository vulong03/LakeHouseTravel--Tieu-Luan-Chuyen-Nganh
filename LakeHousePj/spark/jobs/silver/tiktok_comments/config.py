"""
Configuration for tiktok_comments Silver Layer Pipeline

Pipeline: tiktok_comments
Source: Bronze CSV files (s3a://bronze/lakehouse/tiktok_comments/raw/*.csv)
Target: 2 Silver Iceberg tables:
  1. silver.tiktok_post_metadata (post info from lines 1-17)
  2. silver.tiktok_post_comments (comment data from lines 18+)

Strategy: 2-Step Pattern with Direct APPEND
  - Step 1 (Transform): Bronze CSV → Scratch Parquet (parse header + CSV, preserve all data)
  - Step 2 (Clean & Load): Scratch Parquet → Silver Iceberg (cleaning, type conversion, APPEND)

Deduplication Strategy: OPTION A (Keep Latest File per URL)
  - When same post_url is crawled multiple times → Keep ONLY the LATEST file (by checksum/timestamp)
  - Skip older crawls to avoid duplicate comments in Silver
  - Assumes newer crawl contains most up-to-date data (all old comments + any new ones)
  - Result: Each post_url appears exactly ONCE in Silver → No duplicate comments

Partition: 
  - tiktok_post_metadata: Partition by crawl_date (date-based partitioning for better performance)
  - tiktok_post_comments: Partition by scrape_date (date-based partitioning for better performance)
"""

# Database and table names
SILVER_DATABASE = "silver"
TABLE_NAME_POSTS = "tiktok_post_metadata"
TABLE_NAME_COMMENTS = "tiktok_post_comments"
SILVER_TABLE_POSTS = f"silver.{SILVER_DATABASE}.{TABLE_NAME_POSTS}"
SILVER_TABLE_COMMENTS = f"silver.{SILVER_DATABASE}.{TABLE_NAME_COMMENTS}"

# Bronze source path
BRONZE_BASE_PATH = "s3a://bronze/lakehouse/tiktok_comments/raw"

# Scratch bucket for intermediate data (Parquet files, NOT Iceberg)
SCRATCH_BASE_PATH_POSTS = "s3a://scratch/pipeline/silver/tiktok_post_metadata"
SCRATCH_BASE_PATH_COMMENTS = "s3a://scratch/pipeline/silver/tiktok_post_comments"

# Bronze filename pattern
# Format: tiktok_comments_[optimized_]YYYY-MM-DDTHH-MM-SS_YYYYMMDD_HHMMSS_checksum.csv
# Example 1: tiktok_comments_2025-09-27T10-40-30_20251030_081316_0c64460f.csv
# Example 2: tiktok_comments_optimized_2025-09-27T10-40-30_20251030_081316_0c64460f.csv
BRONZE_FILE_PATTERN = r'tiktok_comments_(?:optimized_)?(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})_\d{8}_\d{6}_[a-f0-9]{8}\.csv'

# Business columns for row_checksum calculation

# Post metadata: Only post_url (primary key)
BUSINESS_COLUMNS_POSTS = [
    "post_url"
]

# Comment data: post_url + comment identifiers
BUSINESS_COLUMNS_COMMENTS = [
    "post_url",
    "stt",
    "ten",
    "comment",
    "comment_date"  # Changed from "time" to "comment_date" (parsed column name)
]

# Partition columns
PARTITION_COLUMNS_POSTS = ["crawl_date"]  # Partition by crawl_date (date-based partitioning)
PARTITION_COLUMNS_COMMENTS = ["scrape_date"]  # Partition by scrape_date (date-based partitioning)

# NOT NULL constraints (for data validation)
NOT_NULL_COLUMNS_POSTS = ["post_url"]
NOT_NULL_COLUMNS_COMMENTS = ["post_url", "comment"]

# PostgreSQL connection parameters
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}

# Batch processing configuration
BATCH_SIZE = 30  # Process 30 partitions per batch (optimized for Hive Metastore memory)

# Performance tuning
ENABLE_BATCH_LOGGING = True  # Log each file individually within batches
MAX_RETRIES_PER_BATCH = 2  # Retry failed batches this many times
CONTINUE_ON_BATCH_FAILURE = True  # Continue to next batch if one fails
