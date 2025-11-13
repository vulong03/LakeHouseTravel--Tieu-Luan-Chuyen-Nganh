"""
Step 1: Transform - Hotels Reviews (Bronze → Scratch)

Purpose: Read from Bronze CSV, preserve ALL data with minimal transformation
Strategy:
  - Find latest Bronze CSV file by timestamp
  - Check if file already processed (via PostgreSQL tracking)
  - Read with multiLine=true (review text may contain line breaks)
  - Normalize column names
  - Drop 'province' column if exists (belongs to hotels_list)
  - Filter NULL hotel_name (data quality)
  - Add metadata: source_file, source_file_checksum, source_file_size_bytes
  - Write to Scratch bucket as Parquet (NO partition - avoid skew)
  - Log to PostgreSQL tracking table
  - NO deduplication, NO data cleaning (preserve Bronze as-is)

Input: Bronze CSV files (s3a://bronze/.../raw/vietnam_hotels_reviews_*.csv)
Output: Scratch Parquet files (s3a://scratch/pipeline/silver/hotels_reviews/run_YYYYMMDD_HHMMSS/)
"""

import sys
import os
import re

sys.path.append('/opt/spark/jobs')

from datetime import datetime
from pyspark.sql import functions as F

from utils.spark_session import get_spark_session
from silver.hotels_reviews.config import (
    SCRATCH_BASE_PATH,
    BRONZE_BASE_PATH,
    BRONZE_FILE_PATTERN,
    PARTITION_COLUMNS,
    NOT_NULL_COLUMNS,
    POSTGRES_CONN
)
from utils.file_tracker import check_if_file_ingested


def get_s3_file_size(spark, file_path):
    """
    Get file size from S3/HDFS using Hadoop FileSystem API
    
    Args:
        spark: SparkSession
        file_path: Full S3 path (e.g., s3a://bronze/.../file.csv)
    
    Returns:
        int: File size in bytes
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs_uri = spark._jvm.java.net.URI(file_path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(fs_uri, hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
        file_status = fs.getFileStatus(path)
        size_bytes = file_status.getLen()
        return size_bytes
    except Exception as e:
        print(f"⚠️  Warning: Could not get file size for {file_path}: {e}")
        return 0


def get_latest_bronze_file(spark, bronze_base_path, file_pattern):
    """
    Find the latest Bronze file based on timestamp in filename
    
    Filename format: vietnam_hotels_reviews_YYYYMMDD_HHMMSS_checksum.csv
    
    Args:
        spark: SparkSession
        bronze_base_path: Base path to Bronze files
        file_pattern: Regex pattern to parse filename
    
    Returns:
        tuple: (file_path, file_checksum, file_name) or (None, None, None)
    """
    try:
        files_df = spark.read.format("binaryFile") \
            .load(f"{bronze_base_path}/*.csv") \
            .select("path")
        
        file_list = [row.path for row in files_df.collect()]
        
        if not file_list:
            print(f"❌ No Bronze files found in {bronze_base_path}")
            return None, None, None
        
        print(f"📂 Found {len(file_list)} Bronze files")
        
        # Parse timestamps from filenames
        files_with_timestamp = []
        
        for file_path in file_list:
            file_name = file_path.split("/")[-1]
            match = re.search(file_pattern, file_name)
            
            if match:
                timestamp_str = match.group(1).replace("_", "")
                checksum = match.group(2)
                files_with_timestamp.append((file_path, timestamp_str, checksum, file_name))
        
        if not files_with_timestamp:
            print(f"❌ No files matching pattern {file_pattern}")
            return None, None, None
        
        # Sort by timestamp (descending) and get latest
        files_with_timestamp.sort(key=lambda x: x[1], reverse=True)
        latest_file_path, latest_timestamp, file_checksum, file_name = files_with_timestamp[0]
        
        print(f"🔍 Latest file: {file_name}")
        print(f"   Timestamp: {latest_timestamp}")
        print(f"   Checksum: {file_checksum}")
        
        return latest_file_path, file_checksum, file_name
        
    except Exception as e:
        print(f"❌ Error finding latest Bronze file: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None





def validate_data(df):
    """Validate NOT NULL constraints and return clean DataFrame"""
    null_checks = {}
    for col_name in NOT_NULL_COLUMNS:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_checks[col_name] = null_count
    
    total_nulls = sum(null_checks.values())
    
    if total_nulls > 0:
        print(f"⚠️  Warning: Found {total_nulls} NULL values in critical columns")
        for col_name, count in null_checks.items():
            if count > 0:
                print(f"   - {col_name}: {count} NULLs")
        
        print(f"   ✅ Filtering out records with NULL critical values")
        df_clean = df
        for col_name in NOT_NULL_COLUMNS:
            df_clean = df_clean.filter(F.col(col_name).isNotNull())
        
        return df_clean, total_nulls
    
    print(f"✅ Data validation passed - no NULL critical values")
    return df, 0


def transform_bronze_to_scratch(spark):
    """
    Transform: Read latest Bronze CSV → Write to Scratch Parquet
    
    Preserves all data from Bronze with minimal transformation:
    - Column normalization
    - Drop 'province' if exists
    - Filter NULL hotel_name
    - Add metadata columns
    """
    print(f"🚀 STEP 1: Transform Bronze → Scratch")
    print(f"   Source: {BRONZE_BASE_PATH}")
    print(f"   Target: {SCRATCH_BASE_PATH}")
    
    # Find latest Bronze file
    latest_file_path, file_checksum, file_name = get_latest_bronze_file(
        spark, BRONZE_BASE_PATH, BRONZE_FILE_PATTERN
    )
    
    if not latest_file_path:
        print(f"❌ No Bronze files found")
        return 0
    
    # Get file size
    file_size_bytes = get_s3_file_size(spark, latest_file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    print(f"\n📄 Processing file:")
    print(f"   Name: {file_name}")
    print(f"   Checksum: {file_checksum}")
    print(f"   Size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
    
    # Check if already processed in Silver layer
    if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
        print(f"\n⏭️  File already processed in Silver layer (checksum: {file_checksum})")
        print(f"   Skipping transformation...")
        return None, 0  # Return tuple: (no output_path, 0 records)
    
    # Read CSV from Bronze layer
    # IMPORTANT: multiLine=true for review text that may contain line breaks
    print(f"\n📖 Reading CSV from Bronze (multiLine mode)...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .csv(latest_file_path)
    
    # Normalize column names (remove spaces, convert to lowercase)
    print(f"🔧 Normalizing column names...")
    for col_name in df.columns:
        normalized = col_name.strip().replace(" ", "_").lower()
        if normalized != col_name:
            df = df.withColumnRenamed(col_name, normalized)
    
    # Drop 'province' column if exists (belongs to hotels_list, not reviews)
    if "province" in df.columns:
        print(f"⚠️  Dropping 'province' column (not part of reviews schema)")
        df = df.drop("province")
    
    total_records = df.count()
    print(f"\n📊 Total records from Bronze: {total_records:,}")
    
    # Validate and clean data
    df_clean, null_count = validate_data(df)
    
    if null_count > 0:
        clean_count = df_clean.count()
        print(f"   Records after validation: {clean_count:,} (removed {null_count})")
        df = df_clean
    
    # Add metadata columns
    print(f"\n🏷️  Adding metadata columns...")
    df_with_metadata = df \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum)) \
        .withColumn("source_file_size_bytes", F.lit(file_size_bytes)) \
        .withColumn("ingestion_timestamp", F.lit(datetime.now()))
    
    # Show sample data
    print(f"\n📋 Sample data (first 5 rows):")
    df_with_metadata.select(
        "hotel_name", "reviewer_name", "review_score", "review_title"
    ).show(5, truncate=False)
    
    # Show distribution stats
    print(f"\n📊 Data distribution:")
    print(f"   Top 10 hotels by review count:")
    df_with_metadata.groupBy("hotel_name") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(10, truncate=False)
    
    print(f"\n   Traveler type distribution:")
    df_with_metadata.groupBy("traveler_type") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(truncate=False)
    
    # Generate unique run ID
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{SCRATCH_BASE_PATH}/run_{run_id}"
    
    # Write to Scratch bucket (Parquet format, NO partition to avoid skew)
    print(f"\n💾 Writing to Scratch bucket...")
    print(f"   Path: {output_path}")
    print(f"   Format: Parquet (Snappy compression)")
    print(f"   Partitioning: None (flat files - avoid partition skew with large hotels)")
    
    df_with_metadata.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    final_count = df_with_metadata.count()
    print(f"\n✅ Transform completed!")
    print(f"   Records written: {final_count:,}")
    print(f"   Output: {output_path}")
    
    return output_path, final_count


def main():
    print("=" * 80)
    print("🔄 SILVER HOTELS REVIEWS - STEP 1: TRANSFORM (Bronze → Scratch)")
    print("=" * 80)
    
    spark = None
    
    try:
        spark = get_spark_session(app_name="Silver_Hotels_Reviews_Step1_Transform")
        
        output_path, record_count = transform_bronze_to_scratch(spark)
        
        print("\n" + "=" * 80)
        if record_count > 0:
            print(f"✅ STEP 1 COMPLETED: {record_count:,} records transformed to Scratch")
        else:
            print(f"✅ STEP 1 COMPLETED: No new data to process")
        print("=" * 80)
        print(f"\n📂 Output location: {output_path}")
        print(f"\n▶️  Next: Run Step 2 (Clean & Load to Silver)")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
