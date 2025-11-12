"""
TikTok Videos - Step 1: Transform (Bronze → Scratch)
- Read latest Bronze CSV file
- Validate data quality
- Clean region column for S3-safe partition names
- Write to Scratch bucket as Parquet (partitioned by region)
"""

import sys
import re
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from config import (
    BRONZE_BASE_PATH, SCRATCH_BASE_PATH, NOT_NULL_COLUMNS,
    BRONZE_FILE_PATTERN, CLEANING_CONFIG, POSTGRES_CONN
)
from utils.spark_session import get_spark_session
from utils.file_tracker import check_if_file_ingested
from pyspark.sql import functions as F


# ============================================================================
# S3 FILE SIZE UTILITY
# ============================================================================

def get_s3_file_size(spark, file_path):
    """
    Get file size in bytes from S3 using Hadoop FileSystem API
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(file_path), 
            hadoop_conf
        )
        
        path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
        file_status = fs.getFileStatus(path)
        size_bytes = file_status.getLen()
        
        size_mb = size_bytes / (1024 * 1024)
        print(f"📦 File size: {size_mb:.2f} MB ({size_bytes:,} bytes)")
        
        return size_bytes
    except Exception as e:
        print(f"⚠️  Could not get file size: {e}")
        return 0


def get_latest_bronze_file(spark, bronze_base_path):
    """
    Find the latest Bronze file based on timestamp in filename
    
    Filename format: merged_videos_YYYYMMDD_HHMMSS_checksum.csv
    
    Returns:
        tuple: (file_path, checksum, file_name) or (None, None, None)
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
            match = re.search(BRONZE_FILE_PATTERN, file_name)
            
            if match:
                timestamp_str = match.group(1).replace("_", "")  # YYYYMMDDHHMMSS
                checksum = match.group(2)
                files_with_timestamp.append((file_path, timestamp_str, checksum, file_name))
        
        if not files_with_timestamp:
            print(f"❌ No files matching pattern {BRONZE_FILE_PATTERN}")
            return None, None, None
        
        # Sort by timestamp (descending) and get latest
        files_with_timestamp.sort(key=lambda x: x[1], reverse=True)
        latest_file_path, latest_timestamp, file_checksum, file_name = files_with_timestamp[0]
        
        print(f"🔍 Latest file: {file_name} (timestamp: {latest_timestamp})")
        print(f"📄 Checksum from filename: {file_checksum}")
        
        return latest_file_path, file_checksum, file_name
        
    except Exception as e:
        print(f"❌ Error finding latest Bronze file: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None


def validate_data(df):
    """
    Validate NOT NULL constraints
    
    Returns:
        tuple: (clean_df, null_count)
    """
    null_checks = {col: df.filter(F.col(col).isNull()).count() 
                   for col in NOT_NULL_COLUMNS}
    
    total_nulls = sum(null_checks.values())
    
    if total_nulls > 0:
        print(f"⚠️  Warning: Found {total_nulls} NULL values in critical columns")
        for col, count in null_checks.items():
            if count > 0:
                print(f"   - {col}: {count} NULLs")
        
        print(f"   ✅ Filtering out records with NULL in critical columns")
        
        # Filter out records with NULL in critical columns
        for col in NOT_NULL_COLUMNS:
            df = df.filter(F.col(col).isNotNull())
        
        return df, total_nulls
    
    print(f"✅ Data validation passed - no NULL critical values")
    return df, 0


def clean_region_column(df):
    """
    Clean region column to remove S3-unsafe characters
    
    S3-unsafe characters: +, /, \, :, *, ?, ", <, >, |
    
    Returns:
        DataFrame with cleaned region column
    """
    # Remove S3-unsafe characters
    df = df.withColumn(
        "region",
        F.regexp_replace(F.col("region"), r'[+/\\:*?"<>|]', '')
    )
    
    return df


def main():
    print("=" * 80)
    print("🔄 SILVER TIKTOK VIDEOS - STEP 1: TRANSFORM (Bronze → Scratch)")
    print("=" * 80)
    
    spark = get_spark_session(app_name="Silver_TikTok_Videos_Step1_Transform")
    
    try:
        print(f"🚀 Starting transformation: Bronze → Scratch")
        print(f"   Source: {BRONZE_BASE_PATH}")
        print(f"   Target: {SCRATCH_BASE_PATH}")
        
        # Get latest Bronze file
        latest_file_path, file_checksum, file_name = get_latest_bronze_file(spark, BRONZE_BASE_PATH)
        
        if not latest_file_path:
            print("❌ No Bronze files found")
            sys.exit(1)
        
        print(f"\n📄 Processing: {file_name}")
        print(f"   Checksum: {file_checksum}")
        
        # Check if already processed in Silver layer
        print(f"\n🔍 Checking tracking database...")
        if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
            print(f"   ⏭️  Already processed in Silver layer")
            print(f"      Checksum: {file_checksum}")
            print(f"      Skipping transformation")
            print(f"\n{'=' * 80}")
            print(f"⏭️  STEP 1 SKIPPED: File already processed")
            print(f"{'=' * 80}")
            return
        print(f"   ✓ New file, proceeding with transformation")
        
        # Get file size
        file_size_bytes = get_s3_file_size(spark, latest_file_path)
        
        # Read CSV from Bronze
        print(f"\n📖 Reading CSV from Bronze...")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("encoding", "UTF-8") \
            .csv(latest_file_path)
        
        total_records = df.count()
        print(f"📝 Total records from Bronze: {total_records:,}")
        
        # Validate data (keep all columns at this stage)
        df_clean, null_count = validate_data(df)
        
        if null_count > 0:
            clean_count = df_clean.count()
            print(f"   Records after cleaning: {clean_count:,} (removed {null_count})")
            df = df_clean
        
        # Clean region column for S3-safe partition names
        print(f"\n🧹 Cleaning region column for S3-safe partition names...")
        df = clean_region_column(df)
        
        # Add metadata columns (for tracking in Step 2)
        print(f"\n🏷️  Adding metadata columns...")
        df = df \
            .withColumn("source_file", F.lit(file_name)) \
            .withColumn("source_file_checksum", F.lit(file_checksum)) \
            .withColumn("source_file_size_bytes", F.lit(file_size_bytes)) \
            .withColumn("extraction_timestamp", F.lit(datetime.now()))
        print(f"   ✓ Added 4 metadata columns")
        
        # Show statistics
        print(f"\n📊 Region distribution:")
        df.groupBy("region").count().orderBy(F.desc("count")).show(20, truncate=False)
        
        print(f"\n📊 Top keywords:")
        df.groupBy("keyword").count().orderBy(F.desc("count")).show(10, truncate=False)
        
        print(f"\n📊 Read status distribution:")
        df.groupBy("read_status").count().show(truncate=False)
        
        # Sample data
        print(f"\n📋 Sample data (first 3 rows):")
        df.select("url", "keyword", "region", "read_status", "posted_date").show(3, truncate=False)
        
        # Generate run ID
        run_id = datetime.now().strftime("run_%Y%m%d_%H%M%S")
        output_path = f"{SCRATCH_BASE_PATH}/{run_id}"
        
        # Write to Scratch bucket
        print(f"\n💾 Writing to Scratch bucket...")
        print(f"   Path: {output_path}")
        print(f"   Format: Parquet (Snappy compression)")
        print(f"   Partitioned by: region")
        
        df.write \
            .mode("overwrite") \
            .partitionBy("region") \
            .parquet(output_path)
        
        final_count = df.count()
        
        print(f"\n✅ Transform completed!")
        print(f"   Records written: {final_count:,}")
        print(f"   Output: {output_path}")
        
        print("\n" + "=" * 80)
        print(f"✅ STEP 1 COMPLETED: {final_count:,} records transformed to Scratch")
        print("=" * 80)
        print(f"\n📂 Output location: {output_path}")
        print(f"\n▶️  Next: Run Step 2 (Clean & Load to Silver)")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
