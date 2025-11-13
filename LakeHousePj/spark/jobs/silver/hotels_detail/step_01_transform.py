"""
Silver Layer - Hotels Detail - Step 1: Transform
Transform Bronze CSV files to Parquet in Scratch bucket

Based on original transform_booking_hotels_detail.py:
- Handle multi-line CSV values (descriptions with line breaks)
- Clean province column for S3-safe partitioning
- Validate NOT NULL constraints
- Add metadata columns
- Save to Scratch bucket for Step 2 processing
"""

import sys
import os
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.file_tracker import check_if_file_ingested
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Import config
from silver.hotels_detail.config import (
    BRONZE_BASE_PATH,
    SCRATCH_BASE_PATH,
    NOT_NULL_COLUMNS,
    POSTGRES_CONN
)


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
    Bronze filename format: vietnam_hotels_detail_YYYYMMDD_HHMMSS_checksum.csv
    
    PRESERVED FROM ORIGINAL LOGIC
    """
    from pyspark.sql.functions import input_file_name
    
    try:
        df_files = spark.read.text(f"{bronze_base_path}/*.csv")
        file_paths = df_files.select(input_file_name().alias("file_path")).distinct().collect()
        
        if not file_paths:
            raise Exception(f"No Bronze files found in {bronze_base_path}")
        
        # Extract timestamp and sort
        files_with_ts = []
        for row in file_paths:
            file_path = row.file_path
            file_name = file_path.split("/")[-1]
            # Extract timestamp: vietnam_hotels_detail_20251029_194555_checksum.csv
            parts = file_name.replace('.csv', '').split('_')
            if len(parts) >= 4:
                timestamp = parts[-3] + parts[-2]  # YYYYMMDD + HHMMSS
                checksum = parts[-1]
                files_with_ts.append((timestamp, checksum, file_path, file_name))
        
        # Sort by timestamp descending (newest first)
        files_with_ts.sort(reverse=True, key=lambda x: x[0])
        
        latest = files_with_ts[0]
        print(f"📂 Found {len(files_with_ts)} Bronze files")
        print(f"🔍 Latest file: {latest[3]} (timestamp: {latest[0]})")
        print(f"📄 Checksum from filename: {latest[1]}")
        
        return latest[2], latest[1], latest[3]  # file_path, checksum, file_name
        
    except Exception as e:
        print(f"❌ Error finding latest Bronze file: {e}")
        raise


def validate_data(df):
    """
    Validate NOT NULL constraints and return clean DataFrame
    PRESERVED FROM ORIGINAL LOGIC
    """
    null_checks = {}
    for col_name in NOT_NULL_COLUMNS:
        null_checks[col_name] = df.filter(F.col(col_name).isNull()).count()
    
    total_nulls = sum(null_checks.values())
    
    if total_nulls > 0:
        print(f"⚠️  Warning: Found NULL values in critical columns:")
        for col_name, null_count in null_checks.items():
            if null_count > 0:
                print(f"   - {col_name}: {null_count} NULLs")
        
        print(f"   ✅ Filtering out {total_nulls} records with NULL critical fields (Silver data quality)")
        
        # Build filter condition
        filter_conditions = [F.col(col).isNotNull() for col in NOT_NULL_COLUMNS]
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = combined_filter & condition
        
        df_clean = df.filter(combined_filter)
        
        clean_count = df_clean.count()
        print(f"   ✅ Remaining clean records: {clean_count}")
        return df_clean
    else:
        print(f"✅ Data validation passed - no NULL critical values")
        return df


def clean_province_column(df):
    """
    Clean province column for S3-safe partitioning
    Remove special characters, URL encoding, and amenities text
    
    PRESERVED FROM ORIGINAL LOGIC
    """
    print(f"\n🧹 Cleaning province column for S3-safe partition names...")
    
    df_cleaned = df.withColumn(
        "province",
        F.regexp_replace(F.col("province"), r"[+,/\\:*?\"<>|]", " ")  # Remove S3-unsafe chars
    ).withColumn(
        "province",
        F.trim(F.col("province"))  # Trim whitespace
    )
    
    return df_cleaned


def transform_to_scratch(spark):
    """
    Transform Bronze CSV to Parquet in Scratch bucket
    Based on original transform logic
    """
    print(f"🚀 Starting transformation: Bronze → Scratch")
    print(f"   Source: {BRONZE_BASE_PATH}")
    print(f"   Target: {SCRATCH_BASE_PATH}")
    
    # Find latest Bronze file
    latest_file_path, file_checksum, file_name = get_latest_bronze_file(spark, BRONZE_BASE_PATH)
    
    print(f"\n📄 Processing: {file_name}")
    print(f"   Checksum: {file_checksum}")
    
    # Check if already processed in Silver layer
    print(f"\n🔍 Checking tracking database...")
    if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
        print(f"   ⏭️  Already processed in Silver layer")
        print(f"      Checksum: {file_checksum}")
        print(f"      Skipping transformation")
        return 0
    print(f"   ✓ New file, proceeding with transformation")
    
    # Get file size
    file_size_bytes = get_s3_file_size(spark, latest_file_path)
    
    # Read CSV from Bronze with multiLine option (PRESERVED FROM ORIGINAL)
    print(f"\n📖 Reading CSV from Bronze (with multiLine support for descriptions)...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(latest_file_path)
    
    total_records = df.count()
    print(f"📝 Total records from Bronze: {total_records:,}")
    
    # Validate NOT NULL constraints
    df = validate_data(df)
    
    # Clean province column
    df = clean_province_column(df)
    
    # Show cleaned province distribution
    print(f"\n📊 Cleaned province distribution (top 20):")
    df.groupBy("province").count().orderBy(F.desc("count")).show(20, truncate=50)
    
    # Show data quality stats (PRESERVED FROM ORIGINAL)
    print(f"\n📊 Data quality stats:")
    print(f"   - Hotels with rating: {df.filter(F.col('rating_score').isNotNull() & (F.col('rating_score') != '')).count():,}")
    print(f"   - Hotels with reviews: {df.filter(F.col('review_count_text').isNotNull() & (F.col('review_count_text') != '')).count():,}")
    print(f"   - Hotels with activities: {df.filter(F.col('activities').isNotNull() & (F.col('activities') != '')).count():,}")
    
    # Add metadata columns (for tracking in Step 2)
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", F.lit(datetime.now())) \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum)) \
        .withColumn("source_file_size_bytes", F.lit(file_size_bytes))
    
    # Show sample data
    print(f"\n📋 Sample data (first 3 rows):")
    df_with_metadata.select("hotel_name", "province", "rating_score", "review_count_text").show(3, truncate=False)
    
    # Generate unique run ID
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{SCRATCH_BASE_PATH}/run_{run_id}"
    
    # Write to Scratch bucket (Parquet format, partitioned by province)
    print(f"\n💾 Writing to Scratch bucket...")
    print(f"   Path: {output_path}")
    print(f"   Format: Parquet (Snappy compression)")
    print(f"   Partitioned by: province")
    
    df_with_metadata.write \
        .mode("overwrite") \
        .partitionBy("province") \
        .parquet(output_path)
    
    final_count = df_with_metadata.count()
    
    print(f"\n✅ Transform completed!")
    print(f"   Records written: {final_count:,}")
    print(f"   Output: {output_path}")
    
    return output_path, final_count


def main():
    print("=" * 80)
    print("🔄 SILVER HOTELS DETAIL - STEP 1: TRANSFORM (Bronze → Scratch)")
    print("=" * 80)
    
    spark = None
    
    try:
        spark = get_spark_session(app_name="Silver_Hotels_Detail_Step1_Transform")
        
        result = transform_to_scratch(spark)
        
        # Check if file was skipped (already processed)
        if result == 0:
            print("\n" + "=" * 80)
            print("⏭️  STEP 1 SKIPPED: File already processed in Silver layer")
            print("=" * 80)
            return
        
        output_path, record_count = result
        
        print("\n" + "=" * 80)
        print(f"✅ STEP 1 COMPLETED: {record_count:,} records transformed to Scratch")
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
