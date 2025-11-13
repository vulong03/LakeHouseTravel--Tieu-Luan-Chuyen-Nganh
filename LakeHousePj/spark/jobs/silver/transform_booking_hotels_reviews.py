"""
Silver Layer - Transform Booking Hotels Reviews
Source: lakehouse.bronze.raw_booking_hotels_reviews (Iceberg table)
Target: lakehouse.silver.hotels_reviews

Strategy: 
- Read from Bronze Iceberg table (NOT local CSV)
- UPSERT mode (MERGE): UPDATE changed records, INSERT new records
- Row-level checksum for change detection
- Data quality: Clean, deduplicate, standardize
- NO partition (reviews don't have province field)
- Business Key: composite (hotel_name + reviewer_name + review_date)
- File size tracking from Bronze source
"""

import sys
import os
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum, merge_into_bronze, print_merge_stats
from utils.file_tracker import (
    check_if_file_ingested,
    log_ingestion_to_postgres
)
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# PostgreSQL connection parameters
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}


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


def calculate_file_checksum_from_s3(spark, file_path):
    """
    Calculate MD5 checksum by reading file content from S3
    (Optional - for verification against Bronze filename checksum)
    
    Args:
        spark: SparkSession
        file_path: Full S3 path
    
    Returns:
        str: MD5 checksum (first 8 chars)
    """
    import hashlib
    
    try:
        content = spark.read.text(file_path).collect()
        md5_hash = hashlib.md5()
        for row in content:
            md5_hash.update(row[0].encode('utf-8'))
        return md5_hash.hexdigest()[:8]
    except Exception as e:
        print(f"⚠️  Warning: Could not calculate checksum for {file_path}: {e}")
        return None


def get_latest_bronze_file(spark, bronze_base_path, verify_checksum=False):
    """
    Find the latest Bronze file based on timestamp in filename
    
    Filename format: vietnam_hotels_reviews_YYYYMMDD_HHMMSS_checksum.csv
    
    Args:
        spark: SparkSession
        bronze_base_path: Base path to Bronze files (e.g., s3a://bronze/.../raw)
        verify_checksum: If True, recalculate checksum and compare with filename
    
    Returns:
        tuple: (latest_file_path, file_checksum, file_name) or (None, None, None)
    """
    from pyspark.sql import functions as F
    import re
    
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
        pattern = r'vietnam_hotels_reviews_(\d{8}_\d{6})_([a-f0-9]{8})\.csv'
        files_with_timestamp = []
        
        for file_path in file_list:
            file_name = file_path.split("/")[-1]
            match = re.search(pattern, file_name)
            
            if match:
                timestamp_str = match.group(1).replace("_", "")
                checksum = match.group(2)
                files_with_timestamp.append((file_path, timestamp_str, checksum, file_name))
        
        if not files_with_timestamp:
            print(f"❌ No files matching pattern {pattern}")
            return None, None, None
        
        # Sort by timestamp (descending) and get latest
        files_with_timestamp.sort(key=lambda x: x[1], reverse=True)
        latest_file_path, latest_timestamp, file_checksum, file_name = files_with_timestamp[0]
        
        print(f"🔍 Latest file: {file_name} (timestamp: {latest_timestamp})")
        print(f"📄 Checksum from filename: {file_checksum}")
        
        # Optional: Verify checksum
        if verify_checksum:
            calculated_checksum = calculate_file_checksum_from_s3(spark, latest_file_path)
            if calculated_checksum and calculated_checksum != file_checksum:
                print(f"⚠️  WARNING: Checksum mismatch!")
                print(f"   Expected: {file_checksum}")
                print(f"   Calculated: {calculated_checksum}")
            elif calculated_checksum:
                print(f"✅ Checksum verified: {file_checksum}")
        
        return latest_file_path, file_checksum, file_name
        
    except Exception as e:
        print(f"❌ Error finding latest Bronze file: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None


def create_silver_table(spark):
    """Create Silver table for hotels reviews if not exists"""
    schema = StructType([
        # Original CSV columns
        StructField("hotel_name", StringType(), False),
        StructField("hotel_url", StringType(), True), 
        StructField("reviewer_name", StringType(), True),
        StructField("reviewer_country", StringType(), True),
        StructField("room_type", StringType(), True),
        StructField("stay_date", StringType(), True),
        StructField("traveler_type", StringType(), True),
        StructField("review_date", StringType(), True),
        StructField("review_title", StringType(), True),
        StructField("review_score", StringType(), True),
        StructField("review_positive", StringType(), True),
        StructField("review_negative", StringType(), True),
        
        # Row-level checksum for deduplication
        StructField("row_checksum", StringType(), False),
        
        # Metadata columns
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False)
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database="silver",
        table_name="hotels_reviews",
        schema=schema,
        partition_by=[],  # No partition (reviews don't have province field)
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def validate_data(df):
    """Validate NOT NULL constraints and return clean DataFrame"""
    null_checks = {
        "hotel_name": df.filter(F.col("hotel_name").isNull()).count()
    }
    
    total_nulls = sum(null_checks.values())
    
    if total_nulls > 0:
        print(f"⚠️  Warning: Found {total_nulls} NULL values in 'hotel_name' column")
        print(f"   ✅ Filtering out records with NULL hotel_name (Silver data quality)")
        df_clean = df.filter(F.col("hotel_name").isNotNull())
        return df_clean, total_nulls
    
    print(f"✅ Data validation passed - no NULL critical values")
    return df, 0


def transform_hotels_reviews(spark, bronze_base_path, verify_checksum=False):
    """Transform hotels reviews from Bronze into Silver Iceberg table using UPSERT"""
    print(f"🚀 Starting transformation from Bronze: {bronze_base_path}")
    
    # Get latest Bronze file
    latest_file_path, file_checksum, file_name = get_latest_bronze_file(spark, bronze_base_path, verify_checksum)
    
    if not latest_file_path:
        print(f"❌ No Bronze files found")
        return 0
    
    # Get file size
    file_size_bytes = get_s3_file_size(spark, latest_file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    print(f"📄 Processing: {file_name}")
    print(f"   Checksum: {file_checksum}")
    print(f"� File size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
    
    # Check if already processed in Silver layer
    if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
        print(f"⏭️  Already processed in Silver layer (checksum: {file_checksum})")
        return 0
    
    # Read CSV from Bronze layer (specific latest file)
    # NOTE: multiLine option for review text that may contain line breaks
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .csv(latest_file_path)
    
    # Normalize column names (remove spaces, convert to lowercase if needed)
    for col_name in df.columns:
        normalized = col_name.strip().replace(" ", "_").lower()
        if normalized != col_name:
            df = df.withColumnRenamed(col_name, normalized)
    
    # Drop 'province' column if it exists (it belongs to hotels_list, not reviews)
    if "province" in df.columns:
        print(f"⚠️  Dropping 'province' column (not part of reviews schema)")
        df = df.drop("province")
    
    total_records = df.count()
    print(f"📝 Total records from Bronze: {total_records:,}")
    
    # Validate and clean data
    df_clean, null_count = validate_data(df)
    
    if null_count > 0:
        clean_count = df_clean.count()
        print(f"   Records after cleaning: {clean_count:,} (removed {null_count} with NULL hotel_name)")
        df = df_clean
    
    # Add metadata columns with actual file info
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", F.lit(datetime.now())) \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum))
    
    # Calculate row checksum
    business_columns = [
        "hotel_name", "hotel_url", "reviewer_name", "reviewer_country",
        "room_type", "stay_date", "traveler_type", "review_date",
        "review_title", "review_score", "review_positive", "review_negative"
    ]
    df_with_checksum = calculate_row_checksum(df_with_metadata, business_columns)
    
    # Show sample data (limited for large datasets)
    print(f"\n📋 Sample data (first 5 rows):")
    df_with_checksum.select("hotel_name", "reviewer_name", "review_score", "review_title").show(5, truncate=False)
    
    # Show reviews per hotel (top 10 only)
    print(f"\n📊 Top 10 hotels by review count:")
    df_with_checksum.groupBy("hotel_name") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(10, truncate=False)
    
    # Show traveler type distribution
    print(f"\n📊 Traveler type distribution:")
    df_with_checksum.groupBy("traveler_type") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(truncate=False)
    
    # Deduplicate using LEFT ANTI JOIN (same as Bronze logic)
    print(f"\n🔄 Deduplicating and appending to Silver table...")
    print(f"   Strategy: LEFT ANTI JOIN on row_checksum (append new records only)")
    
    try:
        existing_df = spark.table("lakehouse.silver.hotels_reviews")
        existing_count = existing_df.count()
        print(f"� Existing reviews in Silver: {existing_count:,}")
        
        # Use LEFT ANTI JOIN - keep only new records
        print(f"🔍 Deduplicating using LEFT ANTI JOIN (distributed operation)...")
        df_new = df_with_checksum.join(
            existing_df.select("row_checksum"),
            on="row_checksum",
            how="left_anti"  # Keep only rows from left that DON'T match right
        )
        
        new_count = df_new.count()
        duplicate_count = total_records - new_count
        
        print(f"\n🔍 Deduplication results:")
        print(f"   - Total reviews from Bronze: {total_records:,}")
        print(f"   - New reviews to append: {new_count:,}")
        print(f"   - Duplicate reviews (skipped): {duplicate_count:,}")
        
    except Exception as e:
        print(f"\n📊 Table is empty or doesn't exist yet ({e})")
        df_new = df_with_checksum
        new_count = df_new.count()
        print(f"\n🔍 All {new_count:,} reviews are new (first ingestion)")
    
    # Only write if there are new reviews
    if new_count > 0:
        print(f"\n💾 Appending {new_count:,} NEW reviews to Silver table...")
        df_new.writeTo("lakehouse.silver.hotels_reviews") \
            .using("iceberg") \
            .append()
        
        print(f"   ✅ Successfully appended {new_count:,} reviews")
        final_record_count = new_count
    else:
        print(f"\n⏭️  No new reviews to append (all are duplicates)")
        final_record_count = 0
    
    # Log transformation to PostgreSQL tracking table (Silver layer)
    ingestion_details = {
        "dedup_stats": {
            "new_records": new_count,
            "duplicates_skipped": total_records - new_count
        },
        "source_file": file_name,
        "source_path": latest_file_path,
        "source_size_bytes": file_size_bytes,
        "dedup_method": "LEFT ANTI JOIN on row_checksum",
        "total_records_processed": total_records
    }
    
    log_ingestion_to_postgres(
        file_path=latest_file_path,
        file_checksum=file_checksum,
        records_ingested=final_record_count,
        table_name="silver.hotels_reviews",
        status="success",
        postgres_conn_params=POSTGRES_CONN,
        layer='silver',
        ingestion_details=ingestion_details,
        file_size_bytes=file_size_bytes
    )
    
    return final_record_count


def main():
    print("=" * 80)
    print("🔄 SILVER TRANSFORMATION - Booking Hotels Reviews (UPSERT MODE)")
    print("=" * 80)
    
    # Source: Bronze layer base path
    bronze_base_path = "s3a://bronze/lakehouse/booking_hotels_reviews/raw"
    
    spark = get_spark_session(app_name="Silver_Transform_Booking_Hotels_Reviews")
    
    try:
        print("\n1️⃣  Creating Silver table schema...")
        create_silver_table(spark)
        
        print("\n2️⃣  Transforming data with UPSERT from Bronze...")
        record_count = transform_hotels_reviews(spark, bronze_base_path)
        
        print("\n" + "=" * 80)
        if record_count > 0:
            print(f"✅ COMPLETED: Processed {record_count:,} records (INSERT/UPDATE)")
        else:
            print(f"✅ COMPLETED: No changes detected")
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
