"""
Silver Layer - Transform TikTok Videos
Source: s3a://bronze/lakehouse/tiktok_videos/raw/*.csv
Target: lakehouse.silver.tiktok_videos

Strategy: 
- Read from Bronze RAW CSV files (single timestamped file)
- UPSERT mode (MERGE): UPDATE changed records, INSERT new records
- Row-level checksum for change detection
- Data quality: Clean, deduplicate, standardize
- Partition by region
- Business Key: url
"""

import sys
import os
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum, merge_into_bronze, print_merge_stats
from utils.file_tracker import (
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
        # Use Hadoop FileSystem API via Spark's JVM bridge
        hadoop_conf = spark._jsc.hadoopConfiguration()
        
        # Get FileSystem instance
        fs_uri = spark._jvm.java.net.URI(file_path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            fs_uri,
            hadoop_conf
        )
        
        # Get file status and size
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
        # Read file content
        content = spark.read.text(file_path).collect()
        
        # Calculate MD5
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
    
    Filename format: merged_videos_YYYYMMDD_HHMMSS_checksum.csv
    
    Args:
        spark: SparkSession
        bronze_base_path: Base path to Bronze files (e.g., s3a://bronze/.../raw)
        verify_checksum: If True, recalculate checksum and compare with filename
    
    Returns:
        tuple: (latest_file_path, file_checksum, file_name) or (None, None, None)
    """
    from pyspark.sql import functions as F
    import re
    
    # List all CSV files in Bronze
    try:
        files_df = spark.read.format("binaryFile") \
            .load(f"{bronze_base_path}/*.csv") \
            .select("path")
        
        file_list = [row.path for row in files_df.collect()]
        
        if not file_list:
            print(f"❌ No Bronze files found in {bronze_base_path}")
            return None, None, None
        
        print(f"📂 Found {len(file_list)} Bronze files")
        
        # Parse timestamps from filenames - FIXED pattern for merged_videos
        pattern = r'merged_videos_(\d{8}_\d{6})_([a-f0-9]{8})\.csv'
        files_with_timestamp = []
        
        for file_path in file_list:
            file_name = file_path.split("/")[-1]
            match = re.search(pattern, file_name)
            
            if match:
                timestamp_str = match.group(1).replace("_", "")  # YYYYMMDDHHMMSS
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
    """Create Silver table for TikTok videos if not exists"""
    schema = StructType([
        # Original CSV columns
        StructField("url", StringType(), False),
        StructField("posted_date", StringType(), True),
        StructField("read_status", StringType(), True),
        StructField("keyword", StringType(), True),
        StructField("ques_id", StringType(), True),
        StructField("target_type", StringType(), True),
        StructField("region", StringType(), True),
        StructField("has_sub", StringType(), True),
        StructField("vi_sub", StringType(), True),
        
        # Row checksum for change detection
        StructField("row_checksum", StringType(), False),
        
        # Metadata columns
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False)
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database="silver",
        table_name="tiktok_videos",
        schema=schema,
        partition_by=["region"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def validate_data(df):
    """Validate NOT NULL constraints and return clean DataFrame"""
    null_checks = {
        "url": df.filter(F.col("url").isNull()).count()
    }
    
    total_nulls = sum(null_checks.values())
    
    if total_nulls > 0:
        print(f"⚠️  Warning: Found {total_nulls} NULL values in 'url' column")
        print(f"   ✅ Filtering out records with NULL URLs (Silver data quality)")
        df_clean = df.filter(F.col("url").isNotNull())
        return df_clean, total_nulls
    
    print(f"✅ Data validation passed - no NULL critical values")
    return df, 0


def transform_tiktok_videos(spark, bronze_base_path, verify_checksum=False):
    """Transform TikTok videos from Bronze CSV into Silver Iceberg table using UPSERT"""
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
    print(f"📦 File size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
    
    # Check if already processed in Silver layer
    from utils.file_tracker import check_if_file_ingested
    if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
        print(f"⏭️  Already processed in Silver layer (checksum: {file_checksum})")
        return 0
    
    # Read CSV from Bronze layer (specific latest file)
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .csv(latest_file_path)
    
    total_records = df.count()
    print(f"📝 Total records from Bronze: {total_records}")
    
    # Validate and clean data
    df_clean, null_count = validate_data(df)
    
    if null_count > 0:
        clean_count = df_clean.count()
        print(f"   Records after cleaning: {clean_count} (removed {null_count} with NULL URLs)")
        df = df_clean
    
    # Add metadata columns with actual file info
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", F.lit(datetime.now())) \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum))
    
    # Calculate row checksum
    business_columns = [
        "url", "posted_date", "read_status", "keyword", "ques_id",
        "target_type", "region", "has_sub", "vi_sub"
    ]
    df_with_checksum = calculate_row_checksum(df_with_metadata, business_columns)
    
    # Show sample data
    print(f"\n📋 Sample data (first 5 rows):")
    df_with_checksum.select("url", "keyword", "region", "read_status").show(5, truncate=False)
    
    # Show region distribution
    print(f"\n� Region distribution:")
    df_with_checksum.groupBy("region").count().orderBy(F.desc("count")).show(20, truncate=False)
    
    # Show keyword distribution
    print(f"\n� Top keywords:")
    df_with_checksum.groupBy("keyword").count().orderBy(F.desc("count")).show(10, truncate=False)
    
    # MERGE into Silver table (UPSERT mode)
    print(f"\n🔄 MERGE into Silver table (UPSERT mode)...")
    print(f"   Business Key: url")
    print(f"   Strategy: UPDATE if changed, INSERT if new, SKIP if unchanged")
    
    stats = merge_into_bronze(
        spark=spark,
        new_data_df=df_with_checksum,
        target_table="lakehouse.silver.tiktok_videos",
        business_key="url",
        business_columns=business_columns
    )
    
    # Print statistics
    print_merge_stats(stats)
    
    # Log transformation to PostgreSQL tracking table (Silver layer)
    ingestion_details = {
        "merge_stats": {
            "inserted": stats['inserted'],
            "updated": stats['updated'],
            "skipped": stats['skipped']
        },
        "source_file": file_name,
        "source_path": latest_file_path,
        "source_size_bytes": file_size_bytes,
        "business_key": "url",
        "total_records_processed": total_records
    }
    
    log_ingestion_to_postgres(
        file_path=latest_file_path,
        file_checksum=file_checksum,
        records_ingested=stats['inserted'] + stats['updated'],
        table_name="silver.tiktok_videos",
        status="success",
        postgres_conn_params=POSTGRES_CONN,
        layer='silver',
        ingestion_details=ingestion_details,
        file_size_bytes=file_size_bytes
    )
    
    return stats['inserted'] + stats['updated']


def main():
    print("=" * 80)
    print("🔄 SILVER TRANSFORMATION - TikTok Videos (UPSERT MODE)")
    print("=" * 80)
    
    # Source: Bronze layer base path
    bronze_base_path = "s3a://bronze/lakehouse/tiktok_videos/raw"
    
    spark = get_spark_session(app_name="Silver_Transform_TikTok_Videos")
    
    try:
        print("\n1️⃣  Creating Silver table schema...")
        create_silver_table(spark)
        
        print("\n2️⃣  Transforming data with UPSERT from Bronze...")
        
        # Get latest Bronze file first
        latest_file = get_latest_bronze_file(spark, bronze_base_path)
        if not latest_file[0]:
            print("❌ No Bronze files found")
            sys.exit(1)
        
        record_count = transform_tiktok_videos(spark, bronze_base_path)
        
        print("\n" + "=" * 80)
        if record_count > 0:
            print(f"✅ COMPLETED: Processed {record_count} records (INSERT/UPDATE)")
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
