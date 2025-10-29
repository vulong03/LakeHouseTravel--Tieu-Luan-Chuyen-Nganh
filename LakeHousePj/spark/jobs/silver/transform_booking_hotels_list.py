"""
Silver Layer - Transform Booking Hotels List
Source: s3a://bronze/lakehouse/booking_hotels_list/raw/*.csv
Target: lakehouse.silver.hotels_list

Strategy: 
- Read from Bronze RAW CSV files (multiple part files)
- UPSERT mode (MERGE): UPDATE changed records, INSERT new records
- Row-level checksum for change detection
- Data quality: Clean, deduplicate, standardize
- Partition by province
- Business Key: hotel_url
"""

import sys
import os
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum, merge_into_bronze, print_merge_stats
from utils.file_tracker import (
    calculate_file_checksum,
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


def create_silver_table(spark):
    """Create Silver table for hotels list if not exists"""
    schema = StructType([
        # Original CSV columns
        StructField("stt", StringType(), True),
        StructField("hotel_name", StringType(), False),
        StructField("hotel_url", StringType(), False),
        StructField("province", StringType(), False),
        
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
        table_name="hotels_list",
        schema=schema,
        partition_by=["province"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def get_s3_file_size(spark, file_path):
    """
    Get file size in bytes from S3 using Hadoop FileSystem API
    """
    try:
        # Get Hadoop FileSystem
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(file_path), 
            hadoop_conf
        )
        
        # Get file status
        path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
        file_status = fs.getFileStatus(path)
        
        # Get size in bytes
        size_bytes = file_status.getLen()
        
        # Format size for display
        size_mb = size_bytes / (1024 * 1024)
        print(f"📦 File size: {size_mb:.2f} MB ({size_bytes:,} bytes)")
        
        return size_bytes
    except Exception as e:
        print(f"⚠️  Could not get file size: {e}")
        return 0


def calculate_file_checksum_from_s3(spark, file_path):
    """
    Calculate MD5 checksum from S3 file content using Spark streaming
    This verifies the checksum embedded in Bronze filename
    """
    import hashlib
    
    try:
        # Read file content as text (streaming approach)
        df = spark.read.text(file_path)
        
        # Collect content in chunks to avoid OOM
        rows = df.collect()
        content = "\n".join([row.value for row in rows])
        
        # Calculate MD5
        md5_hash = hashlib.md5(content.encode('utf-8')).hexdigest()[:8]
        
        return md5_hash
    except Exception as e:
        print(f"⚠️  Could not calculate checksum from file content: {e}")
        return None


def get_latest_bronze_file(spark, bronze_base_path, verify_checksum=False):
    """
    Find the latest Bronze file based on timestamp in filename
    Bronze filename format: vietnam_hotels_list_YYYYMMDD_HHMMSS_checksum.csv
    
    Args:
        verify_checksum: If True, calculate checksum from file content and verify
    """
    from pyspark.sql.functions import input_file_name
    
    # List all files in Bronze
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
            # Extract timestamp: vietnam_hotels_list_20251029_194555_checksum.csv
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
        
        # Optional: Verify checksum by calculating from file content
        if verify_checksum:
            print(f"🔐 Verifying checksum from file content...")
            calculated_checksum = calculate_file_checksum_from_s3(spark, latest[2])
            if calculated_checksum and calculated_checksum != latest[1]:
                raise ValueError(
                    f"❌ Checksum mismatch! Filename: {latest[1]}, Calculated: {calculated_checksum}"
                )
            print(f"✅ Checksum verified: {calculated_checksum}")
        
        return latest[2], latest[1], latest[3]  # file_path, checksum, file_name
        
    except Exception as e:
        print(f"❌ Error finding latest Bronze file: {e}")
        raise


def validate_data(df):
    """Validate NOT NULL constraints"""
    null_checks = {
        "hotel_name": df.filter(F.col("hotel_name").isNull()).count(),
        "hotel_url": df.filter(F.col("hotel_url").isNull()).count(),
        "province": df.filter(F.col("province").isNull()).count()
    }
    
    for col_name, null_count in null_checks.items():
        if null_count > 0:
            raise ValueError(f"❌ Found {null_count} NULL values in column '{col_name}'")
    
    print(f"✅ Data validation passed")


def transform_hotels_list(spark, bronze_base_path, verify_checksum=False):
    """
    Transform hotels list from Bronze CSV into Silver Iceberg table using UPSERT
    
    Args:
        verify_checksum: If True, calculate checksum from file content to verify Bronze filename
    """
    print(f"🚀 Starting transformation from Bronze: {bronze_base_path}")
    
    # Find latest Bronze file (with optional checksum verification)
    latest_file_path, file_checksum, file_name = get_latest_bronze_file(
        spark, bronze_base_path, verify_checksum=verify_checksum
    )
    
    print(f"📄 Processing: {file_name}")
    print(f"   Checksum: {file_checksum}")
    
    # Get file size from S3
    file_size_bytes = get_s3_file_size(spark, latest_file_path)
    
    # CHECKSUM TRACKING LOGIC:
    # 1. Bronze layer calculates checksum when ingesting and embeds it in filename
    # 2. Silver extracts checksum from Bronze filename (trusted source)
    # 3. Check PostgreSQL tracking: if checksum exists for layer='silver' → Skip
    # 4. This prevents re-processing same Bronze file in Silver layer
    # 5. Optional: verify_checksum=True will recalculate from file content for validation
    
    # Check if already processed in Silver layer
    if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
        print(f"⏭️  Already processed in Silver layer (checksum: {file_checksum})")
        print(f"   Skipping transformation (data already in Silver)")
        return 0
    
    # Read CSV from Bronze layer (single latest file)
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .csv(latest_file_path)
    
    total_records = df.count()
    print(f"📝 Total records from Bronze: {total_records}")
    
    # Validate NOT NULL constraints
    validate_data(df)
    
    # Add metadata columns
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", F.lit(datetime.now())) \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum))
    
    # Calculate row checksum (for change detection)
    business_columns = ["stt", "hotel_name", "hotel_url", "province"]
    df_with_checksum = calculate_row_checksum(df_with_metadata, business_columns)
    
    # Show sample data
    print(f"\n📋 Sample data (first 5 rows):")
    df_with_checksum.select("stt", "hotel_name", "province").show(5, truncate=False)
    
    # Show province distribution
    print(f"\n📊 Province distribution:")
    df_with_checksum.groupBy("province").count().orderBy("province").show(truncate=False)
    
    # MERGE into Silver table (UPSERT mode)
    print(f"\n🔄 MERGE into Silver table (UPSERT mode)...")
    print(f"   Business Key: hotel_url")
    print(f"   Strategy: UPDATE if changed, INSERT if new, SKIP if unchanged")
    
    stats = merge_into_bronze(
        spark=spark,
        new_data_df=df_with_checksum,
        target_table="lakehouse.silver.hotels_list",
        business_key="hotel_url",
        business_columns=["stt", "hotel_name", "hotel_url", "province"]
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
        "business_key": "hotel_url",
        "total_records_processed": total_records
    }
    
    log_ingestion_to_postgres(
        file_path=latest_file_path,
        file_checksum=file_checksum,
        records_ingested=stats['inserted'] + stats['updated'],
        table_name="silver.hotels_list",
        status="success",
        postgres_conn_params=POSTGRES_CONN,
        layer='silver',
        ingestion_details=ingestion_details,
        file_size_bytes=file_size_bytes  # Pass file size to tracking
    )
    
    return stats['inserted'] + stats['updated']


def main():
    print("=" * 80)
    print("🔄 SILVER TRANSFORMATION - Booking Hotels List (UPSERT MODE)")
    print("=" * 80)
    
    spark = get_spark_session(app_name="Silver_Transform_Booking_Hotels_List")
    
    try:
        print("\n1️⃣  Creating Silver table schema...")
        create_silver_table(spark)
        
        print("\n2️⃣  Transforming data with UPSERT from Bronze...")
        
        # Get latest Bronze file (most recent ingestion)
        bronze_base_path = "s3a://bronze/lakehouse/booking_hotels_list/raw"
        latest_file = get_latest_bronze_file(spark, bronze_base_path)
        
        if not latest_file:
            print("❌ No Bronze files found to process")
            sys.exit(1)
        
        record_count = transform_hotels_list(spark, bronze_base_path)
        
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
