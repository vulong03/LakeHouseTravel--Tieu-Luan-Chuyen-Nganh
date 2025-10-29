"""
Silver Layer - Transform Booking Hotels Detail
Source: s3a://bronze/lakehouse/booking_hotels_detail/raw/*.csv
Target: lakehouse.silver.hotels_detail

Strategy: 
- Read from Bronze RAW CSV files (multiple part files)
- UPSERT mode (MERGE): UPDATE changed records, INSERT new records
- Row-level checksum for change detection
- Handle multi-line CSV values (descriptions with line breaks)
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
    """Create Silver table for hotels detail if not exists"""
    schema = StructType([
        # Original CSV columns
        StructField("hotel_name", StringType(), False),
        StructField("hotel_url", StringType(), False),
        StructField("province", StringType(), False),
        StructField("description", StringType(), True),
        StructField("top_amenities", StringType(), True),
        StructField("rating_score", StringType(), True),
        StructField("review_count_text", StringType(), True),
        StructField("rating_breakdown", StringType(), True),
        StructField("activities", StringType(), True),
        
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
        table_name="hotels_detail",
        schema=schema,
        partition_by=["province"],  # Province cleaned - S3-safe partition
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def validate_data(df):
    """Validate NOT NULL constraints and return clean DataFrame"""
    null_checks = {
        "hotel_name": df.filter(F.col("hotel_name").isNull()).count(),
        "hotel_url": df.filter(F.col("hotel_url").isNull()).count(),
        "province": df.filter(F.col("province").isNull()).count()
    }
    
    total_nulls = sum(null_checks.values())
    
    if total_nulls > 0:
        print(f"⚠️  Warning: Found NULL values in critical columns:")
        for col_name, null_count in null_checks.items():
            if null_count > 0:
                print(f"   - {col_name}: {null_count} NULLs")
        
        print(f"   ✅ Filtering out {total_nulls} records with NULL critical fields (Silver data quality)")
        
        # Filter out records with NULL in critical columns
        df_clean = df.filter(
            F.col("hotel_name").isNotNull() & 
            F.col("hotel_url").isNotNull() & 
            F.col("province").isNotNull()
        )
        
        clean_count = df_clean.count()
        print(f"   ✅ Remaining clean records: {clean_count}")
        return df_clean
    else:
        print(f"✅ Data validation passed - no NULL critical values")
        return df
    
    print(f"✅ Data validation passed")


def get_latest_bronze_file(spark, bronze_path_pattern):
    """
    Get the latest (most recent) file from Bronze S3 based on filename timestamp
    
    Args:
        spark: SparkSession
        bronze_path_pattern: S3 path pattern like s3a://bronze/lakehouse/booking_hotels_detail/raw/*.csv
        
    Returns:
        str: Full S3 path to the latest file, or None if no files found
    """
    import re
    from datetime import datetime
    
    try:
        # Read to get list of files
        df_temp = spark.read.text(bronze_path_pattern)
        file_paths = df_temp.select(F.input_file_name().alias("file_path")).distinct().collect()
        
        if not file_paths:
            print(f"⚠️  No files found matching pattern: {bronze_path_pattern}")
            return None
        
        # Extract timestamp from filename and find latest
        # Format: vietnam_hotels_detail_20251029_194555_c29a7c72.csv
        #                              ^^^^^^^^ ^^^^^^ 
        #                              YYYYMMDD HHMMSS
        
        latest_file = None
        latest_timestamp = None
        
        for row in file_paths:
            file_path = row.file_path
            file_name = os.path.basename(file_path)
            
            # Extract timestamp from filename (format: YYYYMMDD_HHMMSS)
            match = re.search(r'_(\d{8})_(\d{6})_', file_name)
            if match:
                date_str = match.group(1)  # YYYYMMDD
                time_str = match.group(2)  # HHMMSS
                timestamp_str = f"{date_str}{time_str}"  # YYYYMMDDHHMMSS
                
                try:
                    file_timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                    
                    if latest_timestamp is None or file_timestamp > latest_timestamp:
                        latest_timestamp = file_timestamp
                        latest_file = file_path
                except:
                    continue
        
        if latest_file:
            print(f"📁 Latest Bronze file: {os.path.basename(latest_file)}")
            print(f"   Timestamp: {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            return latest_file
        else:
            print(f"⚠️  Could not determine latest file from pattern")
            return None
            
    except Exception as e:
        print(f"❌ Error finding latest file: {e}")
        return None


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
    Bronze filename format: vietnam_hotels_detail_YYYYMMDD_HHMMSS_checksum.csv
    
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


def transform_hotels_detail(spark, bronze_base_path, verify_checksum=False):
    """
    Transform hotels detail from Bronze CSV into Silver Iceberg table using UPSERT
    
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
    
    # Read CSV from Bronze layer (single latest file) with multiLine option
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(latest_file_path)
    
    total_records = df.count()
    print(f"📝 Total records from Bronze: {total_records}")
    
    # Validate NOT NULL constraints and filter clean data
    df = validate_data(df)
    
    # CLEAN province column for S3-safe partitioning
    # Remove special characters, URL encoding, and amenities text
    print(f"\n🧹 Cleaning province column for S3-safe partition names...")
    df = df.withColumn(
        "province",
        F.regexp_replace(F.col("province"), r"[+,/\\:*?\"<>|]", " ")  # Remove S3-unsafe chars
    ).withColumn(
        "province",
        F.trim(F.col("province"))  # Trim whitespace
    )
    
    # Show cleaned province distribution
    print(f"📊 Cleaned province distribution (top 20):")
    df.groupBy("province").count().orderBy(F.desc("count")).show(20, truncate=50)
    
    # Show data quality stats
    print(f"\n📊 Data quality stats:")
    print(f"   - Hotels with rating: {df.filter(F.col('rating_score').isNotNull() & (F.col('rating_score') != '')).count()}")
    print(f"   - Hotels with reviews: {df.filter(F.col('review_count_text').isNotNull() & (F.col('review_count_text') != '')).count()}")
    print(f"   - Hotels with activities: {df.filter(F.col('activities').isNotNull() & (F.col('activities') != '')).count()}")
    
    # Add metadata columns (use actual file info from Bronze)
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", F.lit(datetime.now())) \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum))
    
    # Calculate row checksum (for change detection)
    business_columns = [
        "hotel_name", "hotel_url", "province", "description", "top_amenities",
        "rating_score", "review_count_text", "rating_breakdown", "activities"
    ]
    df_with_checksum = calculate_row_checksum(df_with_metadata, business_columns)
    
    # Show sample data
    print(f"\n📋 Sample data (first 3 rows):")
    df_with_checksum.select("hotel_name", "province", "rating_score", "review_count_text").show(3, truncate=False)
    
    # MERGE into Silver table (UPSERT mode)
    print(f"\n🔄 MERGE into Silver table (UPSERT mode)...")
    print(f"   Business Key: hotel_url")
    print(f"   Strategy: UPDATE if changed, INSERT if new, SKIP if unchanged")
    
    stats = merge_into_bronze(
        spark=spark,
        new_data_df=df_with_checksum,
        target_table="lakehouse.silver.hotels_detail",
        business_key="hotel_url",
        business_columns=business_columns
    )
    
    # Print statistics
    print_merge_stats(stats)
    
    # Log transformation to PostgreSQL tracking table
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
        table_name="silver.hotels_detail",
        status="success",
        postgres_conn_params=POSTGRES_CONN,
        layer='silver',
        ingestion_details=ingestion_details,
        file_size_bytes=file_size_bytes  # Pass file size to tracking
    )
    
    return stats['inserted'] + stats['updated']


def main():
    print("=" * 80)
    print("🔄 SILVER TRANSFORMATION - Booking Hotels Detail (UPSERT MODE)")
    print("=" * 80)
    
    spark = get_spark_session(app_name="Silver_Transform_Booking_Hotels_Detail")
    
    try:
        print("\n1️⃣  Creating Silver table schema...")
        create_silver_table(spark)
        
        print("\n2️⃣  Transforming data with UPSERT from Bronze...")
        
        # Get latest Bronze file (most recent ingestion)
        bronze_base_path = "s3a://bronze/lakehouse/booking_hotels_detail/raw"
        latest_file = get_latest_bronze_file(spark, bronze_base_path)
        
        if not latest_file:
            print("❌ No Bronze files found to process")
            sys.exit(1)
        
        record_count = transform_hotels_detail(spark, bronze_base_path)
        
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
        
        # Log failed transformation
        log_ingestion_to_postgres(
            file_path="s3a://bronze/lakehouse/booking_hotels_detail/raw/",
            file_checksum=f"hotels_detail_raw",
            records_ingested=0,
            table_name="silver.hotels_detail",
            status="failed",
            postgres_conn_params=POSTGRES_CONN,
            layer='silver',
            error_message=str(e)
        )
        
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
