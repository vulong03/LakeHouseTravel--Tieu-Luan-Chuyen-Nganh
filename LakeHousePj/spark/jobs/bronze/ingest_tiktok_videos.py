"""
Bronze Layer - Ingest TikTok Videos Metadata
Source: data/raw/tiktok/links/merged_videos.csv
Target: lakehouse.bronze.tiktok_videos_metadata

Strategy: APPEND mode with incremental loading (checksum-based deduplication)
- Detects file changes via checksum comparison
- Only ingests new/modified records (not previously seen URLs)
- Preserves historical ingestion timestamps
"""

import sys
import os
from datetime import datetime
import hashlib

# Add parent directory to path
sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


def calculate_file_checksum(file_path):
    """Calculate MD5 checksum of file"""
    hash_md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def check_if_file_ingested(spark, file_checksum):
    """Check if file already ingested by checksum"""
    
    try:
        result = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/metastore_db") \
            .option("dbtable", "file_ingestion_log") \
            .option("user", "lakehouse_user") \
            .option("password", "lakehouse_pass") \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .filter(F.col("file_checksum") == file_checksum) \
            .filter(F.col("status") == "success") \
            .count()
        
        return result > 0
    
    except Exception as e:
        print(f"⚠️ Could not check tracking log: {e}")
        return False


def get_existing_urls(spark):
    """Get set of URLs already in Bronze table"""
    
    try:
        existing_df = spark.table("lakehouse.bronze.tiktok_videos_metadata")
        existing_urls = set([row.url for row in existing_df.select("url").distinct().collect()])
        print(f"📊 Found {len(existing_urls)} existing URLs in Bronze table")
        return existing_urls
    
    except Exception as e:
        print(f"⚠️ Bronze table not found or empty: {e}")
        return set()


def create_bronze_videos_table(spark):
    """Create Bronze table for videos metadata if not exists"""
    
    schema = StructType([
        # Data columns from CSV
        StructField("url", StringType(), False),
        StructField("posted_date", StringType(), True),
        StructField("read_status", StringType(), True),
        StructField("keyword", StringType(), True),
        StructField("ques_id", StringType(), True),
        StructField("target_type", StringType(), True),
        StructField("region", StringType(), True),
        StructField("has_sub", StringType(), True),
        StructField("vi_sub", StringType(), True),
        
        # Metadata columns
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False)
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database="bronze",
        table_name="tiktok_videos_metadata",
        schema=schema,
        partition_by=["region"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def ingest_videos_metadata(spark, source_file_path):
    """
    Ingest merged_videos.csv into Bronze table
    Uses APPEND mode with deduplication to avoid duplicate URLs
    
    Args:
        spark: SparkSession
        source_file_path: Path to merged_videos.csv
    """
    
    print(f"🚀 Starting ingestion: {source_file_path}")
    
    # Check if file exists
    if not os.path.exists(source_file_path):
        raise FileNotFoundError(f"Source file not found: {source_file_path}")
    
    # Calculate checksum
    file_checksum = calculate_file_checksum(source_file_path)
    file_size = os.path.getsize(source_file_path)
    file_name = os.path.basename(source_file_path)
    
    print(f"📄 File: {file_name}")
    print(f"📊 Size: {file_size} bytes")
    print(f"🔐 Checksum: {file_checksum}")
    
    # Check if exact file already ingested (by checksum)
    if check_if_file_ingested(spark, file_checksum):
        print(f"⏭️  File already ingested (checksum: {file_checksum[:8]}...)")
        print(f"   No changes detected, skipping ingestion")
        return 0
    
    # Read CSV
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .csv(source_file_path)
    
    total_records = df.count()
    print(f"📝 Records in file: {total_records}")
    
    # Get existing URLs to avoid duplicates
    existing_urls = get_existing_urls(spark)
    
    # Filter out URLs already in Bronze table
    if existing_urls:
        df_new = df.filter(~F.col("url").isin(existing_urls))
        new_record_count = df_new.count()
        
        print(f"� New URLs to ingest: {new_record_count}")
        print(f"📊 Duplicate URLs skipped: {total_records - new_record_count}")
        
        if new_record_count == 0:
            print(f"⏭️  No new records to ingest")
            
            # Still log to tracking (file processed but no new data)
            log_ingestion_to_postgres(
                spark=spark,
                file_path=source_file_path,
                file_name=file_name,
                file_size=file_size,
                file_checksum=file_checksum,
                records_ingested=0,
                table_name="bronze.tiktok_videos_metadata",
                status="success"
            )
            return 0
    else:
        df_new = df
        new_record_count = total_records
        print(f"📊 First ingestion, processing all {new_record_count} records")
    
    # Add metadata columns
    df_with_metadata = df_new \
        .withColumn("ingestion_timestamp", F.lit(datetime.now())) \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum))
    
    # Show sample
    print("\n📋 Sample new data:")
    df_with_metadata.select("url", "keyword", "region", "read_status").show(5, truncate=False)
    
    # Write to Bronze table (APPEND mode - only new records)
    print(f"\n💾 Appending to Bronze table: bronze.tiktok_videos_metadata")
    
    df_with_metadata.writeTo("lakehouse.bronze.tiktok_videos_metadata") \
        .using("iceberg") \
        .append()
    
    print(f"✅ Successfully ingested {new_record_count} new records")
    
    # Log to PostgreSQL tracking table
    log_ingestion_to_postgres(
        spark=spark,
        file_path=source_file_path,
        file_name=file_name,
        file_size=file_size,
        file_checksum=file_checksum,
        records_ingested=new_record_count,
        table_name="bronze.tiktok_videos_metadata",
        status="success"
    )
    
    return new_record_count


def log_ingestion_to_postgres(spark, file_path, file_name, file_size, 
                               file_checksum, records_ingested, table_name, status):
    """Log ingestion to PostgreSQL tracking table"""
    
    try:
        # Create tracking record with proper types
        tracking_data = [(
            file_path,
            file_name,
            int(file_size),
            file_checksum,
            datetime.now(),  # Use datetime object directly
            int(records_ingested),
            table_name,
            status,
            None  # error_message
        )]
        
        # Define explicit schema
        from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
        
        schema = StructType([
            StructField("file_path", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("file_size_bytes", LongType(), True),
            StructField("file_checksum", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),  # Changed to TimestampType
            StructField("records_ingested", IntegerType(), True),
            StructField("table_name", StringType(), True),
            StructField("status", StringType(), False),
            StructField("error_message", StringType(), True)
        ])
        
        tracking_df = spark.createDataFrame(tracking_data, schema)
        
        # Write to PostgreSQL
        tracking_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/metastore_db") \
            .option("dbtable", "file_ingestion_log") \
            .option("user", "lakehouse_user") \
            .option("password", "lakehouse_pass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(f"📊 Logged to file_ingestion_log table")
        
    except Exception as e:
        print(f"⚠️ Failed to log to PostgreSQL: {e}")


def main():
    """Main execution"""
    
    print("=" * 80)
    print("🔄 BRONZE INGESTION - TikTok Videos Metadata")
    print("=" * 80)
    
    # Source file path (mounted at /data in container)
    source_file = "/data/raw/tiktok/links/merged_videos.csv"
    
    # Get Spark session
    spark = get_spark_session(
        app_name="Bronze_Ingest_TikTok_Videos"
    )
    
    try:
        # Create table if not exists
        print("\n1️⃣ Creating Bronze table schema...")
        create_bronze_videos_table(spark)
        
        # Ingest data
        print("\n2️⃣ Ingesting data...")
        record_count = ingest_videos_metadata(spark, source_file)
        
        print("\n" + "=" * 80)
        if record_count > 0:
            print(f"✅ COMPLETED: Ingested {record_count} new videos metadata records")
        else:
            print(f"✅ COMPLETED: No new records to ingest (file unchanged or all URLs exist)")
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
