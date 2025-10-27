"""
Bronze Layer - Ingest TikTok Videos Metadata
Source: data/raw/tiktok/links/merged_videos.csv
Target: lakehouse.bronze.raw_tiktok_video_links

Strategy: APPEND mode with incremental loading (checksum-based deduplication)
- Detects file changes via checksum comparison
- Only ingests new/modified records (not previously seen URLs)
- Preserves historical ingestion timestamps
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
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


def get_existing_urls(spark):
    """Get set of URLs already in Bronze table (excluding deleted ones)"""
    
    try:
        existing_df = spark.table("lakehouse.bronze.raw_tiktok_video_links") \
            .filter((F.col("is_deleted").isNull()) | (F.col("is_deleted") == "false"))
        existing_urls = set([row.url for row in existing_df.select("url").distinct().collect()])
        print(f"📊 Found {len(existing_urls)} existing URLs in Bronze table (excluding deleted)")
        return existing_urls
    
    except Exception as e:
        print(f"⚠️ Bronze table not found or empty: {e}")
        return set()


def get_all_existing_urls_with_metadata(spark):
    """Get all existing URLs with their metadata for comparison"""
    
    try:
        existing_df = spark.table("lakehouse.bronze.raw_tiktok_video_links") \
            .filter((F.col("is_deleted").isNull()) | (F.col("is_deleted") == "false"))
        
        # Return as dict: {url: {posted_date, read_status, ...}}
        existing_data = {}
        for row in existing_df.collect():
            existing_data[row.url] = {
                'posted_date': row.posted_date,
                'read_status': row.read_status,
                'keyword': row.keyword,
                'ques_id': row.ques_id,
                'target_type': row.target_type,
                'region': row.region,
                'has_sub': row.has_sub,
                'vi_sub': row.vi_sub
            }
        
        print(f"📊 Loaded {len(existing_data)} existing URLs with metadata")
        return existing_data
    
    except Exception as e:
        print(f"⚠️ Could not load existing metadata: {e}")
        return {}


def mark_deleted_urls(spark, deleted_urls):
    """Mark URLs as deleted (soft delete)"""
    
    if not deleted_urls:
        return
    
    try:
        print(f"\n🗑️  Marking {len(deleted_urls)} URLs as deleted...")
        
        # Create temp view with deleted URLs
        deleted_df = spark.createDataFrame(
            [(url,) for url in deleted_urls],
            ["url"]
        )
        deleted_df.createOrReplaceTempView("deleted_urls_temp")
        
        # Use Iceberg MERGE to update deleted flag
        spark.sql("""
            MERGE INTO lakehouse.bronze.raw_tiktok_video_links AS target
            USING deleted_urls_temp AS source
            ON target.url = source.url
            WHEN MATCHED THEN UPDATE SET
                target.is_deleted = 'true',
                target.deleted_at = current_timestamp(),
                target.last_updated_at = current_timestamp()
        """)
        
        print(f"   ✅ Marked {len(deleted_urls)} URLs as deleted")
        
    except Exception as e:
        print(f"   ⚠️ Failed to mark deleted URLs: {e}")
        print(f"   Note: MERGE may not be supported in this Iceberg version")


def update_existing_urls(spark, updated_records):
    """Update metadata for existing URLs that have changed"""
    
    if not updated_records:
        return
    
    try:
        print(f"\n🔄 Updating {len(updated_records)} URLs with changed metadata...")
        
        # Create DataFrame from updated records
        updated_df = spark.createDataFrame(updated_records)
        updated_df.createOrReplaceTempView("updated_urls_temp")
        
        # Use Iceberg MERGE to update metadata
        spark.sql("""
            MERGE INTO lakehouse.bronze.raw_tiktok_video_links AS target
            USING updated_urls_temp AS source
            ON target.url = source.url
            WHEN MATCHED THEN UPDATE SET
                target.posted_date = source.posted_date,
                target.read_status = source.read_status,
                target.keyword = source.keyword,
                target.ques_id = source.ques_id,
                target.target_type = source.target_type,
                target.region = source.region,
                target.has_sub = source.has_sub,
                target.vi_sub = source.vi_sub,
                target.source_file = source.source_file,
                target.source_file_checksum = source.source_file_checksum,
                target.last_updated_at = current_timestamp()
        """)
        
        print(f"   ✅ Updated {len(updated_records)} URLs")
        
    except Exception as e:
        print(f"   ⚠️ Failed to update URLs: {e}")
        print(f"   Note: MERGE may not be supported in this Iceberg version")


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
        StructField("source_file_checksum", StringType(), False),
        
        # Soft delete columns
        StructField("is_deleted", StringType(), True),  # 'true'/'false' as string for compatibility
        StructField("deleted_at", TimestampType(), True),
        StructField("last_updated_at", TimestampType(), True)
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database="bronze",
        table_name="raw_tiktok_video_links",
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
    Features:
    - APPEND mode for new URLs
    - UPDATE mode for changed metadata
    - SOFT DELETE for removed URLs
    
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
    if check_if_file_ingested(file_checksum, POSTGRES_CONN):
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
    
    # Get current URLs from file
    current_urls = set([row.url for row in df.select("url").collect()])
    
    # Get existing URLs and metadata from Bronze table
    existing_urls = get_existing_urls(spark)
    existing_metadata = get_all_existing_urls_with_metadata(spark)
    
    # === 1. DETECT DELETED URLs ===
    deleted_urls = existing_urls - current_urls
    if deleted_urls:
        print(f"\n🗑️  Detected {len(deleted_urls)} deleted URLs")
        print(f"   Sample deleted URLs: {list(deleted_urls)[:3]}")
        mark_deleted_urls(spark, deleted_urls)
    else:
        print(f"\n✅ No deleted URLs detected")
    
    # === 2. DETECT NEW URLs ===
    new_urls = current_urls - existing_urls
    df_new = df.filter(F.col("url").isin(new_urls)) if new_urls else None
    
    if df_new and df_new.count() > 0:
        new_record_count = df_new.count()
        print(f"\n➕ Detected {new_record_count} new URLs")
        
        # Add metadata columns
        df_new_with_metadata = df_new \
            .withColumn("ingestion_timestamp", F.lit(datetime.now())) \
            .withColumn("source_file", F.lit(file_name)) \
            .withColumn("source_file_checksum", F.lit(file_checksum)) \
            .withColumn("is_deleted", F.lit("false")) \
            .withColumn("deleted_at", F.lit(None).cast(TimestampType())) \
            .withColumn("last_updated_at", F.lit(datetime.now()))
        
        # Show sample
        print(f"\n📋 Sample new data:")
        df_new_with_metadata.select("url", "keyword", "region", "read_status").show(5, truncate=False)
        
        # Write to Bronze table (APPEND mode)
        print(f"\n💾 Appending {new_record_count} new URLs to Bronze table...")
        df_new_with_metadata.writeTo("lakehouse.bronze.raw_tiktok_video_links") \
            .using("iceberg") \
            .append()
        
        print(f"   ✅ Successfully ingested {new_record_count} new records")
    else:
        new_record_count = 0
        print(f"\n✅ No new URLs to ingest")
    
    # === 3. DETECT UPDATED URLs (metadata changes) ===
    common_urls = current_urls & existing_urls
    updated_records = []
    
    if common_urls:
        print(f"\n🔍 Checking {len(common_urls)} existing URLs for metadata changes...")
        
        for row in df.filter(F.col("url").isin(common_urls)).collect():
            url = row.url
            existing = existing_metadata.get(url, {})
            
            # Check if any metadata field has changed
            has_changes = (
                row.posted_date != existing.get('posted_date') or
                row.read_status != existing.get('read_status') or
                row.keyword != existing.get('keyword') or
                row.ques_id != existing.get('ques_id') or
                row.target_type != existing.get('target_type') or
                row.region != existing.get('region') or
                row.has_sub != existing.get('has_sub') or
                row.vi_sub != existing.get('vi_sub')
            )
            
            if has_changes:
                updated_records.append({
                    'url': url,
                    'posted_date': row.posted_date,
                    'read_status': row.read_status,
                    'keyword': row.keyword,
                    'ques_id': row.ques_id,
                    'target_type': row.target_type,
                    'region': row.region,
                    'has_sub': row.has_sub,
                    'vi_sub': row.vi_sub,
                    'source_file': file_name,
                    'source_file_checksum': file_checksum
                })
        
        if updated_records:
            print(f"\n🔄 Detected {len(updated_records)} URLs with metadata changes")
            print(f"   Sample updated URLs: {[r['url'] for r in updated_records[:3]]}")
            update_existing_urls(spark, updated_records)
        else:
            print(f"\n✅ No metadata changes detected for existing URLs")
    
    # === 4. SUMMARY ===
    total_changes = new_record_count + len(updated_records) + len(deleted_urls)
    
    print(f"\n" + "=" * 80)
    print(f"📊 INGESTION SUMMARY:")
    print(f"   ➕ New URLs added: {new_record_count}")
    print(f"   🔄 URLs updated: {len(updated_records)}")
    print(f"   🗑️  URLs deleted (soft): {len(deleted_urls)}")
    print(f"   📝 Total changes: {total_changes}")
    print("=" * 80)
    
    # Log to PostgreSQL tracking table
    log_ingestion_to_postgres(
        file_path=source_file_path,
        file_checksum=file_checksum,
        records_ingested=total_changes,
        table_name="bronze.raw_tiktok_video_links",
        status="success",
        postgres_conn_params=POSTGRES_CONN
    )
    
    return total_changes


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
