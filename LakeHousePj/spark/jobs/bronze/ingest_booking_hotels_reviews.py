"""
Bronze Layer - Ingest Booking Hotels Reviews
Source: /data/raw/booking/vietnam_hotels_reviews.csv
Target: lakehouse.bronze.raw_booking_hotels_reviews

Strategy: 
- Load RAW data AS-IS (no transformation)
- File-level checksum tracking
- Row-level checksum for deduplication
- MERGE/UPSERT mode (single file can be updated)
- NO partition (reviews data structure doesn't have province)
"""

import sys
import os
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum
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


def create_bronze_table(spark):
    """Create Bronze table for hotels reviews if not exists"""
    schema = StructType([
        # Original CSV columns
        StructField("hotel_name", StringType(), False),
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
        database="bronze",
        table_name="raw_booking_hotels_reviews",
        schema=schema,
        partition_by=[],  # No partition
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def validate_data(df):
    """Validate and filter NULL hotel_name records"""
    null_count = df.filter(F.col("hotel_name").isNull()).count()
    
    if null_count > 0:
        print(f"⚠️  Warning: Found {null_count} NULL values in column 'hotel_name'")
        print(f"   These records will be filtered out (Bronze layer accepts raw data)")
    else:
        print(f"✅ Data validation passed - no NULL hotel_name values")
    
    return null_count


def ingest_hotels_reviews(spark, source_file_path):
    """Ingest hotels reviews CSV into Bronze table"""
    print(f"🚀 Starting ingestion: {source_file_path}")
    
    if not os.path.exists(source_file_path):
        raise FileNotFoundError(f"Source file not found: {source_file_path}")
    
    file_checksum = calculate_file_checksum(source_file_path)
    file_size = os.path.getsize(source_file_path)
    file_name = os.path.basename(source_file_path)
    
    print(f"📄 File: {file_name}")
    print(f"📊 Size: {file_size:,} bytes")
    print(f"🔐 Checksum: {file_checksum}")
    
    # Check if file already processed
    if check_if_file_ingested(file_checksum, POSTGRES_CONN):
        print(f"⏭️  File already ingested (checksum: {file_checksum[:8]}...)")
        return 0
    
    # Read CSV with all columns as String (raw)
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .csv(source_file_path)
    
    total_records = df.count()
    print(f"📝 Total reviews in file: {total_records}")
    
    # Validate and get NULL count
    null_count = validate_data(df)
    
    # Filter out NULL hotel_name records
    if null_count > 0:
        df = df.filter(F.col("hotel_name").isNotNull())
        valid_records = df.count()
        print(f"   ➡️  Keeping {valid_records} valid reviews (filtered out {null_count} NULL records)")
    
    # ============================================================================
    # ⚠️ COMMENTED OUT - Too expensive for large datasets (1.4M rows)
    # These operations cause multiple full scans and can freeze Spark workers
    # Uncomment only for small test datasets
    # ============================================================================
    # # Show data quality stats
    # print(f"\n📊 Data quality stats:")
    # print(f"   - Reviews with positive text: {df.filter(F.col('review_positive').isNotNull() & (F.col('review_positive') != '')).count()}")
    # print(f"   - Reviews with negative text: {df.filter(F.col('review_negative').isNotNull() & (F.col('review_negative') != '')).count()}")
    # print(f"   - Reviews with score: {df.filter(F.col('review_score').isNotNull() & (F.col('review_score') != '')).count()}")
    
    # Add metadata columns
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", F.lit(datetime.now())) \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum))
    
    # # Show sample data
    # print(f"\n📋 Sample data (first 3 rows):")
    # df_with_metadata.select("hotel_name", "reviewer_name", "reviewer_country", "review_score", "review_title").show(3, truncate=False)
    # 
    # # Show reviews per hotel
    # print(f"\n📊 Top 10 hotels by review count:")
    # df_with_metadata.groupBy("hotel_name") \
    #     .count() \
    #     .orderBy(F.desc("count")) \
    #     .show(10, truncate=False)
    # 
    # # Show traveler type distribution
    # print(f"\n📊 Traveler type distribution:")
    # df_with_metadata.groupBy("traveler_type") \
    #     .count() \
    #     .orderBy(F.desc("count")) \
    #     .show(truncate=False)
    # ============================================================================
    
    # Calculate row checksum BEFORE adding metadata
    business_columns = [
        "hotel_name", "reviewer_name", "reviewer_country",
        "room_type", "stay_date", "traveler_type", "review_date",
        "review_title", "review_score", "review_positive", "review_negative"
    ]
    
    df_with_checksum = calculate_row_checksum(df_with_metadata, business_columns)
    
    # Get existing checksums for deduplication using LEFT ANTI JOIN (much faster than .isin())
    try:
        existing_df = spark.table("lakehouse.bronze.raw_booking_hotels_reviews")
        existing_count = existing_df.count()
        print(f"\n📊 Existing reviews in Bronze: {existing_count}")
        
        # Use LEFT ANTI JOIN instead of .collect() + .isin() for better performance
        print(f"🔍 Deduplicating using LEFT ANTI JOIN (distributed operation)...")
        df_new = df_with_checksum.join(
            existing_df.select("row_checksum"),
            on="row_checksum",
            how="left_anti"  # Keep only rows from left that DON'T match right
        )
        
        new_count = df_new.count()
        total_count = df_with_checksum.count()
        duplicate_count = total_count - new_count
        
        print(f"\n🔍 Deduplication results:")
        print(f"   - Total reviews in file: {total_count}")
        print(f"   - New reviews to ingest: {new_count}")
        print(f"   - Duplicate reviews (skipped): {duplicate_count}")
        
    except Exception as e:
        print(f"\n📊 Table is empty or doesn't exist yet ({e})")
        df_new = df_with_checksum
        new_count = df_new.count()
        print(f"\n🔍 All {new_count} reviews are new (first ingestion)")
    
    # Only write if there are new reviews
    if new_count > 0:
        print(f"\n💾 Appending {new_count} NEW reviews to Bronze table...")
        df_new.writeTo("lakehouse.bronze.raw_booking_hotels_reviews") \
            .using("iceberg") \
            .append()
        
        print(f"   ✅ Successfully ingested {new_count} new reviews")
        final_record_count = new_count
    else:
        print(f"\n⏭️  No new reviews to ingest (all are duplicates)")
        final_record_count = 0
    
    # Log to PostgreSQL tracking table
    log_ingestion_to_postgres(
        file_path=source_file_path,
        file_checksum=file_checksum,
        records_ingested=final_record_count,
        table_name="bronze.raw_booking_hotels_reviews",
        status="success",
        postgres_conn_params=POSTGRES_CONN
    )
    
    return final_record_count


def main():
    print("=" * 80)
    print("🔄 BRONZE INGESTION - Booking Hotels Reviews")
    print("=" * 80)
    
    source_file = "/data/raw/booking/vietnam_hotels_reviews.csv"
    
    spark = get_spark_session(app_name="Bronze_Ingest_Booking_Hotels_Reviews")
    
    try:
        print("\n1️⃣  Creating Bronze table schema...")
        create_bronze_table(spark)
        
        print("\n2️⃣  Ingesting data...")
        record_count = ingest_hotels_reviews(spark, source_file)
        
        print("\n" + "=" * 80)
        if record_count > 0:
            print(f"✅ COMPLETED: Ingested {record_count} reviews")
        else:
            print(f"✅ COMPLETED: No new records to ingest")
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
