"""
Bronze Layer - Ingest Booking Hotels Detail
Source: /data/raw/booking/vietnam_hotels_detail.csv
Target: lakehouse.bronze.raw_booking_hotels_detail

Strategy: 
- UPSERT mode (MERGE): UPDATE changed records, INSERT new records
- Row-level checksum for change detection
- Handle multi-line CSV values (descriptions with line breaks)
- File-level checksum tracking
- Partition by province
- Business Key: hotel_url
"""

import sys
import os
from datetime import datetime
import hashlib

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum, merge_into_bronze, print_merge_stats
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
        print(f"⚠️  Could not check tracking log: {e}")
        return False


def create_bronze_table(spark):
    """Create Bronze table for hotels detail if not exists"""
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
        database="bronze",
        table_name="raw_booking_hotels_detail",
        schema=schema,
        partition_by=["province"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


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


def ingest_hotels_detail(spark, source_file_path):
    """Ingest hotels detail CSV into Bronze table using UPSERT"""
    print(f"🚀 Starting ingestion: {source_file_path}")
    
    if not os.path.exists(source_file_path):
        raise FileNotFoundError(f"Source file not found: {source_file_path}")
    
    file_checksum = calculate_file_checksum(source_file_path)
    file_size = os.path.getsize(source_file_path)
    file_name = os.path.basename(source_file_path)
    
    print(f"📄 File: {file_name}")
    print(f"📊 Size: {file_size:,} bytes")
    print(f"🔐 Checksum: {file_checksum}")
    
    # Read CSV with multiLine option for descriptions
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(source_file_path)
    
    total_records = df.count()
    print(f"📝 Total records in file: {total_records}")
    
    # Validate NOT NULL constraints
    validate_data(df)
    
    # Show data quality stats
    print(f"\n📊 Data quality stats:")
    print(f"   - Hotels with rating: {df.filter(F.col('rating_score').isNotNull() & (F.col('rating_score') != '')).count()}")
    print(f"   - Hotels with reviews: {df.filter(F.col('review_count_text').isNotNull() & (F.col('review_count_text') != '')).count()}")
    print(f"   - Hotels with activities: {df.filter(F.col('activities').isNotNull() & (F.col('activities') != '')).count()}")
    
    # Add metadata columns
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
    
    # Show province distribution
    print(f"\n📊 Province distribution:")
    df_with_checksum.groupBy("province").count().orderBy("province").show(truncate=False)
    
    # MERGE into Bronze table (UPSERT mode)
    print(f"\n🔄 MERGE into Bronze table (UPSERT mode)...")
    print(f"   Business Key: hotel_url")
    print(f"   Strategy: UPDATE if changed, INSERT if new, SKIP if unchanged")
    
    stats = merge_into_bronze(
        spark=spark,
        new_data_df=df_with_checksum,
        target_table="lakehouse.bronze.raw_booking_hotels_detail",
        business_key="hotel_url",
        business_columns=business_columns
    )
    
    # Print statistics
    print_merge_stats(stats)
    
    # Log to PostgreSQL tracking table
    log_ingestion_to_postgres(
        spark, source_file_path, file_name, file_size, 
        file_checksum, stats['inserted'] + stats['updated'], 
        "bronze.raw_booking_hotels_detail", "success"
    )
    
    return stats['inserted'] + stats['updated']
    
    return total_records


def log_ingestion_to_postgres(spark, file_path, file_name, file_size, 
                               file_checksum, records_ingested, table_name, status):
    """Log ingestion to PostgreSQL tracking table"""
    try:
        from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
        
        tracking_data = [(
            file_path, file_name, int(file_size), file_checksum,
            datetime.now(), int(records_ingested), table_name, status, None
        )]
        
        schema = StructType([
            StructField("file_path", StringType(), False),
            StructField("file_name", StringType(), False),
            StructField("file_size_bytes", LongType(), True),
            StructField("file_checksum", StringType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("records_ingested", IntegerType(), True),
            StructField("table_name", StringType(), True),
            StructField("status", StringType(), False),
            StructField("error_message", StringType(), True)
        ])
        
        tracking_df = spark.createDataFrame(tracking_data, schema)
        
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
        print(f"⚠️  Failed to log to PostgreSQL: {e}")


def main():
    print("=" * 80)
    print("🔄 BRONZE INGESTION - Booking Hotels Detail (UPSERT MODE)")
    print("=" * 80)
    
    source_file = "/data/raw/booking/vietnam_hotels_detail.csv"
    
    spark = get_spark_session(app_name="Bronze_Upsert_Booking_Hotels_Detail")
    
    try:
        print("\n1️⃣  Creating Bronze table schema...")
        create_bronze_table(spark)
        
        print("\n2️⃣  Ingesting data with UPSERT...")
        record_count = ingest_hotels_detail(spark, source_file)
        
        print("\n" + "=" * 80)
        if record_count > 0:
            print(f"✅ COMPLETED: Processed {record_count} records (INSERT/UPDATE)")
        else:
            print(f"✅ COMPLETED: No changes detected")
        print("=" * 80)
        if record_count > 0:
            print(f"✅ COMPLETED: Ingested {record_count} records")
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
