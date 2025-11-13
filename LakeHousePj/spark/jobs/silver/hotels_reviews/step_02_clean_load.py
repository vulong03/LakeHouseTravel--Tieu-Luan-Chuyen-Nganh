"""
Step 2: Clean & Load - Hotels Reviews (Scratch → Silver)

Purpose: Apply data cleaning, type conversion, deduplication, and load to Silver
Strategy:
  - Read from Scratch Parquet files
  - Parse review_date: Vietnamese format → DateType (yyyy-MM-dd)
  - Convert review_score: String → DoubleType
  - Add ingestion_timestamp: TimestampType
  - Calculate row_checksum (all 12 business columns)
  - Deduplicate: LEFT ANTI JOIN on row_checksum (remove existing records)
  - APPEND to Silver Iceberg table
  - Log to PostgreSQL tracking table

Input: Scratch Parquet files (s3a://scratch/pipeline/silver/hotels_reviews/run_*/)
Output: Silver Iceberg table (silver.silver.hotels_reviews)
"""

import sys
import os

sys.path.append('/opt/spark/jobs')

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    DateType, TimestampType, LongType
)

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum
from utils.file_tracker import log_ingestion_to_postgres
from silver.hotels_reviews.config import (
    SILVER_DATABASE,
    TABLE_NAME,
    SILVER_TABLE,
    SCRATCH_BASE_PATH,
    BUSINESS_COLUMNS,
    PARTITION_COLUMNS,
    POSTGRES_CONN
)


def parse_review_date(df):
    """
    Parse Vietnamese review_date format to DateType
    
    Format: "Ngày đánh giá: ngày DD tháng MM năm YYYY"
    Example: "Ngày đánh giá: ngày 15 tháng 3 năm 2024"
    
    Strategy:
    1. Extract day, month, year using regex
    2. Use F.make_date(year, month, day) to create DateType
    3. Handle NULL values (keep as NULL)
    
    Args:
        df: Input DataFrame with review_date as StringType
    
    Returns:
        DataFrame with review_date as DateType
    """
    print(f"🔧 Parsing review_date (Vietnamese format → DateType)...")
    
    # Regex pattern: "ngày DD tháng MM năm YYYY"
    pattern = r"ngày (\d+) tháng (\d+) năm (\d{4})"
    
    # Extract day, month, year
    df_parsed = df \
        .withColumn("_day", F.regexp_extract(F.col("review_date"), pattern, 1).cast("int")) \
        .withColumn("_month", F.regexp_extract(F.col("review_date"), pattern, 2).cast("int")) \
        .withColumn("_year", F.regexp_extract(F.col("review_date"), pattern, 3).cast("int"))
    
    # Create DateType using F.make_date (handles NULL automatically)
    df_with_date = df_parsed \
        .withColumn("review_date_parsed", 
            F.when(
                (F.col("_day").isNotNull()) & 
                (F.col("_month").isNotNull()) & 
                (F.col("_year").isNotNull()),
                F.make_date(F.col("_year"), F.col("_month"), F.col("_day"))
            ).otherwise(F.lit(None).cast(DateType()))
        ) \
        .drop("review_date", "_day", "_month", "_year") \
        .withColumnRenamed("review_date_parsed", "review_date")
    
    # Validate parsing results
    total_count = df.count()
    null_count = df.filter(F.col("review_date").isNull()).count()
    parsed_count = df_with_date.filter(F.col("review_date").isNotNull()).count()
    
    print(f"   Total records: {total_count:,}")
    print(f"   NULL before parsing: {null_count:,}")
    print(f"   Successfully parsed: {parsed_count:,}")
    print(f"   Parse rate: {(parsed_count / (total_count - null_count) * 100):.2f}%")
    
    return df_with_date


def clean_and_transform(df):
    """
    Apply data cleaning and type conversions
    
    Transformations:
    1. Parse review_date: String → DateType (Vietnamese format)
    2. Convert review_score: String → DoubleType
    3. Keep all other columns as-is (String types)
    4. Add ingestion_timestamp: TimestampType (current datetime)
    
    Args:
        df: Input DataFrame from Scratch
    
    Returns:
        Cleaned DataFrame with proper types
    """
    print(f"\n🧹 Applying data cleaning and transformations...")
    
    # 1. Parse review_date (Vietnamese format → DateType)
    df_cleaned = parse_review_date(df)
    
    # 2. Convert review_score: String → DoubleType
    # ✅ FIX: Replace comma with dot (Vietnamese format: "8,5" → "8.5")
    print(f"🔧 Converting review_score (String → Double)...")
    df_cleaned = df_cleaned \
        .withColumn("review_score", 
            F.when(F.col("review_score").isNotNull(), 
                   F.regexp_replace(F.col("review_score"), ",", ".").cast(DoubleType())
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
    
    # 3. Update ingestion_timestamp to current datetime (TimestampType)
    print(f"🔧 Adding ingestion_timestamp (TimestampType)...")
    df_cleaned = df_cleaned \
        .withColumn("ingestion_timestamp", F.lit(datetime.now()))
    
    # Show sample after cleaning
    print(f"\n📋 Sample cleaned data:")
    df_cleaned.select(
        "hotel_name", "review_date", "review_score", "traveler_type"
    ).show(5, truncate=False)
    
    return df_cleaned


def get_latest_scratch_run(spark):
    """Get the latest run folder from Scratch bucket"""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(SCRATCH_BASE_PATH),
            hadoop_conf
        )
        
        base_path = spark._jvm.org.apache.hadoop.fs.Path(SCRATCH_BASE_PATH)
        
        if not fs.exists(base_path):
            raise Exception(f"Scratch path does not exist: {SCRATCH_BASE_PATH}")
        
        # List all run directories
        file_statuses = fs.listStatus(base_path)
        run_dirs = [
            status.getPath().getName()
            for status in file_statuses
            if status.isDirectory() and status.getPath().getName().startswith("run_")
        ]
        
        if not run_dirs:
            raise Exception(f"No run directories found in {SCRATCH_BASE_PATH}")
        
        # Sort by timestamp (run_YYYYMMDD_HHMMSS) and get latest
        run_dirs.sort(reverse=True)
        latest_run = run_dirs[0]
        
        latest_path = f"{SCRATCH_BASE_PATH}/{latest_run}"
        print(f"📂 Latest Scratch run: {latest_run}")
        print(f"   Path: {latest_path}")
        
        return latest_path, latest_run
        
    except Exception as e:
        print(f"❌ Error finding latest Scratch run: {e}")
        raise


def create_silver_table(spark):
    """Create Silver table if not exists"""
    schema = StructType([
        # Business columns (cleaned types)
        StructField("hotel_name", StringType(), False),
        StructField("hotel_url", StringType(), True),
        StructField("reviewer_name", StringType(), True),
        StructField("reviewer_country", StringType(), True),
        StructField("room_type", StringType(), True),
        StructField("stay_date", StringType(), True),
        StructField("traveler_type", StringType(), True),
        StructField("review_date", DateType(), True),  # ✅ DateType (business date)
        StructField("review_title", StringType(), True),
        StructField("review_score", DoubleType(), True),  # ✅ DoubleType
        StructField("review_positive", StringType(), True),
        StructField("review_negative", StringType(), True),
        
        # Checksum for deduplication
        StructField("row_checksum", StringType(), False),
        
        # Metadata columns
        StructField("ingestion_timestamp", TimestampType(), False),  # ✅ TimestampType
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False),
        StructField("source_file_size_bytes", LongType(), False)
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        catalog="silver",
        database=SILVER_DATABASE,
        table_name=TABLE_NAME,
        schema=schema,
        partition_by=PARTITION_COLUMNS,
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def deduplicate_with_left_anti_join(spark, df_new):
    """
    Deduplicate using LEFT ANTI JOIN on row_checksum
    
    Strategy:
    - Read existing Silver table
    - LEFT ANTI JOIN: Keep only new records NOT in Silver
    - Optimized: Repartition both DataFrames on hotel_name before join
    
    Args:
        spark: SparkSession
        df_new: New records from Scratch (with row_checksum)
    
    Returns:
        DataFrame with only new records (deduplicated)
    """
    print(f"\n🔄 Deduplicating using LEFT ANTI JOIN...")
    
    try:
        # ✅ OPTIMIZATION: Only select row_checksum column (not entire table)
        print(f"� Reading existing checksums from Silver table...")
        existing_checksums = spark.table(SILVER_TABLE).select("row_checksum")
        existing_count = existing_checksums.count()
        print(f"📊 Existing records in Silver: {existing_count:,}")
        
        # LEFT ANTI JOIN: Keep only rows from df_new NOT in existing
        # Spark will auto-optimize this join (may broadcast if small enough)
        print(f"🔍 Performing LEFT ANTI JOIN on row_checksum...")
        df_deduplicated = df_new.join(
            existing_checksums,
            on="row_checksum",
            how="left_anti"
        )
        
        new_count = df_deduplicated.count()
        total_count = df_new.count()
        duplicate_count = total_count - new_count
        
        print(f"\n📊 Deduplication results:")
        print(f"   Total from Scratch: {total_count:,}")
        print(f"   New records: {new_count:,}")
        print(f"   Duplicates (skipped): {duplicate_count:,}")
        
        return df_deduplicated, new_count, duplicate_count
        
    except Exception as e:
        print(f"📊 Silver table empty or doesn't exist yet ({e})")
        print(f"   All records are new (first ingestion)")
        new_count = df_new.count()
        return df_new, new_count, 0


def clean_and_load_to_silver(spark):
    """
    Main ETL: Read Scratch Parquet → Clean → Deduplicate → Load to Silver
    
    Steps:
    1. Read from Scratch Parquet files
    2. Apply data cleaning (date parsing, type conversion)
    3. Calculate row_checksum (all 12 business columns)
    4. Deduplicate using LEFT ANTI JOIN
    5. APPEND to Silver table
    6. Log to PostgreSQL tracking table
    
    Returns:
        int: Number of records loaded to Silver
    """
    print(f"🚀 STEP 2: Clean & Load (Scratch → Silver)")
    print(f"   Source: {SCRATCH_BASE_PATH}")
    print(f"   Target: {SILVER_TABLE}")
    
    # Get latest Scratch run
    scratch_path, run_id = get_latest_scratch_run(spark)
    
    # Read from Scratch Parquet
    print(f"\n📖 Reading from Scratch bucket...")
    df_scratch = spark.read.parquet(scratch_path)
    
    scratch_count = df_scratch.count()
    print(f"📝 Records from Scratch: {scratch_count:,}")
    
    if scratch_count == 0:
        print(f"⚠️  No data in Scratch - nothing to process")
        return 0
    
    # Get source file metadata (from first record - all same file)
    source_metadata = df_scratch.select(
        "source_file", 
        "source_file_checksum", 
        "source_file_size_bytes"
    ).first()
    
    source_file = source_metadata["source_file"]
    source_checksum = source_metadata["source_file_checksum"]
    source_size_bytes = source_metadata["source_file_size_bytes"]
    
    print(f"\n📄 Source file info:")
    print(f"   Name: {source_file}")
    print(f"   Checksum: {source_checksum}")
    print(f"   Size: {source_size_bytes / (1024 * 1024):.2f} MB")
    
    # Apply cleaning and transformations
    df_cleaned = clean_and_transform(df_scratch)
    
    # Apply year/month filter if in batch mode (after date parsing)
    year_filter = spark.conf.get("spark.sql.year_filter", None)
    month_filter = spark.conf.get("spark.sql.month_filter", None)
    
    if year_filter:
        print(f"🔍 Filtering by year: {year_filter}")
        df_cleaned = df_cleaned.filter(F.year(F.col("review_date")) == int(year_filter))
        
        if month_filter:
            print(f"🔍 Filtering by month: {month_filter}")
            df_cleaned = df_cleaned.filter(F.month(F.col("review_date")) == int(month_filter))
        
        filtered_count = df_cleaned.count()
        if month_filter:
            filter_desc = f"{year_filter}-{int(month_filter):02d}"
        else:
            filter_desc = f"year {year_filter}"
        print(f"📊 Records after filter ({filter_desc}): {filtered_count:,}")
        
        if filtered_count == 0:
            print(f"⚠️  No records for {filter_desc} - skipping")
            return 0
    
    # Calculate row_checksum (all 12 business columns)
    print(f"\n🔐 Calculating row_checksum (MD5 of {len(BUSINESS_COLUMNS)} columns)...")
    df_with_checksum = calculate_row_checksum(df_cleaned, BUSINESS_COLUMNS)
    
    # Deduplicate using LEFT ANTI JOIN
    df_new, new_count, duplicate_count = deduplicate_with_left_anti_join(
        spark, df_with_checksum
    )
    
    # Write to Silver table (APPEND mode)
    if new_count > 0:
        print(f"\n💾 Appending {new_count:,} NEW records to Silver table...")
        df_new.writeTo(SILVER_TABLE) \
            .using("iceberg") \
            .append()
        
        print(f"   ✅ Successfully appended {new_count:,} records")
        
        # Show final distribution
        print(f"\n📊 Final Silver table stats:")
        silver_df = spark.table(SILVER_TABLE)
        total_silver = silver_df.count()
        print(f"   Total records in Silver: {total_silver:,}")
        
        print(f"\n   Top 10 hotels by review count:")
        silver_df.groupBy("hotel_name") \
            .count() \
            .orderBy(F.desc("count")) \
            .show(10, truncate=False)
        
    else:
        print(f"\n⏭️  No new records to append (all duplicates)")
    
    # Log to PostgreSQL tracking table
    print(f"\n📝 Logging to PostgreSQL tracking table...")
    ingestion_details = {
        "dedup_stats": {
            "new_records": new_count,
            "duplicates_skipped": duplicate_count,
            "total_processed": scratch_count
        },
        "source_file": source_file,
        "source_path": f"s3a://bronze/lakehouse/booking_hotels_reviews/raw/{source_file}",
        "source_size_bytes": source_size_bytes,
        "dedup_method": "LEFT ANTI JOIN on row_checksum",
        "transformations": {
            "review_date": "Vietnamese format → DateType",
            "review_score": "String → DoubleType",
            "ingestion_timestamp": "TimestampType (current datetime)"
        }
    }
    
    log_ingestion_to_postgres(
        file_path=f"s3a://bronze/lakehouse/booking_hotels_reviews/raw/{source_file}",
        file_checksum=source_checksum,
        records_ingested=new_count,
        table_name="silver.hotels_reviews",
        status="success",
        postgres_conn_params=POSTGRES_CONN,
        layer='silver',
        ingestion_details=ingestion_details,
        file_size_bytes=source_size_bytes
    )
    
    return new_count


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, help='Filter by year (for batch processing)')
    parser.add_argument('--month', type=int, help='Filter by month (1-12, for batch processing)')
    args = parser.parse_args()
    
    print("=" * 80)
    if args.year and args.month:
        print(f"STEP 2: CLEAN & LOAD - Hotels Reviews {args.year}-{args.month:02d} (Scratch → Silver)")
    elif args.year:
        print(f"STEP 2: CLEAN & LOAD - Hotels Reviews Year {args.year} (Scratch → Silver)")
    else:
        print("STEP 2: CLEAN & LOAD - Hotels Reviews (Scratch → Silver)")
    print("=" * 80)
    
    spark = get_spark_session(app_name="Silver_Hotels_Reviews_Step2_Clean_Load")
    
    # Set year/month filter as Spark config if provided
    if args.year:
        spark.conf.set("spark.sql.year_filter", str(args.year))
        if args.month:
            spark.conf.set("spark.sql.month_filter", str(args.month))
            print(f"🔍 BATCH MODE: Processing year={args.year}, month={args.month}")
        else:
            print(f"🔍 BATCH MODE: Processing year={args.year} only")
    
    try:
        print("\n1️⃣  Creating Silver table schema...")
        create_silver_table(spark)
        
        print("\n2️⃣  Cleaning and loading to Silver...")
        record_count = clean_and_load_to_silver(spark)
        
        print("\n" + "=" * 80)
        if record_count > 0:
            print(f"✅ STEP 2 COMPLETED: Loaded {record_count:,} records to Silver")
        else:
            print(f"✅ STEP 2 COMPLETED: No new records to load")
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
