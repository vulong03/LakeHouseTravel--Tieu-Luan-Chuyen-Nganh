"""
TikTok Videos - Step 2: Clean & Load (Scratch → Silver)
- Read from Scratch bucket
- Apply advanced cleaning transformations:
  1. Format posted_date to standard yyyy-MM-dd
  2. Convert read_status from 0/1 to true/false (Boolean)
  3. Trim whitespace from string columns
- Calculate row checksums
- MERGE into Silver Iceberg table (UPSERT)
"""

import sys
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from config import (
    SILVER_TABLE, BRONZE_BASE_PATH, SCRATCH_BASE_PATH, BUSINESS_KEY, BUSINESS_COLUMNS,
    PARTITION_COLUMNS, CLEANING_CONFIG
)
from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum, merge_into_bronze, print_merge_stats
from utils.file_tracker import (
    check_if_file_ingested,
    log_ingestion_to_postgres
)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, 
    DateType, TimestampType
)

# PostgreSQL connection parameters
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}


def get_latest_scratch_run(spark, scratch_base_path):
    """
    Find the latest Scratch run folder
    
    Returns:
        str: Latest run folder name (e.g., "run_20251108_161212")
    """
    try:
        # Use Hadoop FileSystem API to list directories
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs_uri = spark._jvm.java.net.URI(scratch_base_path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(fs_uri, hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(scratch_base_path)
        
        if not fs.exists(path):
            return None
        
        # List all run folders
        file_statuses = fs.listStatus(path)
        run_folders = []
        
        for file_status in file_statuses:
            if file_status.isDirectory():
                folder_name = file_status.getPath().getName()
                if folder_name.startswith("run_"):
                    run_folders.append(folder_name)
        
        if not run_folders:
            return None
        
        # Sort by timestamp (descending)
        run_folders.sort(reverse=True)
        return run_folders[0]
        
    except Exception as e:
        print(f"⚠️  Warning: Could not find Scratch runs: {e}")
        return None


def create_silver_table(spark):
    """
    Create Silver table for TikTok videos if not exists
    
    Schema changes from Bronze:
    - vi_sub: DROPPED (full NULL)
    - read_status: String → Boolean
    - posted_date: String → Date (formatted to yyyy-MM-dd)
    """
    schema = StructType([
        # Original columns
        StructField("url", StringType(), False),
        StructField("posted_date", DateType(), True),  # CHANGED: String → Date
        StructField("read_status", BooleanType(), True),  # CHANGED: String → Boolean
        StructField("keyword", StringType(), True),
        StructField("ques_id", StringType(), True),
        StructField("target_type", StringType(), True),
        StructField("region", StringType(), True),
        StructField("has_sub", StringType(), True),
        # vi_sub: DROPPED (full NULL)
        
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
        partition_by=PARTITION_COLUMNS,
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        },
        catalog="silver"  # Force creation in Silver bucket
    )


def apply_data_cleaning(df):
    """
    Apply data cleaning transformations:
    1. Trim whitespace from string columns
    2. Format posted_date to standard yyyy-MM-dd (Date type)
    3. Convert read_status from 0/1 to true/false (Boolean)
    
    Returns:
        Cleaned DataFrame
    """
    print(f"\n🧹 Applying data cleaning...")
    
    # Get original count
    original_count = df.count()
    
    # 1. Trim whitespace from all string columns
    print(f"   📊 Step 1: Trimming whitespace from string columns...")
    string_columns = [field.name for field in df.schema.fields 
                     if isinstance(field.dataType, StringType)]
    
    for col in string_columns:
        df = df.withColumn(col, F.trim(F.col(col)))
    
    # 2. Format posted_date to Date type
    print(f"   📅 Step 2: Formatting posted_date to yyyy-MM-dd (Date type)...")
    # Cover all common date formats (ordered by likelihood based on actual Bronze CSV data)
    # Current format: M/d/yyyy  e.g. "6/9/2025", "11/30/2024", "9/3/2024"
    # Fallback formats included for future-proofing if source format changes
    df = df.withColumn(
        "posted_date",
        F.coalesce(
            # --- Slash separator (M/D/YYYY) — current format in merged_videos.csv ---
            F.to_date(F.col("posted_date"), "M/d/yyyy"),      # 6/9/2025, 9/3/2024
            F.to_date(F.col("posted_date"), "MM/dd/yyyy"),    # 11/30/2024
            F.to_date(F.col("posted_date"), "M/dd/yyyy"),     # 6/30/2024
            F.to_date(F.col("posted_date"), "MM/d/yyyy"),     # 11/3/2024
            # --- ISO format (YYYY-MM-DD) ---
            F.to_date(F.col("posted_date"), "yyyy-MM-dd"),    # 2025-04-17
            # --- Dash separator (M-D-YYYY) — fallback ---
            F.to_date(F.col("posted_date"), "M-d-yyyy"),      # 6-9-2025
            F.to_date(F.col("posted_date"), "MM-dd-yyyy"),    # 11-30-2024
            F.to_date(F.col("posted_date"), "M-dd-yyyy"),     # 6-30-2024
            F.to_date(F.col("posted_date"), "MM-d-yyyy"),     # 11-3-2024
            # --- Day-Month-Year formats (Vietnamese style, just in case) ---
            F.to_date(F.col("posted_date"), "d/M/yyyy"),      # 9/6/2025
            F.to_date(F.col("posted_date"), "dd/MM/yyyy"),    # 30/11/2024
            F.to_date(F.col("posted_date"), "d-M-yyyy"),      # 9-6-2025
            F.to_date(F.col("posted_date"), "dd-MM-yyyy"),    # 30-11-2024
        )
    )

    # Log NULL count after parsing (for monitoring data quality)
    null_date_count = df.filter(F.col("posted_date").isNull()).count()
    total_count = df.count()
    ok_count = total_count - null_date_count
    print(f"   posted_date parse result: {ok_count:,} OK, {null_date_count:,} NULL "
          f"({null_date_count / total_count * 100:.1f}%) — NULLs are empty in source CSV")
    
    # 3. Convert read_status to Boolean (0/1 → false/true)
    print(f"   🔢 Step 3: Converting read_status to Boolean (0/1 → false/true)...")
    df = df.withColumn(
        "read_status",
        F.when(F.col("read_status").isin(["1", "true", "True"]), True)
         .when(F.col("read_status").isin(["0", "false", "False"]), False)
         .otherwise(None)
    )
    
    # 4. Replace empty strings with NULL for optional fields
    print(f"   🧹 Step 4: Replacing empty strings with NULL...")
    optional_fields = ["keyword", "ques_id", "target_type", "has_sub"]
    
    for col in optional_fields:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(F.col(col) == "", None).otherwise(F.col(col))
            )
    
    cleaned_count = df.count()
    removed_count = original_count - cleaned_count
    
    print(f"   Original records: {original_count:,}")
    print(f"   Cleaned records: {cleaned_count:,}")
    print(f"   Removed: {removed_count}")
    
    return df, {
        'original': original_count,
        'cleaned': cleaned_count,
        'removed': removed_count
    }


def main():
    print("=" * 80)
    print("🔄 SILVER TIKTOK VIDEOS - STEP 2: CLEAN & LOAD (Scratch → Silver)")
    print("=" * 80)
    
    spark = get_spark_session(app_name="Silver_TikTok_Videos_Step2_Clean_Load")
    
    try:
        print("\n1️⃣  Ensuring Silver table exists...")
        print(f"📋 Creating Silver table if not exists: {SILVER_TABLE}")
        create_silver_table(spark)
        print(f"✅ Silver table ready: {SILVER_TABLE}")
        
        print("\n2️⃣  Cleaning and loading to Silver...")
        print(f"🚀 Starting Clean & Load: Scratch → Silver")
        
        # Get latest Scratch run
        latest_run = get_latest_scratch_run(spark, SCRATCH_BASE_PATH)
        
        if not latest_run:
            print(f"❌ No Scratch runs found in {SCRATCH_BASE_PATH}")
            sys.exit(1)
        
        scratch_path = f"{SCRATCH_BASE_PATH}/{latest_run}"
        print(f"📂 Latest Scratch run: {latest_run}")
        print(f"   Path: {scratch_path}")
        
        # Read from Scratch bucket
        print(f"\n📖 Reading from Scratch bucket...")
        df = spark.read.parquet(scratch_path)
        
        total_records = df.count()
        print(f"📝 Records from Scratch: {total_records:,}")
        
        # Drop vi_sub column (full NULL) - Step 2 only
        if "vi_sub" in df.columns:
            print(f"\n🗑️  Dropping vi_sub column (full NULL)...")
            df = df.drop("vi_sub")
        
        # Show sample before cleaning
        print(f"\n📋 Sample data before cleaning (first 3 rows):")
        df.select("url", "keyword", "region", "read_status", "posted_date").show(3, truncate=False)
        
        # Apply data cleaning
        df_clean, cleaning_stats = apply_data_cleaning(df)
        
        # Show sample after cleaning
        print(f"\n📋 Sample data after cleaning (first 3 rows):")
        df_clean.select("url", "keyword", "region", "read_status", "posted_date").show(3, truncate=False)
        
        # Extract source file info from DataFrame (added by Step 1)
        print(f"\n🔍 Extracting source file metadata...")
        source_file_info = df_clean.select("source_file", "source_file_checksum", "source_file_size_bytes").first()
        source_file = source_file_info.source_file if source_file_info else "unknown"
        source_checksum = source_file_info.source_file_checksum if source_file_info else "unknown"
        source_size_bytes = source_file_info.source_file_size_bytes if source_file_info else 0
        print(f"   Source file: {source_file}")
        print(f"   Checksum: {source_checksum}")
        print(f"   Size: {source_size_bytes:,} bytes")
        
        # Add ingestion_timestamp and update source columns
        df_clean = df_clean \
            .withColumn("ingestion_timestamp", F.lit(datetime.now())) \
            .withColumn("source_file", F.lit(source_file)) \
            .withColumn("source_file_checksum", F.lit(source_checksum))
        
        # Calculate row checksums
        print(f"\n🔐 Calculating row checksums for change detection...")
        df_with_checksum = calculate_row_checksum(df_clean, BUSINESS_COLUMNS)
        
        # Show sample with checksum
        print(f"\n📋 Sample data after checksum (first 3 rows):")
        df_with_checksum.select("url", "region", "row_checksum").show(3, truncate=False)
        
        # MERGE into Silver table (UPSERT mode)
        print(f"\n🔄 MERGE into Silver table (UPSERT mode)...")
        print(f"   Target: {SILVER_TABLE}")
        print(f"   Business Key: {BUSINESS_KEY}")
        print(f"   Strategy: UPDATE if changed, INSERT if new, SKIP if unchanged")
        
        print(f"📝 Executing MERGE statement...")
        print(f"   Business Key: {BUSINESS_KEY}")
        print(f"   Target Table: {SILVER_TABLE}")
        
        stats = merge_into_bronze(
            spark=spark,
            new_data_df=df_with_checksum,
            target_table=SILVER_TABLE,
            business_key=BUSINESS_KEY,
            business_columns=BUSINESS_COLUMNS
        )
        
        # Print statistics
        print_merge_stats(stats)
        
        # Log to PostgreSQL tracking
        bronze_file_path = f"{BRONZE_BASE_PATH}/{source_file}"

        ingestion_details = {
            "merge_stats": {
                "inserted": stats['inserted'],
                "updated": stats['updated'],
                "skipped": stats['skipped']
            },
            "source_file": source_file,
            "scratch_run": latest_run,
            "business_key": BUSINESS_KEY,
            "cleaning_stats": cleaning_stats
        }

        # Log là audit trail — MERGE đã thành công thì task không nên crash chỉ vì log lỗi
        try:
            log_ingestion_to_postgres(
                file_path=bronze_file_path,
                file_checksum=source_checksum,
                records_ingested=stats['inserted'] + stats['updated'],
                table_name=SILVER_TABLE,
                status="success",
                postgres_conn_params=POSTGRES_CONN,
                layer='silver',
                ingestion_details=ingestion_details,
                file_size_bytes=source_size_bytes
            )
            print(f"✅ Logged to PostgreSQL tracking")
        except Exception as log_err:
            print(f"⚠️ WARNING: Ghi log PostgreSQL thất bại (data đã vào Silver thành công): {log_err}")
        
        print("\n" + "=" * 80)
        print("✅ STEP 2 COMPLETED")
        print("=" * 80)
        print(f"   Inserted: {stats['inserted']:,}")
        print(f"   Updated: {stats['updated']:,}")
        print(f"   Skipped: {stats['skipped']:,}")
        
        print(f"\n🎉 TikTok Videos pipeline finished successfully!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
