"""
Task 2: Clean & Load to Silver
- Read from 01_transformed/
- Apply cleaning rules (nulls, duplicates, standardization)
- Calculate row checksums
- MERGE into Silver Iceberg table (UPSERT mode)
- Log to PostgreSQL tracking
- Cleanup tmp folders
"""

import sys
sys.path.append('/opt/spark/jobs')
sys.path.append('/opt/spark/jobs/silver/hotels_list')

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum, merge_into_bronze, print_merge_stats
from utils.file_tracker import log_ingestion_to_postgres
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from config import *

# ============================================================================
# CLEANING FUNCTIONS
# ============================================================================

def convert_invalid_to_null(df):
    """Convert empty strings and '0' to NULL for all string columns"""
    print(f"      • Converting invalid values to NULL (empty strings, '0')...")
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(
                    (F.trim(F.col(field.name)) == "") | 
                    (F.col(field.name) == "0"),
                    None
                ).otherwise(F.col(field.name))
            )
    return df


def remove_nulls(df):
    """Remove records with NULL in required columns"""
    print(f"      • Removing NULLs in: {', '.join(REQUIRED_COLUMNS)}")
    before_count = df.count()
    
    for col in REQUIRED_COLUMNS:
        df = df.filter(F.col(col).isNotNull())
    
    after_count = df.count()
    removed = before_count - after_count
    
    if removed > 0:
        print(f"        → Removed {removed:,} records ({removed/before_count*100:.2f}%)")
    return df, removed


def remove_duplicates(df):
    """Remove duplicates based on business key"""
    print(f"      • Removing duplicates by: {BUSINESS_KEY}")
    before_count = df.count()
    
    df = df.dropDuplicates([BUSINESS_KEY])
    
    after_count = df.count()
    removed = before_count - after_count
    
    if removed > 0:
        print(f"        → Removed {removed:,} duplicates ({removed/before_count*100:.2f}%)")
    return df, removed


def trim_whitespace(df):
    """Trim whitespace from all string columns"""
    print(f"      • Trimming whitespace from string columns...")
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def validate_urls(df):
    """Remove records with invalid URLs"""
    print(f"      • Validating URLs...")
    before_count = df.count()
    
    df = df.filter(
        F.col(BUSINESS_KEY).startswith("http://") | 
        F.col(BUSINESS_KEY).startswith("https://")
    )
    
    after_count = df.count()
    removed = before_count - after_count
    
    if removed > 0:
        print(f"        → Removed {removed:,} invalid URLs")
    return df, removed


# ============================================================================
# SILVER TABLE CREATION
# ============================================================================

def create_silver_table(spark):
    """Create Silver table for hotels list if not exists"""
    schema = StructType([
        # Business columns
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
        database=SILVER_DATABASE,
        table_name=TABLE_NAME,
        schema=schema,
        partition_by=PARTITION_COLUMNS,
        catalog="silver",  # Use 'silver' catalog for correct warehouse path
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


# ============================================================================
# CLEANUP FUNCTION
# ============================================================================

def cleanup_tmp_folders(spark, transformed_path):
    """Delete tmp folder after successful load"""
    print(f"\n6️⃣  Cleaning up tmp folders...")
    
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI("s3a://scratch"),
            hadoop_conf
        )
        
        path = spark._jvm.org.apache.hadoop.fs.Path(transformed_path)
        if fs.exists(path):
            fs.delete(path, True)  # True = recursive
            print(f"   ✓ Deleted: {transformed_path}")
        
        # Keep metadata folder (if it exists)
        metadata_path = transformed_path.replace("/01_transformed", "/_metadata.json")
        print(f"   ℹ️  Metadata preserved: {metadata_path}")
        
    except Exception as e:
        print(f"   ⚠️  Could not delete tmp folder: {e}")
        print(f"      (Manual cleanup may be needed)")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_latest_run_folder(spark):
    """Find the latest run folder in scratch bucket"""
    scratch_base = f"s3a://scratch/pipeline/{LAYER}/{TABLE_NAME}"
    
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(scratch_base),
            hadoop_conf
        )
        
        base_path = spark._jvm.org.apache.hadoop.fs.Path(scratch_base)
        
        if not fs.exists(base_path):
            raise Exception(f"Scratch bucket path not found: {scratch_base}")
        
        # List all run_ folders
        run_folders = []
        status_list = fs.listStatus(base_path)
        
        for status in status_list:
            path = str(status.getPath())
            folder_name = path.split("/")[-1]
            if folder_name.startswith("run_"):
                run_folders.append(folder_name)
        
        if not run_folders:
            raise Exception(f"No run folders found in {scratch_base}")
        
        # Sort to get latest (run_YYYYMMDD_HHMMSS format)
        latest_run = sorted(run_folders)[-1]
        latest_path = f"{scratch_base}/{latest_run}/01_transformed"
        
        print(f"   📂 Found {len(run_folders)} run(s)")
        print(f"   🔍 Latest run: {latest_run}")
        print(f"   ✓ Using: {latest_path}")
        
        return latest_path
        
    except Exception as e:
        raise Exception(f"Could not find latest run folder: {e}")


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def clean_and_load(spark):
    """
    Main logic: Clean data and load to Silver
    """
    
    print(f"🚀 Task 2: Clean & Load to Silver")
    print(f"   Target: {SILVER_TABLE}")
    print(f"")
    
    # 1. Find and read latest transformed data from Task 1
    print(f"1️⃣  Finding latest run folder...")
    transformed_path = get_latest_run_folder(spark)
    
    print(f"\n2️⃣  Reading transformed data...")
    df = spark.read.parquet(transformed_path)
    original_count = df.count()
    print(f"   ✓ Read {original_count:,} records")
    
    # Get source file info for tracking
    source_file = df.select("source_file").first()[0]
    source_checksum = df.select("source_file_checksum").first()[0]
    source_size_bytes = df.select("source_file_size_bytes").first()[0]
    
    # 3. Apply cleaning rules
    print(f"\n3️⃣  Applying cleaning rules:")
    
    df = convert_invalid_to_null(df)
    df, nulls_removed = remove_nulls(df)
    df, dups_removed = remove_duplicates(df)
    df = trim_whitespace(df)
    df, invalid_urls = validate_urls(df)
    
    cleaned_count = df.count()
    total_removed = original_count - cleaned_count
    
    print(f"\n   📊 Cleaning Summary:")
    print(f"      Original: {original_count:,} → Cleaned: {cleaned_count:,}")
    print(f"      Removed: {total_removed:,} records ({total_removed/original_count*100:.2f}%)")
    
    # 3. Show province distribution
    print(f"\n3️⃣  Province distribution (after cleaning):")
    df.groupBy("province").count() \
        .orderBy(F.desc("count")) \
        .show(10, truncate=False)
    
    # 4. Prepare for Silver (add ingestion timestamp, calculate checksum)
    print(f"\n4️⃣  Preparing for Silver table...")
    
    # Add/update ingestion timestamp
    df = df.withColumn("ingestion_timestamp", F.lit(datetime.now()))
    
    # Calculate row checksum for change detection
    df = calculate_row_checksum(df, BUSINESS_COLUMNS)
    
    print(f"   ✓ Added ingestion_timestamp")
    print(f"   ✓ Calculated row_checksum for {cleaned_count:,} records")
    
    # 5. MERGE into Silver table (UPSERT mode)
    print(f"\n5️⃣  MERGE into Silver table...")
    print(f"   Strategy: UPSERT (UPDATE if changed, INSERT if new, SKIP if unchanged)")
    print(f"   Business Key: {BUSINESS_KEY}")
    
    stats = merge_into_bronze(
        spark=spark,
        new_data_df=df,
        target_table=SILVER_TABLE,
        business_key=BUSINESS_KEY,
        business_columns=BUSINESS_COLUMNS
    )
    
    # Print merge statistics
    print(f"\n   📊 MERGE Statistics:")
    print_merge_stats(stats)
    
    # 6. Log to PostgreSQL tracking
    print(f"\n   📝 Logging to tracking database...")
    
    ingestion_details = {
        "merge_stats": {
            "inserted": stats['inserted'],
            "updated": stats['updated'],
            "skipped": stats['skipped']
        },
        "cleaning_stats": {
            "original_records": original_count,
            "cleaned_records": cleaned_count,
            "nulls_removed": nulls_removed,
            "duplicates_removed": dups_removed,
            "invalid_urls_removed": invalid_urls
        },
        "source_file": source_file,
        "run_timestamp": RUN_TIMESTAMP,
        "pipeline_version": "v2_two_task"
    }
    
    log_ingestion_to_postgres(
        file_path=f"{BRONZE_BASE_PATH}/{source_file}",
        file_checksum=source_checksum,
        records_ingested=stats['inserted'] + stats['updated'],
        table_name=SILVER_TABLE,
        status="success",
        postgres_conn_params=POSTGRES_CONN,
        layer='silver',
        ingestion_details=ingestion_details,
        file_size_bytes=source_size_bytes
    )
    
    print(f"   ✓ Logged to tracking database")
    
    # 7. Cleanup tmp folders
    cleanup_tmp_folders(spark, transformed_path)
    
    # 8. Final summary
    print(f"\n{'=' * 80}")
    print(f"✅ Task 2 COMPLETED SUCCESSFULLY")
    print(f"{'=' * 80}")
    print(f"   Records processed: {original_count:,}")
    print(f"   Records cleaned:   {cleaned_count:,}")
    print(f"   Records changed:   {stats['inserted'] + stats['updated']:,}")
    print(f"      • Inserted:     {stats['inserted']:,}")
    print(f"      • Updated:      {stats['updated']:,}")
    print(f"      • Skipped:      {stats['skipped']:,}")
    print(f"{'=' * 80}")
    
    return stats


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("SILVER LAYER - HOTELS LIST - TASK 2: CLEAN & LOAD")
    print("=" * 80)
    print(f"Run ID: {RUN_TIMESTAMP}")
    print(f"Table: {TABLE_NAME}")
    print(f"Layer: {LAYER}")
    print("=" * 80)
    print("")
    
    spark = get_spark_session(app_name=f"Silver_Task2_CleanLoad_{TABLE_NAME}")
    
    try:
        # Create Silver table if not exists
        print(f"0️⃣  Creating Silver table (if not exists)...")
        create_silver_table(spark)
        print(f"   ✓ Table ready: {SILVER_TABLE}")
        print("")
        
        # Run clean and load
        stats = clean_and_load(spark)
        
        print(f"\n🎉 PIPELINE COMPLETED")
        print(f"   Total changed: {stats['inserted'] + stats['updated']:,} records")
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print(f"❌ Task 2 FAILED")
        print(f"{'=' * 80}")
        print(f"Error: {e}")
        print(f"\n⚠️  Tmp folders preserved for debugging:")
        print(f"   {PATHS['transformed']}")
        print("")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
