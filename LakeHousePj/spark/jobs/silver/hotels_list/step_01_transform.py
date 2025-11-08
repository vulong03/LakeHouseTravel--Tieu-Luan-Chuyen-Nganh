"""
Task 1: Transform from Bronze
- Extract latest Bronze CSV file
- Check if already processed (tracking)
- Parse CSV with metadata
- Write to scratch: 01_transformed/

This task reuses most logic from original transform_booking_hotels_list.py
Only changes: Write to scratch instead of directly to Silver
"""

import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append('/opt/spark/jobs')
sys.path.append('/opt/spark/jobs/silver/hotels_list')

from utils.spark_session import get_spark_session
from utils.file_tracker import check_if_file_ingested
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name

# Import config from same directory
from config import *

# ============================================================================
# HELPER FUNCTIONS (Reused from original code)
# ============================================================================

def get_s3_file_size(spark, file_path):
    """
    Get file size in bytes from S3 using Hadoop FileSystem API
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(file_path), 
            hadoop_conf
        )
        
        path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
        file_status = fs.getFileStatus(path)
        size_bytes = file_status.getLen()
        
        size_mb = size_bytes / (1024 * 1024)
        print(f"📦 File size: {size_mb:.2f} MB ({size_bytes:,} bytes)")
        
        return size_bytes
    except Exception as e:
        print(f"⚠️  Could not get file size: {e}")
        return 0


def get_latest_bronze_file(spark, bronze_base_path):
    """
    Find the latest Bronze file based on timestamp in filename
    Bronze filename format: vietnam_hotels_list_YYYYMMDD_HHMMSS_checksum.csv
    
    Returns: (file_path, checksum, file_name, timestamp)
    """
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
            # Extract: vietnam_hotels_list_20251029_194555_checksum.csv
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
        print(f"📄 Checksum: {latest[1]}")
        
        return latest[2], latest[1], latest[3], latest[0]  # path, checksum, filename, timestamp
        
    except Exception as e:
        print(f"❌ Error finding latest Bronze file: {e}")
        raise


# ============================================================================
# MAIN TRANSFORM FUNCTION
# ============================================================================

def transform_from_bronze(spark):
    """
    Main transformation logic - Extract from Bronze and write to scratch
    
    Changes from original:
    - ❌ Removed: validate_data() - moved to Task 3
    - ❌ Removed: MERGE logic - moved to Task 3
    - ❌ Removed: PostgreSQL logging - moved to Task 3
    - ✅ Changed: Write to scratch instead of Silver table
    """
    
    print(f"🚀 Task 1: Transform from Bronze")
    print(f"   Source: {BRONZE_BASE_PATH}")
    print(f"   Target: {PATHS['transformed']}")
    print(f"")
    
    # 1. Find latest Bronze file
    print(f"1️⃣  Finding latest Bronze file...")
    latest_file_path, file_checksum, file_name, file_timestamp = get_latest_bronze_file(
        spark, BRONZE_BASE_PATH
    )
    
    print(f"   ✓ Selected: {file_name}")
    
    # 2. Get file size
    file_size_bytes = get_s3_file_size(spark, latest_file_path)
    
    # 3. Check if already processed in Silver layer
    print(f"\n2️⃣  Checking tracking database...")
    if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
        print(f"   ⏭️  Already processed in Silver layer")
        print(f"      Checksum: {file_checksum}")
        print(f"      Skipping transformation")
        return 0
    print(f"   ✓ New file, proceeding with transformation")
    
    # 4. Read CSV from Bronze
    print(f"\n3️⃣  Reading Bronze CSV...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .csv(latest_file_path)
    
    total_records = df.count()
    print(f"   ✓ Read {total_records:,} records")
    
    # 5. Add metadata columns (for tracking)
    print(f"\n4️⃣  Adding metadata columns...")
    df_with_metadata = df \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum)) \
        .withColumn("source_file_timestamp", F.lit(file_timestamp)) \
        .withColumn("source_file_size_bytes", F.lit(file_size_bytes)) \
        .withColumn("extraction_timestamp", F.lit(datetime.now()))
    
    print(f"   ✓ Added 5 metadata columns")
    
    # 6. Show sample data
    print(f"\n5️⃣  Sample data (first 3 rows):")
    df_with_metadata.select("hotel_name", "province", "source_file").show(3, truncate=False)
    
    # 7. Show province distribution
    print(f"\n6️⃣  Province distribution:")
    province_dist = df_with_metadata.groupBy("province").count() \
        .orderBy(F.desc("count"))
    province_dist.show(10, truncate=False)
    
    # 8. Write to scratch bucket (NEW: Changed from writing to Silver)
    print(f"\n7️⃣  Writing to scratch bucket...")
    print(f"   Path: {PATHS['transformed']}")
    
    df_with_metadata.write \
        .mode("overwrite") \
        .parquet(PATHS['transformed'])
    
    print(f"   ✓ Written {total_records:,} records as Parquet")
    
    # 9. Success summary
    print(f"\n{'=' * 80}")
    print(f"✅ Task 1 COMPLETED")
    print(f"   Records transformed: {total_records:,}")
    print(f"   Output: {PATHS['transformed']}")
    print(f"   Next: Run Task 2 (Clean)")
    print(f"{'=' * 80}")
    
    return total_records


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("SILVER LAYER - HOTELS LIST - TASK 1: TRANSFORM")
    print("=" * 80)
    print(f"Run ID: {RUN_TIMESTAMP}")
    print(f"Table: {TABLE_NAME}")
    print(f"Layer: {LAYER}")
    print("=" * 80)
    print("")
    
    spark = get_spark_session(app_name=f"Silver_Task1_Transform_{TABLE_NAME}")
    
    try:
        record_count = transform_from_bronze(spark)
        
        if record_count == 0:
            print("\n⚠️  No new data to process (already ingested)")
            sys.exit(0)
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print(f"❌ Task 1 FAILED")
        print(f"{'=' * 80}")
        print(f"Error: {e}")
        print("")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
