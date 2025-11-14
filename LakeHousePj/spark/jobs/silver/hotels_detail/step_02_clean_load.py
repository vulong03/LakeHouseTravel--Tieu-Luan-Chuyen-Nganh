"""
Silver Layer - Hotels Detail - Step 2: Clean & Load
Read from Scratch, apply cleaning, and MERGE into Silver Iceberg table

Based on original transform_booking_hotels_detail.py:
- UPSERT mode (MERGE): UPDATE changed records, INSERT new records
- Row-level checksum for change detection
- Business key: hotel_url
- Partition by province
"""
# Standard library imports
import sys
import os
from datetime import datetime

# Third-party imports
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType, MapType

# Internal imports
sys.path.append('/opt/spark/jobs')
from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum, merge_into_bronze
from utils.file_tracker import log_ingestion_to_postgres

# Import config
from silver.hotels_detail.config import (
    SILVER_DATABASE,
    TABLE_NAME,
    SILVER_TABLE,
    BRONZE_BASE_PATH,
    SCRATCH_BASE_PATH,
    BUSINESS_KEY,
    BUSINESS_COLUMNS,
    PARTITION_COLUMNS,
    POSTGRES_CONN
)

def create_silver_table_if_needed(spark):
    """
    Create Silver Iceberg table if not exists
    Schema preserved from original transform_booking_hotels_detail.py
    """
    schema = StructType([
        # Original CSV columns
        StructField("hotel_name", StringType(), False),
        StructField("hotel_url", StringType(), False),
        StructField("province", StringType(), False),
        StructField("description", StringType(), True),
        StructField("top_amenities", StringType(), True),
        StructField("rating_score", DoubleType(), True),
        StructField("review_count", IntegerType(), True),
        StructField("rating_breakdown", StringType(), True),
        # Row checksum for change detection
        StructField("row_checksum", StringType(), False),
        # Metadata columns
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False)
    ])
    
    print(f"Creating Silver table if not exists: {SILVER_TABLE}")
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database=SILVER_DATABASE,
        table_name=TABLE_NAME,
        schema=schema,
        partition_by=PARTITION_COLUMNS,
        catalog="silver",  # Use silver catalog
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )
    
    print(f"✅ Silver table ready: {SILVER_TABLE}")


def get_latest_scratch_run(spark):
    """Get the latest run folder from Scratch bucket"""
    try:
        # Use Hadoop FileSystem to list directories
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(SCRATCH_BASE_PATH),
            hadoop_conf
        )
        
        base_path = spark._jvm.org.apache.hadoop.fs.Path(SCRATCH_BASE_PATH)
        
        if not fs.exists(base_path):
            raise Exception(f"Scratch path does not exist: {SCRATCH_BASE_PATH}")
        
        # List all run_* directories
        status_list = fs.listStatus(base_path)
        run_dirs = []
        
        for status in status_list:
            if status.isDirectory():
                dir_name = status.getPath().getName()
                if dir_name.startswith("run_"):
                    run_dirs.append(dir_name)
        
        if not run_dirs:
            raise Exception(f"No run directories found in {SCRATCH_BASE_PATH}")
        
        # Sort to get latest (format: run_YYYYMMDD_HHMMSS)
        run_dirs.sort(reverse=True)
        latest_run = run_dirs[0]
        
        latest_path = f"{SCRATCH_BASE_PATH}/{latest_run}"
        print(f"Latest Scratch run: {latest_run}")
        print(f"   Path: {latest_path}")
        
        return latest_path, latest_run
        
    except Exception as e:
        print(f"❌ Error finding latest Scratch run: {e}")
        raise


def clean_and_load_to_silver(spark):
    """
    Read from Scratch, apply cleaning, and MERGE into Silver
    Based on original UPSERT logic
    """
    print(f"Starting Clean & Load: Scratch → Silver")
    
    # Get latest Scratch run
    scratch_path, run_id = get_latest_scratch_run(spark)
    
    # Read from Scratch
    print(f"\nReading from Scratch bucket...")
    df = spark.read.parquet(scratch_path)
    
    original_count = df.count()
    print(f"Records from Scratch: {original_count:,}")
    
    # Show sample before cleaning
    print(f"\nSample data before cleaning (first 3 rows):")
    df.select("hotel_name", "province", "rating_score", "top_amenities").show(3, truncate=50)
    
    # CLEANING LOGIC - Advanced transformations
    print(f"\nApplying data cleaning...")
    
    # 1. Trim whitespace from string columns
    string_columns = ["hotel_name", "description", "top_amenities", 
                     "review_count_text", "rating_breakdown", "activities"]
    
    for col in string_columns:
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))
    
    # 2. Parse rating_score to Double (remove non-numeric characters)
    print(f"   Converting rating_score to Double...")
    df = df.withColumn(
        "rating_score",
        F.when(
            F.col("rating_score").isNotNull() & (F.col("rating_score") != ""),
            F.regexp_replace(F.col("rating_score"), r"[^0-9.]", "").cast(DoubleType())
        ).otherwise(None)
    )
    
    # 3. Làm sạch review_count_text: chỉ lấy số, sau đó đổi tên thành review_count
    print(f"   Extracting review_count from review_count_text và thay thế trực tiếp...")
    df = df.withColumn(
        "review_count_text",
        F.when(
            F.col("review_count_text").isNotNull(),
            F.regexp_extract(F.col("review_count_text"), r"(\d+)", 1).cast(IntegerType())
        ).otherwise(None)
    )
    # Đổi tên cột review_count_text thành review_count
    df = df.withColumnRenamed("review_count_text", "review_count")
    # Xóa cột review_count_text nếu vẫn còn tồn tại (phòng trường hợp lỗi rename hoặc các bước khác)
    if "review_count_text" in df.columns:
        df = df.drop("review_count_text")
    
    # 4. Clean top_amenities - remove duplicate commas and spaces
    print(f"   Cleaning top_amenities...")
    df = df.withColumn(
        "top_amenities",
        F.when(
            F.col("top_amenities").isNotNull(),
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(F.col("top_amenities"), r"\s+", " "),  # Multiple spaces → 1
                        r",\s*,+", ","  # ,, or , , → ,
                    ),
                    r",\s*$", ""  # Remove trailing comma
                )
            )
        ).otherwise(None)
    )
    

    # 5. Replace empty strings with NULL for optional fields
    optional_fields = ["description", "top_amenities", "review_count_text", 
                      "rating_breakdown", "activities"]
    for col in optional_fields:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(F.col(col) == "", None).otherwise(F.col(col))
            )

    # 6. Làm phẳng cột rating_breakdown thành chuỗi: 'Nhân viên phục vụ: 8,0, Tiện nghi: 8,1, ...'
    print("\nLàm phẳng cột rating_breakdown thành chuỗi mô tả...")
    from pyspark.sql.functions import udf
    def flatten_rating_breakdown(s):
        try:
            import ast
            if s is None or s.strip() == '' or s.strip() == '{}':
                return None
            d = ast.literal_eval(s)
            if not isinstance(d, dict):
                return None
            # Loại bỏ các key có giá trị rỗng/null
            items = [f"{k}: {v}" for k, v in d.items() if v is not None and str(v).strip() != '']
            return ', '.join(items)
        except Exception:
            return None
    flatten_udf = udf(flatten_rating_breakdown, StringType())
    df = df.withColumn("rating_breakdown", flatten_udf(F.col("rating_breakdown")))
    print("Mẫu rating_breakdown sau khi làm phẳng:")
    df.select("rating_breakdown").show(5, truncate=False)

    # Drop cột 'activities'
    print("\nDrop cột 'activities'...")
    df = df.drop("activities")

    # Chỉ giữ lại các dòng mà rating_score, review_count, rating_breakdown đều KHÔNG null
    print("\nLọc các dòng có đủ dữ liệu ở rating_score, review_count, rating_breakdown...")
    df = df.filter(
        F.col('rating_score').isNotNull() &
        F.col('review_count').isNotNull() &
        F.col('rating_breakdown').isNotNull()
    )

    cleaned_count = df.count()
    removed_count = original_count - cleaned_count

    # Làm sạch cụm “Xem tất cả ... tiện nghi” ở cuối cột top_amenities
    print("\nLoại bỏ cụm 'Xem tất cả ... tiện nghi' ở cuối cột top_amenities...")
    df = df.withColumn(
        "top_amenities",
        F.regexp_replace(F.col("top_amenities"), r",?\s*Xem tất cả \d+ tiện nghi\.?$", "")
    )

    print(f"   Original records: {original_count:,}")
    print(f"   Cleaned records: {cleaned_count:,}")
    print(f"   Removed: {removed_count:,}")
    
    # Calculate row checksum for change detection (PRESERVED FROM ORIGINAL)
    print(f"\nCalculating row checksums for change detection...")
    df_with_checksum = calculate_row_checksum(df, BUSINESS_COLUMNS)
    
    # Show sample after cleaning
    print(f"\nSample data after cleaning (first 3 rows):")
    df_with_checksum.select("hotel_name", "province", "rating_score", "row_checksum").show(3, truncate=False)
    
    # MERGE into Silver table (UPSERT mode - PRESERVED FROM ORIGINAL)
    print(f"\nMERGE into Silver table (UPSERT mode)...")
    print(f"   Target: {SILVER_TABLE}")
    print(f"   Business Key: {BUSINESS_KEY}")
    print(f"   Strategy: UPDATE if changed, INSERT if new, SKIP if unchanged")
    
    stats = merge_into_bronze(
        spark=spark,
        new_data_df=df_with_checksum,
        target_table=SILVER_TABLE,
        business_key=BUSINESS_KEY,
        business_columns=BUSINESS_COLUMNS
    )
    
    # Print statistics
    print(f"\nMERGE Results:")
    print(f"   ✅ Inserted: {stats['inserted']:,} new records")
    print(f"   Updated: {stats['updated']:,} changed records")
    print(f"   Skipped: {stats['skipped']:,} unchanged records")
    print(f"   Total processed: {stats['inserted'] + stats['updated'] + stats['skipped']:,}")
    
    # Get source file info for tracking (added by Step 1)
    source_file_info = df.select("source_file", "source_file_checksum", "source_file_size_bytes").first()
    source_file = source_file_info.source_file if source_file_info else "unknown"
    file_checksum = source_file_info.source_file_checksum if source_file_info else "unknown"
    source_size_bytes = source_file_info.source_file_size_bytes if source_file_info else 0
    
    # Construct Bronze file path for logging
    bronze_file_path = f"{BRONZE_BASE_PATH}/{source_file}"
    
    # Log to PostgreSQL tracking (always log, even if skipped=100%)
    ingestion_details = {
        "merge_stats": {
            "inserted": stats['inserted'],
            "updated": stats['updated'],
            "skipped": stats['skipped']
        },
        "source_file": source_file,
        "scratch_run": run_id,
        "business_key": BUSINESS_KEY,
        "cleaning_stats": {
            "original": original_count,
            "cleaned": cleaned_count,
            "removed": removed_count
        }
    }
    
    log_ingestion_to_postgres(
        file_path=bronze_file_path,
        file_checksum=file_checksum,
        records_ingested=stats['inserted'] + stats['updated'],
        table_name=SILVER_TABLE,
        status="success",
        postgres_conn_params=POSTGRES_CONN,
        layer='silver',
        ingestion_details=ingestion_details,
        file_size_bytes=source_size_bytes
    )
    print(f"✅ Logged to PostgreSQL tracking")
    
    return stats

def main():
    print("=" * 80)
    print("SILVER HOTELS DETAIL - STEP 2: CLEAN & LOAD (Scratch → Silver)")
    print("=" * 80)
    
    spark = None
    try:
        spark = get_spark_session(app_name="Silver_Hotels_Detail_Step2_Clean_Load")
        
        # Create Silver table if needed
        print("\nEnsuring Silver table exists...")
        create_silver_table_if_needed(spark)
        
        # Clean and load
        print("\nCleaning and loading to Silver...")
        stats = clean_and_load_to_silver(spark)
        
        print("\n" + "=" * 80)
        print(f"✅ STEP 2 COMPLETED")
        print("=" * 80)
        print(f"   ✅ Inserted: {stats['inserted']:,}")
        print(f"   Updated: {stats['updated']:,}")
        print(f"   Skipped: {stats['skipped']:,}")
        print(f"\n✅ Hotels Detail pipeline finished successfully!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        # Log failure
        log_ingestion_to_postgres(
            file_path=SCRATCH_BASE_PATH,
            file_checksum="hotels_detail_step2_failed",
            records_ingested=0,
            table_name=SILVER_TABLE,
            status="failed",
            postgres_conn_params=POSTGRES_CONN,
            layer='silver',
            error_message=str(e)
        )
        
        sys.exit(1)     
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()