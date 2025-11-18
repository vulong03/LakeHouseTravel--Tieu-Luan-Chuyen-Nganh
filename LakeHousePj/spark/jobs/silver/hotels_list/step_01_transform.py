"""
Tác vụ 1: Chuyển đổi từ Bronze
- Lấy file CSV mới nhất từ Bronze
- Kiểm tra đã xử lý chưa (tracking)
- Phân tích CSV và thêm metadata
- Ghi vào scratch: 01_transformed/
"""

import sys
import os
from datetime import datetime

# Thêm đường dẫn thư mục cha vào sys.path
sys.path.append('/opt/spark/jobs')
sys.path.append('/opt/spark/jobs/silver/hotels_list')

from utils.spark_session import get_spark_session
from utils.file_tracker import check_if_file_ingested
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name

# Nhập cấu hình từ cùng thư mục
from config import *

# ============================================================================
# Functions Helpers
# ============================================================================

def get_s3_file_size(spark, file_path):
    """
    Lấy kích thước file (bytes) từ S3 bằng Hadoop FileSystem API
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
        print(f"Kích thước file: {size_mb:.2f} MB ({size_bytes:,} bytes)")
          
        return size_bytes
    except Exception as e:
        print(f"⚠️ Không thể lấy kích thước file: {e}")
        return 0

def get_latest_bronze_file(spark, bronze_base_path):
    """
    Tìm file Bronze mới nhất dựa trên timestamp trong tên file
    Định dạng tên file Bronze: vietnam_hotels_list_YYYYMMDD_HHMMSS_checksum.csv
    Trả về: (file_path, checksum, file_name, timestamp)
    """
    try:
        df_files = spark.read.text(f"{bronze_base_path}/*.csv")
        file_paths = df_files.select(input_file_name().alias("file_path")).distinct().collect()
        
        if not file_paths:
            raise Exception(f"Không tìm thấy file Bronze trong {bronze_base_path}")
        
        # Trích xuất timestamp từ tên file và sắp xếp
        files_with_ts = []
        for row in file_paths:
            file_path = row.file_path
            file_name = file_path.split("/")[-1]
            # Ví dụ tên file: vietnam_hotels_list_20251029_194555_checksum.csv
            parts = file_name.replace('.csv', '').split('_')
            if len(parts) >= 4:
                timestamp = parts[-3] + parts[-2]  # YYYYMMDD + HHMMSS
                checksum = parts[-1]
                files_with_ts.append((timestamp, checksum, file_path, file_name))
        
        # Sắp xếp theo timestamp giảm dần (mới nhất trước)
        files_with_ts.sort(reverse=True, key=lambda x: x[0])
        
        latest = files_with_ts[0]
        print(f"Tìm thấy {len(files_with_ts)} file Bronze")
        print(f"File mới nhất: {latest[3]} (timestamp: {latest[0]})")
        print(f"Checksum: {latest[1]}")
        
        return latest[2], latest[1], latest[3], latest[0]  # path, checksum, filename, timestamp
        
    except Exception as e:
        print(f"❌ Lỗi khi tìm file Bronze mới nhất: {e}")
        raise

# ============================================================================
# FUNCTIONS CHÍNH
# ============================================================================

def transform_from_bronze(spark):
    """
    Luồng chuyển đổi chính - Trích xuất từ Bronze và ghi vào scratch
    Thay đổi so với bản gốc:
    - ❌ Đã bỏ: validate_data() - chuyển sang Task 3
    - ❌ Đã bỏ: MERGE logic - chuyển sang Task 3
    - ❌ Đã bỏ: PostgreSQL logging - chuyển sang Task 3
    - ✅ Đã đổi: ghi vào scratch thay vì bảng Silver
    """
    
    print(f"Task 1: Chuyển đổi từ Bronze")
    print(f"   Nguồn: {BRONZE_BASE_PATH}")
    print(f"   Đích: {PATHS['transformed']}")
    print(f"")
    
    # 1. Tìm file Bronze mới nhất
    print(f"Đang tìm file Bronze mới nhất...")
    latest_file_path, file_checksum, file_name, file_timestamp = get_latest_bronze_file(
        spark, BRONZE_BASE_PATH
    )
    
    print(f"   Đã chọn: {file_name}")
    
    # 2. Get file size
    file_size_bytes = get_s3_file_size(spark, latest_file_path)
    
    # 3. Check if already processed in Silver layer
    print(f"\nĐang kiểm tra cơ sở dữ liệu tracking...")
    if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
        print(f"   Đã xử lý ở layer Silver")
        print(f"      Checksum: {file_checksum}")
        print(f"      Bỏ qua chuyển đổi")
        return 0
    print(f"   ✅ File mới, tiếp tục chuyển đổi")
    
    # 4. Read CSV from Bronze
    print(f"\nĐang đọc CSV từ Bronze...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .csv(latest_file_path)
    
    total_records = df.count()
    print(f"   ✅ Đã đọc {total_records:,} bản ghi")
    
    # 5. Add metadata columns (for tracking)
    print(f"\nĐang thêm cột metadata...")
    df_with_metadata = df \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum)) \
        .withColumn("source_file_timestamp", F.lit(file_timestamp)) \
        .withColumn("source_file_size_bytes", F.lit(file_size_bytes)) \
        .withColumn("extraction_timestamp", F.lit(datetime.now()))
    
    print(f"   ✅ Đã thêm 5 cột metadata")
    
    # 6. Show sample data
    print(f"\nDữ liệu mẫu (3 dòng đầu):")
    df_with_metadata.select("hotel_name", "province", "source_file").show(3, truncate=False)
    
    # 7. Show province distribution
    print(f"\nPhân bố theo tỉnh:")
    province_dist = df_with_metadata.groupBy("province").count() \
        .orderBy(F.desc("count"))
    province_dist.show(10, truncate=False)
    
    # 8. Write to scratch bucket (NEW: Changed from writing to Silver)
    print(f"\nĐang ghi vào scratch bucket...")
    print(f"   Đường dẫn: {PATHS['transformed']}")
    
    df_with_metadata.write \
        .mode("overwrite") \
        .parquet(PATHS['transformed'])
    
    print(f"   ✅ Đã ghi {total_records:,} bản ghi dưới dạng Parquet")
    
    # 9. Success summary
    print(f"\n{'=' * 80}")
    print(f"✅ Task 1 HOÀN THÀNH")
    print(f"   Số bản ghi đã chuyển đổi: {total_records:,}")
    print(f"   Đầu ra: {PATHS['transformed']}")
    print(f"   Tiếp theo: Chạy Task 2 (Clean)")
    print(f"{ '=' * 80}")
    
    return total_records

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    print("=" * 80)
    print("LAYER SILVER - HOTELS LIST - TÁC VỤ 1: CHUYỂN ĐỔI")
    print("=" * 80)
    print(f"Run ID: {RUN_TIMESTAMP}")
    print(f"Bảng: {TABLE_NAME}")
    print(f"Layer: {LAYER}")
    print("=" * 80)
    print("")
    
    spark = get_spark_session(app_name=f"Silver_Task1_Transform_{TABLE_NAME}")
    
    try:
        record_count = transform_from_bronze(spark)
        
        if record_count == 0:
            print("\n⚠️ Không có dữ liệu mới để xử lý (đã được ingest)")
            sys.exit(0)
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print(f"❌ Tác vụ 1 THẤT BẠI")
        print(f"{'=' * 80}")
        print(f"Lỗi: {e}")
        print("")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()