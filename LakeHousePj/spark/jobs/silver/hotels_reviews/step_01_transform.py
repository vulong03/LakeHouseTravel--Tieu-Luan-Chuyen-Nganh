"""
Bước 1: Transform - Hotels Reviews (Bronze → Scratch)

Mục đích: Đọc từ Bronze CSV, giữ NGUYÊN dữ liệu với transform tối thiểu
Chiến lược:
  - Tìm Bronze CSV mới nhất dựa trên timestamp trong tên file
  - Kiểm tra file đã được xử lý chưa (qua PostgreSQL tracking)
  - Đọc với multiLine=true (review text có thể xuống dòng)
  - Chuẩn hoá tên cột
  - Drop cột 'province' nếu tồn tại (thuộc hotels_list, không thuộc reviews)
  - Lọc bỏ bản ghi có hotel_name NULL (data quality)
  - Thêm metadata: source_file, source_file_checksum, source_file_size_bytes
  - Ghi ra Scratch bucket dạng Parquet (KHÔNG partition để tránh skew)
  - Log vào PostgreSQL tracking table
  - KHÔNG dedup, KHÔNG làm sạch dữ liệu (giữ Bronze giống gốc)

Input: Bronze CSV files (s3a://bronze/.../raw/vietnam_hotels_reviews_*.csv)
Output: Scratch Parquet files (s3a://scratch/pipeline/silver/hotels_reviews/run_YYYYMMDD_HHMMSS/)
"""

import sys
import os
import re

sys.path.append('/opt/spark/jobs')

from datetime import datetime
from pyspark.sql import functions as F

from utils.spark_session import get_spark_session
from silver.hotels_reviews.config import (
    SCRATCH_BASE_PATH,
    BRONZE_BASE_PATH,
    BRONZE_FILE_PATTERN,
    PARTITION_COLUMNS,
    NOT_NULL_COLUMNS,
    POSTGRES_CONN
)
from utils.file_tracker import check_if_file_ingested


def get_s3_file_size(spark, file_path):
    """
    Lấy kích thước file từ S3/HDFS bằng Hadoop FileSystem API
    
    Args:
        spark: SparkSession
        file_path: Đường dẫn đầy đủ trên S3 (vd: s3a://bronze/.../file.csv)
    
    Returns:
        int: Kích thước file (bytes)
    """
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs_uri = spark._jvm.java.net.URI(file_path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(fs_uri, hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
        file_status = fs.getFileStatus(path)
        size_bytes = file_status.getLen()
        return size_bytes
    except Exception as e:
        print(f"⚠️  Cảnh báo: Không thể lấy kích thước file cho {file_path}: {e}")
        return 0


def get_latest_bronze_file(spark, bronze_base_path, file_pattern):
    """
    Tìm Bronze file mới nhất dựa trên timestamp trong tên file
    
    Định dạng tên file: vietnam_hotels_reviews_YYYYMMDD_HHMMSS_checksum.csv
    
    Args:
        spark: SparkSession
        bronze_base_path: Đường dẫn base tới Bronze files
        file_pattern: Regex pattern để parse tên file
    
    Returns:
        tuple: (file_path, file_checksum, file_name) hoặc (None, None, None)
    """
    try:
        files_df = spark.read.format("binaryFile") \
            .load(f"{bronze_base_path}/*.csv") \
            .select("path")
        
        file_list = [row.path for row in files_df.collect()]
        
        if not file_list:
            print(f"❌ Không tìm thấy Bronze file nào trong {bronze_base_path}")
            return None, None, None
        
        print(f"Đã tìm thấy {len(file_list)} Bronze files")
        
        # Parse timestamp từ tên file
        files_with_timestamp = []
        
        for file_path in file_list:
            file_name = file_path.split("/")[-1]
            match = re.search(file_pattern, file_name)
            
            if match:
                timestamp_str = match.group(1).replace("_", "")
                checksum = match.group(2)
                files_with_timestamp.append((file_path, timestamp_str, checksum, file_name))
        
        if not files_with_timestamp:
            print(f"❌ Không có file nào khớp pattern {file_pattern}")
            return None, None, None
        
        # Sort theo timestamp (giảm dần) và lấy file mới nhất
        files_with_timestamp.sort(key=lambda x: x[1], reverse=True)
        latest_file_path, latest_timestamp, file_checksum, file_name = files_with_timestamp[0]
        
        print(f"File mới nhất: {file_name}")
        print(f"   Timestamp: {latest_timestamp}")
        print(f"   Checksum: {file_checksum}")
        
        return latest_file_path, file_checksum, file_name
        
    except Exception as e:
        print(f"❌ Lỗi khi tìm Bronze file mới nhất: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None


def validate_data(df):
    """Validate NOT NULL cho các cột quan trọng và trả về DataFrame đã làm sạch"""
    null_checks = {}
    for col_name in NOT_NULL_COLUMNS:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_checks[col_name] = null_count
    
    total_nulls = sum(null_checks.values())
    
    if total_nulls > 0:
        print(f"⚠️  Cảnh báo: Phát hiện {total_nulls} giá trị NULL trong các cột quan trọng")
        for col_name, count in null_checks.items():
            if count > 0:
                print(f"   - {col_name}: {count} bản ghi NULL")
        
        print(f"   Đang lọc các bản ghi có giá trị NULL ở cột quan trọng")
        df_clean = df
        for col_name in NOT_NULL_COLUMNS:
            df_clean = df_clean.filter(F.col(col_name).isNotNull())
        
        return df_clean, total_nulls
    
    print(f"✅ Kiểm tra dữ liệu thành công - không có NULL ở các cột quan trọng")
    return df, 0


def transform_bronze_to_scratch(spark):
    """
    Transform: Đọc Bronze CSV mới nhất → Ghi ra Scratch Parquet
    
    Giữ nguyên dữ liệu từ Bronze với transform tối thiểu:
    - Chuẩn hoá tên cột
    - Drop 'province' nếu tồn tại
    - Lọc NULL hotel_name (data quality)
    - Thêm metadata columns
    """
    print(f"BƯỚC 1: Transform Bronze → Scratch")
    print(f"   Nguồn (Source): {BRONZE_BASE_PATH}")
    print(f"   Đích (Target): {SCRATCH_BASE_PATH}")
    
    # Tìm Bronze file mới nhất
    latest_file_path, file_checksum, file_name = get_latest_bronze_file(
        spark, BRONZE_BASE_PATH, BRONZE_FILE_PATTERN
    )
    
    if not latest_file_path:
        print(f"❌ Không có Bronze file nào để xử lý")
        return 0
    
    # Lấy kích thước file
    file_size_bytes = get_s3_file_size(spark, latest_file_path)
    file_size_mb = file_size_bytes / (1024 * 1024)
    print(f"\nĐang xử lý file:")
    print(f"   Tên file: {file_name}")
    print(f"   Checksum: {file_checksum}")
    print(f"   Kích thước: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
    
    # Kiểm tra file đã được xử lý ở Silver layer chưa
    if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
        print(f"\nFile đã được xử lý ở Silver layer (checksum: {file_checksum})")
        print(f"   Bỏ qua bước transform cho file này...")
        return None, 0  # Trả về tuple: (không có output_path, 0 records)
    
    # Đọc CSV từ Bronze layer
    # LƯU Ý: multiLine=true cho review text có thể xuống dòng
    print(f"\nĐang đọc CSV từ Bronze (multiLine mode)...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("encoding", "UTF-8") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .csv(latest_file_path)
    
    # Chuẩn hoá tên cột (bỏ khoảng trắng, chuyển sang lowercase)
    print(f"Đang chuẩn hoá tên cột...")
    for col_name in df.columns:
        normalized = col_name.strip().replace(" ", "_").lower()
        if normalized != col_name:
            df = df.withColumnRenamed(col_name, normalized)
    
    # Drop cột 'province' nếu tồn tại (thuộc hotels_list, không thuộc reviews)
    if "province" in df.columns:
        print(f"⚠️  Drop cột 'province' (không thuộc schema reviews)")
        df = df.drop("province")
    
    total_records = df.count()
    print(f"\nTổng số bản ghi từ Bronze: {total_records:,}")
    
    # Validate và làm sạch dữ liệu
    df_clean, null_count = validate_data(df)
    
    if null_count > 0:
        clean_count = df_clean.count()
        print(f"   Số bản ghi sau khi validate: {clean_count:,} (đã loại {null_count} bản ghi)")
        df = df_clean
    
    # Thêm metadata columns
    print(f"\nĐang thêm metadata columns...")
    df_with_metadata = df \
        .withColumn("source_file", F.lit(file_name)) \
        .withColumn("source_file_checksum", F.lit(file_checksum)) \
        .withColumn("source_file_size_bytes", F.lit(file_size_bytes)) \
        .withColumn("ingestion_timestamp", F.lit(datetime.now()))
    
    # Hiển thị sample data
    print(f"\nSample dữ liệu (5 dòng đầu):")
    df_with_metadata.select(
        "hotel_name", "reviewer_name", "review_score", "review_title"
    ).show(5, truncate=False)
    
    # Thống kê phân bố dữ liệu
    print(f"\nThống kê phân bố dữ liệu:")
    print(f"   Top 10 khách sạn theo số lượng review:")
    df_with_metadata.groupBy("hotel_name") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(10, truncate=False)
    
    print(f"\n   Phân bố theo traveler_type:")
    df_with_metadata.groupBy("traveler_type") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(truncate=False)
    
    # Sinh run_id duy nhất
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{SCRATCH_BASE_PATH}/run_{run_id}"
    
    # Ghi ra Scratch bucket (định dạng Parquet, KHÔNG partition để tránh skew)
    print(f"\nĐang ghi dữ liệu ra Scratch bucket...")
    print(f"   Đường dẫn: {output_path}")
    print(f"   Định dạng: Parquet (Snappy compression)")
    print(f"   Partitioning: Không partition (flat files - tránh partition skew với khách sạn lớn)")
    
    df_with_metadata.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    final_count = df_with_metadata.count()
    print(f"\n✅ Transform hoàn tất!")
    print(f"   Số bản ghi đã ghi: {final_count:,}")
    print(f"   Output: {output_path}")
    
    return output_path, final_count


def main():
    print("=" * 80)
    print("SILVER HOTELS REVIEWS - BƯỚC 1: TRANSFORM (Bronze → Scratch)")
    print("=" * 80)
    
    spark = None
    
    try:
        spark = get_spark_session(app_name="Silver_Hotels_Reviews_Step1_Transform")
        
        output_path, record_count = transform_bronze_to_scratch(spark)
        
        print("\n" + "=" * 80)
        if record_count > 0:
            print(f"✅ BƯỚC 1 HOÀN TẤT: {record_count:,} bản ghi đã được transform sang Scratch")
        else:
            print(f"✅ BƯỚC 1 HOÀN TẤT: Không có dữ liệu mới để xử lý")
        print("=" * 80)
        print(f"\nVị trí output: {output_path}")
        print(f"\nTiếp theo: Chạy Bước 2 (Clean & Load to Silver)")
        
    except Exception as e:
        print(f"\n❌ LỖI: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()