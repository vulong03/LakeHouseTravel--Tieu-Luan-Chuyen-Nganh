"""
Nhiệm vụ 2: Clean & Load lên Silver
- Đọc từ thư mục `01_transformed/`
- Áp dụng quy tắc làm sạch (NULL, duplicate, chuẩn hoá)
- Tính row_checksum cho mỗi hàng
- MERGE vào Silver Iceberg table (chế độ UPSERT)
- Ghi log vào PostgreSQL tracking
- (Tuỳ chọn) Dọn dẹp thư mục tạm
"""

import sys
sys.path.append('/opt/spark/jobs')
sys.path.append('/opt/spark/jobs/silver/hotels_list')

from datetime import datetime

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.merge_utils import calculate_row_checksum, merge_into_bronze, print_merge_stats
from utils.file_tracker import log_ingestion_to_postgres
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from config import *

# ============================================================================
# HÀM LÀM SẠCH DỮ LIỆU
# ============================================================================

def convert_invalid_to_null(df):
    """Chuyển chuỗi rỗng và '0' thành NULL cho tất cả cột kiểu string."""
    print("Đang chuyển giá trị không hợp lệ sang NULL (chuỗi rỗng, '0') cho các cột string...")
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
    """Loại bỏ bản ghi có NULL ở các cột bắt buộc."""
    print(f"Đang loại bỏ bản ghi có NULL ở các cột bắt buộc: {', '.join(REQUIRED_COLUMNS)}")
    before_count = df.count()
    
    for col in REQUIRED_COLUMNS:
        df = df.filter(F.col(col).isNotNull())
    
    after_count = df.count()
    removed = before_count - after_count
    
    if removed > 0 and before_count > 0:
        pct = removed / before_count * 100
        print(f"✅ Đã loại bỏ {removed:,} bản ghi NULL ({pct:.2f}%)")
    return df, removed


def remove_duplicates(df):
    """Loại bỏ bản ghi trùng lặp theo business key."""
    print(f"Đang loại bỏ bản ghi trùng theo business key: {BUSINESS_KEY}")
    before_count = df.count()
    
    df = df.dropDuplicates([BUSINESS_KEY])
    
    after_count = df.count()
    removed = before_count - after_count
    
    if removed > 0 and before_count > 0:
        pct = removed / before_count * 100
        print(f"✅ Đã loại bỏ {removed:,} bản ghi trùng ({pct:.2f}%)")
    return df, removed


def trim_whitespace(df):
    """Cắt khoảng trắng ở hai đầu cho tất cả cột kiểu string."""
    print("Đang cắt khoảng trắng cho các cột string...")
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def normalize_province(df):
    """Chuẩn hoá tên tỉnh:
    - 'Huế' -> 'Thừa Thiên Huế'
    - 'Vũng Tàu' -> 'Bà Rịa Vũng Tàu'
    Giữ nguyên các giá trị khác.
    """
    print("Chuẩn hoá giá trị cột 'province'...")
    df = df.withColumn(
        "province",
        F.expr(
            "CASE WHEN province = 'Huế' THEN 'Thừa Thiên Huế' WHEN province = 'Vũng Tàu' THEN 'Bà Rịa Vũng Tàu' ELSE province END"
        )
    )
    return df


def validate_urls(df):
    """Loại bỏ bản ghi có URL không hợp lệ ở business key."""
    print("Đang kiểm tra và loại bỏ URL không hợp lệ...")
    before_count = df.count()
    
    df = df.filter(
        F.col(BUSINESS_KEY).startswith("http://") |
        F.col(BUSINESS_KEY).startswith("https://")
    )
    
    after_count = df.count()
    removed = before_count - after_count
    
    if removed > 0:
        print(f"✅ Đã loại bỏ {removed:,} URL không hợp lệ")
    return df, removed


# ============================================================================
# TẠO BẢNG SILVER
# ============================================================================

def create_silver_table(spark):
    """Tạo Silver table cho hotels_list nếu chưa tồn tại."""
    schema = StructType([
        # Business columns
        StructField("hotel_name", StringType(), False),
        StructField("hotel_url", StringType(), False),
        StructField("province", StringType(), False),
        # Row checksum để phát hiện thay đổi
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
        catalog="silver",  # catalog 'silver' cho đúng warehouse path
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


# ============================================================================
# HÀM PHỤ TRỢ
# ============================================================================

def get_latest_run_folder(spark):
    """Tìm thư mục run_ mới nhất trong scratch bucket."""
    scratch_base = f"s3a://scratch/pipeline/{LAYER}/{TABLE_NAME}"
    
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(scratch_base),
            hadoop_conf
        )
        
        base_path = spark._jvm.org.apache.hadoop.fs.Path(scratch_base)
        
        if not fs.exists(base_path):
            raise Exception(f"Không tìm thấy đường dẫn scratch bucket: {scratch_base}")
        
        # Liệt kê tất cả thư mục run_
        run_folders = []
        status_list = fs.listStatus(base_path)
        
        for status in status_list:
            path = str(status.getPath())
            folder_name = path.split("/")[-1]
            if folder_name.startswith("run_"):
                run_folders.append(folder_name)
        
        if not run_folders:
            raise Exception(f"Không tìm thấy thư mục run_ nào trong {scratch_base}")
        
        # Sắp xếp để lấy run mới nhất (định dạng run_YYYYMMDD_HHMMSS)
        latest_run = sorted(run_folders)[-1]
        latest_path = f"{scratch_base}/{latest_run}/01_transformed"
        
        print(f"Đã tìm thấy {len(run_folders)} run trong scratch")
        print(f"Run mới nhất: {latest_run}")
        print(f"Sử dụng đường dẫn: {latest_path}")
        
        return latest_path
        
    except Exception as e:
        raise Exception(f"Không thể tìm thư mục run mới nhất: {e}")


# ============================================================================
# HÀM CHÍNH CHO TASK 2
# ============================================================================

def clean_and_load(spark):
    """
    Luồng chính: Làm sạch dữ liệu và nạp vào Silver.
    - Đọc từ 01_transformed/
    - Áp dụng quy tắc làm sạch
    - Tính row_checksum
    - MERGE vào Silver Iceberg (UPSERT)
    - Ghi log vào PostgreSQL tracking
    """
    
    print("NHIỆM VỤ 2: Clean & Load lên Silver")
    print(f"Đích: {SILVER_TABLE}\n")
    
    # 1. Tìm và đọc dữ liệu đã transform từ Task 1
    print("Bước 1: Tìm thư mục run mới nhất trong scratch...")
    transformed_path = get_latest_run_folder(spark)
    
    print("\nBước 2: Đọc dữ liệu đã transform từ 01_transformed/...")
    df = spark.read.parquet(transformed_path)
    original_count = df.count()
    print(f"✅ Đã đọc {original_count:,} bản ghi từ Scratch")
    
    # Lấy thông tin file nguồn để tracking
    source_file = df.select("source_file").first()[0]
    source_checksum = df.select("source_file_checksum").first()[0]
    source_size_bytes = df.select("source_file_size_bytes").first()[0]
    
    # 3. Áp dụng các quy tắc làm sạch
    print("\nBước 3: Áp dụng quy tắc làm sạch dữ liệu:")
    
    df = convert_invalid_to_null(df)
    df, nulls_removed = remove_nulls(df)
    df, dups_removed = remove_duplicates(df)
    df = trim_whitespace(df)
    # Chuẩn hoá tên tỉnh sau khi trim whitespace
    df = normalize_province(df)
    df, invalid_urls = validate_urls(df)
    
    cleaned_count = df.count()
    total_removed = original_count - cleaned_count
    pct_total = (total_removed / original_count * 100) if original_count > 0 else 0.0
    
    print("\nTóm tắt bước làm sạch:")
    print(f"   Số bản ghi ban đầu: {original_count:,}")
    print(f"   Số bản ghi sau khi làm sạch: {cleaned_count:,}")
    print(f"   Đã loại bỏ: {total_removed:,} bản ghi ({pct_total:.2f}%)")
    
    # 4. Phân bố theo tỉnh sau khi làm sạch
    print("\nPhân bố theo tỉnh (sau khi làm sạch):")
    df.groupBy("province").count() \
        .orderBy(F.desc("count")) \
        .show(10, truncate=False)
    
    # 5. Chuẩn bị dữ liệu cho Silver (thêm ingestion_timestamp, tính row_checksum)
    print("\nBước 4: Chuẩn bị dữ liệu cho Silver table...")
    
    # Xoá cột 'stt' nếu tồn tại
    if "stt" in df.columns:
        df = df.drop("stt")
        print("Đã xoá cột 'stt' (nếu tồn tại).")
    
    # Thêm/ cập nhật ingestion_timestamp
    df = df.withColumn("ingestion_timestamp", F.lit(datetime.now()))
    
    # Tính row_checksum cho phát hiện thay đổi
    df = calculate_row_checksum(df, BUSINESS_COLUMNS)
    
    print(f"✅ Đã thêm ingestion_timestamp và tính row_checksum cho {cleaned_count:,} bản ghi")
    
    # 6. MERGE vào Silver table (UPSERT mode)
    print("\nBước 5: MERGE vào Silver Iceberg table (chế độ UPSERT)...")
    print("Chiến lược: UPDATE nếu bản ghi thay đổi, INSERT nếu bản ghi mới, SKIP nếu không thay đổi")
    print(f"Business Key: {BUSINESS_KEY}")
    
    stats = merge_into_bronze(
        spark=spark,
        new_data_df=df,
        target_table=SILVER_TABLE,
        business_key=BUSINESS_KEY,
        business_columns=BUSINESS_COLUMNS
    )
    
    # In thống kê MERGE
    print("\nThống kê MERGE:")
    print_merge_stats(stats)
    
    # 7. Ghi log vào PostgreSQL tracking
    print("\nĐang ghi log vào PostgreSQL tracking...")
    
    ingestion_details = {
        "merge_stats": {
            "inserted": stats["inserted"],
            "updated": stats["updated"],
            "skipped": stats["skipped"]
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
        records_ingested=stats["inserted"] + stats["updated"],
        table_name=SILVER_TABLE,
        status="success",
        postgres_conn_params=POSTGRES_CONN,
        layer="silver",
        ingestion_details=ingestion_details,
        file_size_bytes=source_size_bytes
    )
    
    print("✅ Đã ghi log vào tracking database")
    
    # (Tuỳ chọn) Bước 6: Dọn dẹp thư mục tạm nếu cần – hiện tại chưa triển khai để tránh thay đổi logic pipeline
    
    # 8. Tóm tắt cuối cùng
    print(f"\n{'=' * 80}")
    print("✅ NHIỆM VỤ 2 HOÀN TẤT THÀNH CÔNG")
    print(f"{'=' * 80}")
    print(f"   Tổng bản ghi xử lý:   {original_count:,}")
    print(f"   Sau khi làm sạch:     {cleaned_count:,}")
    print(f"   Bản ghi thay đổi:     {stats['inserted'] + stats['updated']:,}")
    print(f"      - Inserted:        {stats['inserted']:,}")
    print(f"      - Updated:         {stats['updated']:,}")
    print(f"      - Skipped:         {stats['skipped']:,}")
    print(f"{'=' * 80}")
    
    return stats


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("SILVER LAYER - HOTELS LIST - NHIỆM VỤ 2: CLEAN & LOAD")
    print("=" * 80)
    print(f"Run ID: {RUN_TIMESTAMP}")
    print(f"Table: {TABLE_NAME}")
    print(f"Layer: {LAYER}")
    print("=" * 80)
    print("")
    
    spark = get_spark_session(app_name=f"Silver_Task2_CleanLoad_{TABLE_NAME}")
    
    try:
        # Tạo Silver table nếu chưa tồn tại
        print("Đang tạo Silver table (nếu chưa tồn tại)...")
        create_silver_table(spark)
        print(f"✅ Silver table sẵn sàng: {SILVER_TABLE}\n")
        
        # Chạy luồng Clean & Load
        stats = clean_and_load(spark)
        
        print("\n✅ PIPELINE HOÀN TẤT")
        print(f"   Tổng số bản ghi thay đổi (insert + update): {stats['inserted'] + stats['updated']:,}")
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print("❌ NHIỆM VỤ 2 THẤT BẠI")
        print(f"{'=' * 80}")
        print(f"Lỗi: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()