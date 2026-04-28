"""
Bước 2: Clean & Load - Hotels Reviews (Scratch → Silver)

Mục đích: Làm sạch dữ liệu, convert kiểu dữ liệu, deduplication và load vào Silver
Chiến lược:
  - Đọc từ Scratch Parquet files
  - Parse review_date: định dạng tiếng Việt → DateType (yyyy-MM-dd)
  - Convert review_score: String → DoubleType
  - Chuẩn hoá stay_date (tháng/năm → DateType với day=1)
    - Làm sạch text: review_positive, review_negative, review_title (clean mạnh)
  - Chuẩn hoá traveler_type (lowercase + gộp space) + map "phòng gia đình" → "Gia đình"
  - Loại bỏ bản ghi NULL ở các cột business chính
  - Thêm ingestion_timestamp: TimestampType
  - Tính row_checksum (12 cột business)
  - Deduplicate: LEFT ANTI JOIN trên row_checksum (loại bỏ bản ghi đã tồn tại)
  - APPEND vào Silver Iceberg table
  - Ghi log vào PostgreSQL tracking table

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
    Parse review_date tiếng Việt sang DateType
    Format: "Ngày đánh giá: ngày DD tháng MM năm YYYY"
    """
    print("Đang parse review_date (định dạng tiếng Việt → DateType)...")

    pattern = r"ngày (\d+) tháng (\d+) năm (\d{4})"

    df_parsed = df \
        .withColumn("_day", F.regexp_extract(F.col("review_date"), pattern, 1).cast("int")) \
        .withColumn("_month", F.regexp_extract(F.col("review_date"), pattern, 2).cast("int")) \
        .withColumn("_year", F.regexp_extract(F.col("review_date"), pattern, 3).cast("int"))

    df_with_date = df_parsed \
        .withColumn(
            "review_date_parsed",
            F.when(
                (F.col("_day").isNotNull()) &
                (F.col("_month").isNotNull()) &
                (F.col("_year").isNotNull()),
                F.make_date(F.col("_year"), F.col("_month"), F.col("_day"))
            ).otherwise(F.lit(None).cast(DateType()))
        ) \
        .drop("review_date", "_day", "_month", "_year") \
        .withColumnRenamed("review_date_parsed", "review_date")

    print("Parse review_date hoàn tất")
    return df_with_date


def clean_and_transform(df):
    """
    Làm sạch dữ liệu và convert kiểu dữ liệu
    """
    print("\nĐang áp dụng bước làm sạch dữ liệu và transform...")

    # 1. Filter NULL records FIRST (before any transformation)
    print("\nĐang loại các bản ghi có NULL ở `review_date`, `traveler_type`, `review_score`, `room_type`...")
    before_null_filter = df.count()
    df_cleaned = df.filter(
        (F.col("review_date").isNotNull()) &
        (F.col("traveler_type").isNotNull()) &
        (F.col("review_score").isNotNull()) &
        (F.col("room_type").isNotNull()) &
        (F.col("review_title").isNotNull()) &
        (F.col("reviewer_name").isNotNull()) &
        (F.col("reviewer_country").isNotNull())
    )
    after_null_filter = df_cleaned.count()
    removed_nulls = before_null_filter - after_null_filter
    print(f"Số bản ghi trước khi filter NULL: {before_null_filter:,}")
    print(f"Số bản ghi sau khi filter NULL:  {after_null_filter:,}")
    print(f"Số bản ghi bị loại (NULL):       {removed_nulls:,}")

    # 2. Parse review_date
    df_cleaned = parse_review_date(df_cleaned)

    # 3. Convert review_score
    print("\nĐang convert review_score (String → Double)...")
    df_cleaned = df_cleaned.withColumn(
        "review_score",
        F.when(
            F.col("review_score").isNotNull(),
            F.regexp_replace(F.col("review_score"), ",", ".").cast(DoubleType())
        ).otherwise(F.lit(None).cast(DoubleType()))
    )

    # 4. Chuẩn hoá stay_date
    print("\nĐang chuẩn hoá stay_date: lowercase và bỏ 'tháng' trước khi extract month/year...")
    df_cleaned = df_cleaned.withColumn("_stay_raw", F.lower(F.coalesce(F.col("stay_date"), F.lit(""))))
    df_cleaned = df_cleaned.withColumn("_stay_raw", F.regexp_replace(F.col("_stay_raw"), r"tháng[:\s]*", ""))
    df_cleaned = df_cleaned.withColumn("_stay_month", F.regexp_extract(F.col("_stay_raw"), r"(\d{1,2})(?=/|\s|$)", 1)) \
                           .withColumn("_stay_year", F.regexp_extract(F.col("_stay_raw"), r"(\d{4})", 1))

    df_cleaned = df_cleaned.withColumn(
        "stay_date",
        F.when(
            (F.col("_stay_month") != "") & (F.col("_stay_year") != ""),
            F.make_date(F.col("_stay_year").cast("int"), F.col("_stay_month").cast("int"), F.lit(1))
        ).otherwise(F.lit(None).cast(DateType()))
    )

    df_cleaned = df_cleaned.drop("_stay_raw", "_stay_month", "_stay_year")
    print("Parse stay_date hoàn tất")

    # 5. Clean mạnh review_positive / review_negative / review_title
    print("\nĐang làm sạch text columns (lowercase, remove HTML/URLs/special chars)...")

    def clean_text(col_name: str):
        return F.when(
            F.col(col_name).isNotNull(),
            F.lower(
                F.regexp_replace(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_replace(F.trim(F.col(col_name)), r"<[^>]+>", " "),
                            r"http\S+|www\.[^\s]+", " "
                        ),
                        r"[\r\n]+", " "
                    ),
                    r"[^\p{L}\p{N}\p{P}\p{Z}]+", " "
                )
            )
        ).otherwise(F.lit(None))

    df_cleaned = df_cleaned \
        .withColumn("review_positive", clean_text("review_positive")) \
        .withColumn("review_negative", clean_text("review_negative")) \
        .withColumn("review_title", clean_text("review_title"))

    df_cleaned = df_cleaned \
        .withColumn("review_positive", F.regexp_replace(F.col("review_positive"), r"\s+", " ")) \
        .withColumn("review_negative", F.regexp_replace(F.col("review_negative"), r"\s+", " ")) \
        .withColumn("review_title", F.regexp_replace(F.col("review_title"), r"\s+", " "))

    print("Text cleaning hoàn tất")

    # 6. Thêm ingestion_timestamp
    df_cleaned = df_cleaned.withColumn("ingestion_timestamp", F.lit(datetime.now()))

    print("\nClean & Transform hoàn tất. Sample dữ liệu:")
    df_cleaned.select(
        "hotel_name", "review_date", "review_score", "traveler_type", "room_type"
    ).show(5, truncate=False)

    return df_cleaned


def get_latest_scratch_run(spark):
    """Lấy run folder mới nhất từ Scratch bucket"""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(SCRATCH_BASE_PATH),
            hadoop_conf
        )

        base_path = spark._jvm.org.apache.hadoop.fs.Path(SCRATCH_BASE_PATH)

        if not fs.exists(base_path):
            raise Exception(f"Scratch path không tồn tại: {SCRATCH_BASE_PATH}")

        file_statuses = fs.listStatus(base_path)
        run_dirs = [
            status.getPath().getName()
            for status in file_statuses
            if status.isDirectory() and status.getPath().getName().startswith("run_")
        ]

        if not run_dirs:
            raise Exception(f"Không tìm thấy run directory nào trong {SCRATCH_BASE_PATH}")

        run_dirs.sort(reverse=True)
        latest_run = run_dirs[0]

        latest_path = f"{SCRATCH_BASE_PATH}/{latest_run}"
        print(f"Run Scratch mới nhất: {latest_run}")
        print(f"Path: {latest_path}")

        return latest_path, latest_run

    except Exception as e:
        print(f"Lỗi khi tìm Scratch run mới nhất: {e}")
        raise


def create_silver_table(spark):
    """Tạo Silver table nếu chưa tồn tại"""
    schema = StructType([
        StructField("hotel_name", StringType(), False),
        StructField("hotel_url", StringType(), True),
        StructField("reviewer_name", StringType(), True),
        StructField("reviewer_country", StringType(), True),
        StructField("room_type", StringType(), True),
        StructField("stay_date", DateType(), True),
        StructField("traveler_type", StringType(), True),
        StructField("review_date", DateType(), True),
        StructField("review_title", StringType(), True),
        StructField("review_score", DoubleType(), True),
        StructField("review_positive", StringType(), True),
        StructField("review_negative", StringType(), True),
        StructField("row_checksum", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False),
        StructField("source_file_size_bytes", LongType(), False)
    ])

    create_iceberg_table_if_not_exists(
        spark=spark,
        database=SILVER_DATABASE,
        table_name=TABLE_NAME,
        schema=schema,
        partition_by=PARTITION_COLUMNS,
        catalog="silver",
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def deduplicate_with_left_anti_join(spark, df_new):
    """
    Deduplicate bằng LEFT ANTI JOIN trên row_checksum
    """
    print("\nĐang deduplicate bằng LEFT ANTI JOIN...")

    try:
        print("Đang đọc existing row_checksum từ Silver table...")
        existing_checksums = spark.table(SILVER_TABLE).select("row_checksum")
        existing_count = existing_checksums.count()
        print(f"Số bản ghi hiện có trong Silver: {existing_count:,}")

        print("Đang thực hiện LEFT ANTI JOIN trên row_checksum...")
        df_deduplicated = df_new.join(
            existing_checksums,
            on="row_checksum",
            how="left_anti"
        )

        new_count = df_deduplicated.count()
        total_count = df_new.count()
        duplicate_count = total_count - new_count

        print("\nKết quả deduplication:")
        print(f"Tổng từ Scratch: {total_count:,}")
        print(f"Bản ghi mới: {new_count:,}")
        print(f"Bản ghi trùng (bỏ qua): {duplicate_count:,}")

        return df_deduplicated, new_count, duplicate_count

    except Exception as e:
        print(f"Silver table rỗng hoặc chưa tồn tại ({e})")
        print("Tất cả bản ghi đều là mới (lần ingest đầu tiên)")
        new_count = df_new.count()
        return df_new, new_count, 0


def clean_and_load_to_silver(spark):
    """
    Main ETL: Đọc Scratch Parquet → Clean → Deduplicate → Load vào Silver
    """
    print("STEP 2: Clean & Load (Scratch → Silver)")
    print(f"Source (Scratch): {SCRATCH_BASE_PATH}")
    print(f"Target (Silver): {SILVER_TABLE}")

    scratch_path, run_id = get_latest_scratch_run(spark)

    print("\nĐang đọc dữ liệu từ Scratch bucket...")
    df_scratch = spark.read.parquet(scratch_path)

    scratch_count = df_scratch.count()
    print(f"Số bản ghi đọc từ Scratch: {scratch_count:,}")

    if scratch_count == 0:
        print("Không có dữ liệu trong Scratch - không có gì để xử lý")
        return 0

    source_metadata = df_scratch.select(
        "source_file",
        "source_file_checksum",
        "source_file_size_bytes"
    ).first()

    source_file = source_metadata["source_file"]
    source_checksum = source_metadata["source_file_checksum"]
    source_size_bytes = source_metadata["source_file_size_bytes"]

    print("\nThông tin file nguồn:")
    print(f"Tên file: {source_file}")
    print(f"Checksum: {source_checksum}")
    print(f"Kích thước: {source_size_bytes / (1024 * 1024):.2f} MB")

    df_cleaned = clean_and_transform(df_scratch)

    year_filter = spark.conf.get("spark.sql.year_filter", None)
    month_filter = spark.conf.get("spark.sql.month_filter", None)

    if year_filter:
        print(f"Đang filter theo năm: {year_filter}")
        df_cleaned = df_cleaned.filter(F.year(F.col("review_date")) == int(year_filter))

        if month_filter:
            print(f"Đang filter theo tháng: {month_filter}")
            df_cleaned = df_cleaned.filter(F.month(F.col("review_date")) == int(month_filter))

        filtered_count = df_cleaned.count()
        filter_desc = f"{year_filter}-{int(month_filter):02d}" if month_filter else f"year {year_filter}"
        print(f"Số bản ghi sau filter ({filter_desc}): {filtered_count:,}")

        if filtered_count == 0:
            print(f"Không có bản ghi cho {filter_desc} - bỏ qua batch này")
            return 0

    print(f"\nĐang tính row_checksum (MD5 trên {len(BUSINESS_COLUMNS)} cột business)...")
    df_with_checksum = calculate_row_checksum(df_cleaned, BUSINESS_COLUMNS)

    df_new, new_count, duplicate_count = deduplicate_with_left_anti_join(
        spark, df_with_checksum
    )

    if new_count > 0:
        print(f"\nĐang append {new_count:,} bản ghi MỚI vào Silver table...")
        df_new.writeTo(SILVER_TABLE) \
            .using("iceberg") \
            .append()

        print(f"Append thành công {new_count:,} bản ghi vào Silver")

        print("\nThống kê Silver table sau khi load:")
        silver_df = spark.table(SILVER_TABLE)
        total_silver = silver_df.count()
        print(f"Tổng số bản ghi trong Silver: {total_silver:,}")

        print("\nTop 10 khách sạn theo số lượng review:")
        silver_df.groupBy("hotel_name") \
            .count() \
            .orderBy(F.desc("count")) \
            .show(10, truncate=False)
    else:
        print("\nKhông có bản ghi mới để append (tất cả đều trùng)")

    print("\nĐang ghi log vào PostgreSQL tracking table...")
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
            "ingestion_timestamp": "TimestampType (current datetime)",
            "room_type": "No cleaning or mapping applied (kept as source)",
            "traveler_type": "No normalization or mapping applied"
        }
    }

    # Log là audit trail — Append đã thành công thì task không nên crash chỉ vì log lỗi
    try:
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
    except Exception as log_err:
        print(f"⚠️ WARNING: Ghi log PostgreSQL thất bại (data đã vào Silver thành công): {log_err}")

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

    if args.year:
        spark.conf.set("spark.sql.year_filter", str(args.year))
        if args.month:
            spark.conf.set("spark.sql.month_filter", str(args.month))
            print(f"Chế độ BATCH: Đang xử lý year={args.year}, month={args.month}")
        else:
            print(f"Chế độ BATCH: Đang xử lý year={args.year} (không filter tháng)")

    try:
        print("\nĐang tạo Silver table schema (nếu chưa tồn tại)...")
        create_silver_table(spark)

        print("\nĐang thực thi bước Clean & Load vào Silver...")
        record_count = clean_and_load_to_silver(spark)

        print("\n" + "=" * 80)
        if record_count > 0:
            print(f"STEP 2 HOÀN TẤT: Đã load {record_count:,} bản ghi vào Silver")
        else:
            print("STEP 2 HOÀN TẤT: Không có bản ghi mới để load")
        print("=" * 80)

    except Exception as e:
        print(f"\nLỖI: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
