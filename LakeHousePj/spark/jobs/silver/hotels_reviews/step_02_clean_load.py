"""
Bước 2: Clean & Load - Hotels Reviews (Scratch → Silver)

Mục đích: Làm sạch dữ liệu, convert kiểu dữ liệu, deduplication và load vào Silver
Chiến lược:
  - Đọc từ Scratch Parquet files
  - Parse review_date: định dạng tiếng Việt → DateType (yyyy-MM-dd)
  - Convert review_score: String → DoubleType
  - Chuẩn hoá stay_date (tháng/năm → DateType với day=1)
  - Làm sạch text: review_positive, review_negative, review_title (clean mạnh)
  - Làm sạch nhẹ room_type, map về group (phòng đôi, phòng đơn, căn hộ, dorm, bungalow, biệt thự, ...)
    + Nếu KHÔNG match group nào → giữ nguyên giá trị room_type đã được clean
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


# =========================
# Helper: Normalize room_type
# =========================
def normalize_room_type_py(value: str) -> str:
    """
    Chuẩn hoá room_type về các group:
      - phòng đơn / phòng đôi / phòng twin
      - phòng tiêu chuẩn
      - phòng cao cấp
      - phòng suite
      - phòng giường king / queen
      - phòng gia đình
      - căn hộ
      - phòng dorm
      - bungalow (gồm chalet, cabin)
      - biệt thự (gồm villa, dinh thự)
      - phòng điều hành
      - phòng 3 người
      - phòng 4 người
      - lều (tent / glamping)

    Nếu KHÔNG match bất kỳ nhóm nào ở trên → trả về CHÍNH giá trị room_type đã được clean.
    (tức là không ép về 'đặc biệt' nữa để giữ chi tiết cho dashboard)
    """
    if value is None:
        return None

    s = value.strip().lower()
    if not s:
        return None

    original = s  # để trả lại nếu không match group nào

    # ==== ƯU TIÊN NHÓM ĐẶC THÙ TRƯỚC ====

    # Dorm / phòng ngủ tập thể
    dorm_keywords = [
        "dorm",
        "phòng dorm",
        "phòng ngủ tập thể",
        "giường trong phòng ngủ tập thể",
    ]
    if any(k in s for k in dorm_keywords):
        return "phòng dorm"

    # Bungalow / Chalet / Cabin
    if "bungalow" in s or "bunglalow" in s or "bungalô" in s:
        return "bungalow"
    if "chalet" in s or "cabin" in s:
        # Gom chalet/cabin vào nhóm bungalow
        return "bungalow"

    # Lều / Tent / Glamping
    if "lều" in s or "lèu" in s or "tent" in s or "glamping" in s:
        return "lều"

    # Biệt thự / Villa / Dinh thự
    villa_keywords = [
        "biệt thự",
        "villa",
        "dinh thự",
    ]
    if any(k in s for k in villa_keywords):
        return "biệt thự"

    # Căn hộ / Residence / Penthouse / Maisonette
    apt_keywords = [
        "căn hộ",
        "apartment",
        "residence",
        "penthouse",
        "maisonette"
    ]
    if any(k in s for k in apt_keywords):
        return "căn hộ"

    # Phòng gia đình
    family_keywords = [
        "gia đình",
        "family"
    ]
    if any(k in s for k in family_keywords):
        return "phòng gia đình"

    # Sức chứa: 3 người
    if "3 người" in s or "3 người" in s or " triple" in s or s.startswith("triple"):
        return "phòng 3 người"

    # Sức chứa: 4 người
    if "4 người" in s or "4 người" in s or "quadruple" in s:
        return "phòng 4 người"

    # Giường King / Queen
    if "king" in s:
        return "phòng giường king"
    if "queen" in s:
        return "phòng giường queen"

    # Twin
    if "twin" in s:
        return "phòng twin"

    # Suite / Presidential
    if "suite" in s or "presidential" in s:
        return "phòng suite"

    # Executive → phòng điều hành
    if "executive" in s or "phòng điều hành" in s:
        return "phòng điều hành"

    # Deluxe / Superior / Premium / Club / Grand / Luxury / Signature → phòng cao cấp
    if any(k in s for k in ["deluxe", "superior", "premium", "club", "grand", "luxury", "signature"]):
        return "phòng cao cấp"

    # Single / phòng đơn
    if "single" in s or "phòng đơn" in s or "phong don" in s:
        return "phòng giường đơn"

    # Double / giường đôi / phòng đôi
    if "double" in s or "giường đôi" in s or "giuong doi" in s or "phòng đôi" in s:
        return "phòng giường đôi"

    # Standard / tiêu chuẩn
    if "tiêu chuẩn" in s or "tieu chuan" in s or "standard" in s:
        return "phòng tiêu chuẩn"

    # Studio
    if "studio" in s:
        return "studio"

    # Không match group nào → giữ nguyên giá trị đã clean
    return original


normalize_room_type_udf = F.udf(normalize_room_type_py, StringType())


def parse_review_date(df):
    """
    Parse review_date tiếng Việt sang DateType
    
    Format: "Ngày đánh giá: ngày DD tháng MM năm YYYY"
    Ví dụ: "Ngày đánh giá: ngày 15 tháng 3 năm 2024"
    
    Chiến lược:
    1. Extract day, month, year bằng regex
    2. Dùng F.make_date(year, month, day) để tạo DateType
    3. Xử lý NULL (giữ nguyên NULL)
    
    Args:
        df: DataFrame input có cột review_date dạng StringType
    
    Returns:
        DataFrame với review_date dạng DateType
    """
    print("Đang parse review_date (định dạng tiếng Việt → DateType)...")
    
    # Regex pattern: "ngày DD tháng MM năm YYYY"
    pattern = r"ngày (\d+) tháng (\d+) năm (\d{4})"
    
    # Extract day, month, year
    df_parsed = df \
        .withColumn("_day", F.regexp_extract(F.col("review_date"), pattern, 1).cast("int")) \
        .withColumn("_month", F.regexp_extract(F.col("review_date"), pattern, 2).cast("int")) \
        .withColumn("_year", F.regexp_extract(F.col("review_date"), pattern, 3).cast("int"))
    
    # Tạo DateType bằng F.make_date (tự xử lý NULL)
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
    
    # Kiểm tra kết quả parsing
    total_count = df.count()
    null_before = df.filter(F.col("review_date").isNull()).count()
    parsed_count = df_with_date.filter(F.col("review_date").isNotNull()).count()
    
    print(f"   Tổng số bản ghi: {total_count:,}")
    print(f"   NULL trước khi parse: {null_before:,}")
    print(f"   Parse thành công: {parsed_count:,}")
    if total_count - null_before > 0:
        print(f"   Tỷ lệ parse: {(parsed_count / (total_count - null_before) * 100):.2f}%")
    
    return df_with_date


def clean_and_transform(df):
    """
    Làm sạch dữ liệu và convert kiểu dữ liệu
    
    Transformations:
    1. Parse review_date: String → DateType (định dạng tiếng Việt)
    2. Convert review_score: String → DoubleType
    3. Chuẩn hoá stay_date (tháng/năm → DateType)
    4. Clean text mạnh cho review_positive, review_negative, review_title
    5. Clean nhẹ room_type, map về group (phòng đôi, dorm, bungalow, biệt thự, ...)
       + Nếu không match group thì giữ nguyên room_type đã clean
    6. Loại bản ghi có NULL ở review_date, traveler_type, review_score, room_type
    7. Thêm ingestion_timestamp
    
    Args:
        df: DataFrame input từ Scratch
    
    Returns:
        DataFrame đã làm sạch với kiểu dữ liệu đúng
    """
    print("\nĐang áp dụng bước làm sạch dữ liệu và transform...")
    
    # 1. Parse review_date (định dạng tiếng Việt → DateType)
    df_cleaned = parse_review_date(df)
    
    # 2. Convert review_score: String → DoubleType
    print("Đang convert review_score (String → Double)...")
    df_cleaned = df_cleaned \
        .withColumn(
            "review_score",
            F.when(
                F.col("review_score").isNotNull(),
                F.regexp_replace(F.col("review_score"), ",", ".").cast(DoubleType())
            ).otherwise(F.lit(None).cast(DoubleType()))
        )
    
    # 3. Chuẩn hoá stay_date: lowercase, bỏ 'tháng', extract MM/YYYY
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

    try:
        total_stay = df_cleaned.count()
        parsed_stay = df_cleaned.filter((F.col("_stay_month") != "") & (F.col("_stay_year") != "")).count()
        print(f"   Tổng bản ghi stay_date: {total_stay:,}, parse được MM/YYYY: {parsed_stay:,}")
    except Exception:
        pass

    df_cleaned = df_cleaned.drop("_stay_raw", "_stay_month", "_stay_year")

    # 4. CLEAN TEXT COLUMNS: review_positive, review_negative, review_title (clean mạnh)
    print("\nĐang làm sạch text columns `review_positive`, `review_negative`, `review_title` (lowercase, loại ký tự lạ)...")
    try:
        before_pos_null = df_cleaned.filter(F.col("review_positive").isNull()).count()
        before_neg_null = df_cleaned.filter(F.col("review_negative").isNull()).count()
        before_title_null = df_cleaned.filter(F.col("review_title").isNull()).count()
    except Exception:
        before_pos_null = before_neg_null = before_title_null = None

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

    # Collapse nhiều khoảng trắng thành 1
    df_cleaned = df_cleaned \
        .withColumn("review_positive", F.regexp_replace(F.col("review_positive"), r"\s+", " ")) \
        .withColumn("review_negative", F.regexp_replace(F.col("review_negative"), r"\s+", " ")) \
        .withColumn("review_title", F.regexp_replace(F.col("review_title"), r"\s+", " "))

    try:
        after_pos_null = df_cleaned.filter(F.col("review_positive").isNull()).count()
        after_neg_null = df_cleaned.filter(F.col("review_negative").isNull()).count()
        after_title_null = df_cleaned.filter(F.col("review_title").isNull()).count()
        if before_pos_null is not None:
            print(f"   review_positive NULL trước: {before_pos_null}, sau: {after_pos_null}")
            print(f"   review_negative NULL trước: {before_neg_null}, sau: {after_neg_null}")
            print(f"   review_title NULL trước: {before_title_null}, sau: {after_title_null}")
    except Exception:
        pass

    # 5. CLEAN NHẸ room_type (giữ dấu, không xoá ký tự lạ)
    print("\nĐang làm sạch nhẹ cột room_type (lowercase, bỏ HTML/URL/newline, giữ dấu tiếng Việt)...")
    df_cleaned = df_cleaned.withColumn(
        "room_type",
        F.when(
            F.col("room_type").isNotNull(),
            F.lower(
                F.regexp_replace(
                    F.regexp_replace(
                        F.trim(F.col("room_type")),
                        r"<[^>]+>", " "
                    ),
                    r"http\S+|www\.[^\s]+", " "
                )
            )
        ).otherwise(F.lit(None))
    )

    # Gộp nhiều khoảng trắng
    df_cleaned = df_cleaned.withColumn("room_type", F.regexp_replace(F.col("room_type"), r"\s+", " "))

    # Convert empty string → NULL
    df_cleaned = df_cleaned.withColumn(
        "room_type",
        F.when((F.col("room_type").isNull()) | (F.col("room_type") == ""), F.lit(None)).otherwise(F.col("room_type"))
    )

    # ================================
    # Normalize & map `traveler_type`
    # - Lowercase, trim, collapse spaces
    # - Map values like 'phòng gia đình' or 'family' -> 'gia đình'
    # (Do mapping trước khi loại NULL để filter đúng)
    # ================================
    print("\nĐang chuẩn hoá cột `traveler_type` và map giá trị family → 'gia đình' nếu cần...")
    df_cleaned = df_cleaned.withColumn(
        "traveler_type",
        F.when(
            F.col("traveler_type").isNotNull(),
            F.lower(F.regexp_replace(F.trim(F.col("traveler_type")), r"\s+", " "))
        ).otherwise(F.lit(None))
    )

    # Map exact/common family values to canonical 'gia đình'
    # Note: `traveler_type` was lowercased and trimmed above, so we can match exact normalized strings.
    df_cleaned = df_cleaned.withColumn(
        "traveler_type",
        F.when(F.col("traveler_type").isin("phòng gia đình"), F.lit("Gia đình")).otherwise(F.col("traveler_type"))
    )

    # 6. Loại bản ghi có NULL ở các cột business quan trọng (bao gồm room_type)
    print("\n⚠️  Đang loại các bản ghi có NULL ở `review_date`, `traveler_type`, `review_score`, `room_type`...")
    before_null_filter = df_cleaned.count()
    df_cleaned = df_cleaned.filter(
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
    print(f"   Số bản ghi trước khi filter NULL: {before_null_filter:,}")
    print(f"   Số bản ghi sau khi filter NULL:  {after_null_filter:,}")
    print(f"   Số bản ghi bị loại (NULL):       {removed_nulls:,}")

    # 7. Map room_type → group chuẩn bằng UDF
    #    - Các giá trị match pattern → gom về group (phòng đôi, căn hộ, dorm, bungalow, biệt thự,...)
    #    - Các giá trị KHÔNG match → giữ nguyên (original cleaned text)
    print("\nĐang chuẩn hoá room_type về group nếu match (ngược lại giữ nguyên)...")
    df_cleaned = df_cleaned.withColumn("room_type", normalize_room_type_udf(F.col("room_type")))

    # 8. Thêm ingestion_timestamp
    print("Đang thêm cột ingestion_timestamp (TimestampType)...")
    df_cleaned = df_cleaned.withColumn("ingestion_timestamp", F.lit(datetime.now()))

    # Hiển thị sample sau khi clean
    print("\nSample dữ liệu sau khi làm sạch:")
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
        
        # List tất cả run directories
        file_statuses = fs.listStatus(base_path)
        run_dirs = [
            status.getPath().getName()
            for status in file_statuses
            if status.isDirectory() and status.getPath().getName().startswith("run_")
        ]
        
        if not run_dirs:
            raise Exception(f"Không tìm thấy run directory nào trong {SCRATCH_BASE_PATH}")
        
        # Sort theo timestamp (run_YYYYMMDD_HHMMSS) và lấy run mới nhất
        run_dirs.sort(reverse=True)
        latest_run = run_dirs[0]
        
        latest_path = f"{SCRATCH_BASE_PATH}/{latest_run}"
        print(f"Run Scratch mới nhất: {latest_run}")
        print(f"   Path: {latest_path}")
        
        return latest_path, latest_run
        
    except Exception as e:
        print(f"❌ Lỗi khi tìm Scratch run mới nhất: {e}")
        raise


def create_silver_table(spark):
    """Tạo Silver table nếu chưa tồn tại"""
    schema = StructType([
        # Business columns (kiểu dữ liệu đã làm sạch)
        StructField("hotel_name", StringType(), False),
        StructField("hotel_url", StringType(), True),
        StructField("reviewer_name", StringType(), True),
        StructField("reviewer_country", StringType(), True),
        StructField("room_type", StringType(), True),
        StructField("stay_date", DateType(), True),
        StructField("traveler_type", StringType(), True),
        StructField("review_date", DateType(), True),  # DateType (business date)
        StructField("review_title", StringType(), True),
        StructField("review_score", DoubleType(), True),  # DoubleType
        StructField("review_positive", StringType(), True),
        StructField("review_negative", StringType(), True),
        
        # Checksum dùng cho deduplication
        StructField("row_checksum", StringType(), False),
        
        # Metadata columns
        StructField("ingestion_timestamp", TimestampType(), False),  # TimestampType
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
    
    Chiến lược:
    - Đọc Silver table hiện tại
    - LEFT ANTI JOIN: Giữ lại bản ghi mới chưa có trong Silver
    - Tối ưu: Chỉ lấy cột row_checksum khi join
    
    Args:
        spark: SparkSession
        df_new: Bản ghi mới từ Scratch (đã có row_checksum)
    
    Returns:
        DataFrame chỉ còn bản ghi mới (đã deduplicated)
    """
    print("\nĐang deduplicate bằng LEFT ANTI JOIN...")
    
    try:
        # Tối ưu: chỉ select cột row_checksum (không load toàn bảng)
        print("Đang đọc existing row_checksum từ Silver table...")
        existing_checksums = spark.table(SILVER_TABLE).select("row_checksum")
        existing_count = existing_checksums.count()
        print(f"Số bản ghi hiện có trong Silver: {existing_count:,}")
        
        # LEFT ANTI JOIN: giữ lại bản ghi ở df_new chưa có trong existing
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
        print(f"   Tổng từ Scratch: {total_count:,}")
        print(f"   Bản ghi mới: {new_count:,}")
        print(f"   Bản ghi trùng (bỏ qua): {duplicate_count:,}")
        
        return df_deduplicated, new_count, duplicate_count
        
    except Exception as e:
        print(f"Silver table rỗng hoặc chưa tồn tại ({e})")
        print("   Tất cả bản ghi đều là mới (lần ingest đầu tiên)")
        new_count = df_new.count()
        return df_new, new_count, 0


def clean_and_load_to_silver(spark):
    """
    Main ETL: Đọc Scratch Parquet → Clean → Deduplicate → Load vào Silver
    
    Các bước:
    1. Đọc từ Scratch Parquet files
    2. Làm sạch dữ liệu (parse date, convert kiểu, room_type group,...)
    3. Tính row_checksum (MD5 trên các business columns)
    4. Deduplicate bằng LEFT ANTI JOIN
    5. APPEND vào Silver table
    6. Ghi log vào PostgreSQL tracking table
    
    Returns:
        int: Số bản ghi được load vào Silver
    """
    print("STEP 2: Clean & Load (Scratch → Silver)")
    print(f"   Source (Scratch): {SCRATCH_BASE_PATH}")
    print(f"   Target (Silver): {SILVER_TABLE}")
    
    # Lấy Scratch run mới nhất
    scratch_path, run_id = get_latest_scratch_run(spark)
    
    # Đọc từ Scratch Parquet
    print("\nĐang đọc dữ liệu từ Scratch bucket...")
    df_scratch = spark.read.parquet(scratch_path)
    
    scratch_count = df_scratch.count()
    print(f"Số bản ghi đọc từ Scratch: {scratch_count:,}")
    
    if scratch_count == 0:
        print("⚠️  Không có dữ liệu trong Scratch - không có gì để xử lý")
        return 0
    
    # Lấy metadata file nguồn (từ bản ghi đầu tiên - tất cả cùng file)
    source_metadata = df_scratch.select(
        "source_file", 
        "source_file_checksum", 
        "source_file_size_bytes"
    ).first()
    
    source_file = source_metadata["source_file"]
    source_checksum = source_metadata["source_file_checksum"]
    source_size_bytes = source_metadata["source_file_size_bytes"]
    
    print("\nThông tin file nguồn:")
    print(f"   Tên file: {source_file}")
    print(f"   Checksum: {source_checksum}")
    print(f"   Kích thước: {source_size_bytes / (1024 * 1024):.2f} MB")
    
    # Làm sạch và transform dữ liệu
    df_cleaned = clean_and_transform(df_scratch)
    
    # Áp dụng filter theo year/month nếu chạy batch (sau khi parse date)
    year_filter = spark.conf.get("spark.sql.year_filter", None)
    month_filter = spark.conf.get("spark.sql.month_filter", None)
    
    if year_filter:
        print(f"Đang filter theo năm: {year_filter}")
        df_cleaned = df_cleaned.filter(F.year(F.col("review_date")) == int(year_filter))
        
        if month_filter:
            print(f"Đang filter theo tháng: {month_filter}")
            df_cleaned = df_cleaned.filter(F.month(F.col("review_date")) == int(month_filter))
        
        filtered_count = df_cleaned.count()
        if month_filter:
            filter_desc = f"{year_filter}-{int(month_filter):02d}"
        else:
            filter_desc = f"year {year_filter}"
        print(f"Số bản ghi sau filter ({filter_desc}): {filtered_count:,}")
        
        if filtered_count == 0:
            print(f"⚠️  Không có bản ghi cho {filter_desc} - bỏ qua batch này")
            return 0
    
    # Tính row_checksum (MD5 trên các business columns)
    print(f"\nĐang tính row_checksum (MD5 trên {len(BUSINESS_COLUMNS)} cột business)...")
    df_with_checksum = calculate_row_checksum(df_cleaned, BUSINESS_COLUMNS)
    
    # Deduplicate bằng LEFT ANTI JOIN
    df_new, new_count, duplicate_count = deduplicate_with_left_anti_join(
        spark, df_with_checksum
    )
    
    # Ghi vào Silver table (APPEND mode)
    if new_count > 0:
        print(f"\nĐang append {new_count:,} bản ghi MỚI vào Silver table...")
        df_new.writeTo(SILVER_TABLE) \
            .using("iceberg") \
            .append()
        
        print(f"✅ Append thành công {new_count:,} bản ghi vào Silver")
        
        # Thống kê final Silver
        print("\nThống kê Silver table sau khi load:")
        silver_df = spark.table(SILVER_TABLE)
        total_silver = silver_df.count()
        print(f"   Tổng số bản ghi trong Silver: {total_silver:,}")
        
        print("\n   Top 10 khách sạn theo số lượng review:")
        silver_df.groupBy("hotel_name") \
            .count() \
            .orderBy(F.desc("count")) \
            .show(10, truncate=False)
        
    else:
        print("\nKhông có bản ghi mới để append (tất cả đều trùng)")
    
    # Ghi log vào PostgreSQL tracking table
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
            "room_type": "Group common patterns (phòng đôi, căn hộ, dorm, bungalow, biệt thự, ...) – fallback giữ nguyên giá trị đã clean"
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
    
    # Set year/month filter vào Spark config nếu có truyền tham số
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
            print(f"✅ STEP 2 HOÀN TẤT: Đã load {record_count:,} bản ghi vào Silver")
        else:
            print("✅ STEP 2 HOÀN TẤT: Không có bản ghi mới để load")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ LỖI: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
