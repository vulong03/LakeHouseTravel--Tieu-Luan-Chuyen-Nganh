# Spark Utils — Shared Utility Modules

## Tổng quan

Thư mục `spark/jobs/utils/` chứa **7 module** dùng chung cho tất cả Spark jobs (Bronze, Silver, Gold, ML). Đây là "foundation layer" của toàn bộ data pipeline.

```
spark/jobs/utils/
├── __init__.py              # Package marker
├── spark_session.py         # Tạo SparkSession (Iceberg + S3/MinIO)
├── iceberg_utils.py         # CREATE TABLE IF NOT EXISTS (Iceberg)
├── file_tracker.py          # Checksum + PostgreSQL ingestion log
├── s3_utf8_uploader.py      # Upload file qua mc client (UTF-8 safe)
├── merge_utils.py           # MERGE/UPSERT + row_checksum (MD5)
└── gold_job_logger.py       # GoldJobLogger (psycopg2 → PostgreSQL)
```

### Dependency Map

```
Bronze jobs ──→ spark_session.py
           ──→ file_tracker.py (checksum + log)
           ──→ s3_utf8_uploader.py (mc cp upload)

Silver jobs ──→ spark_session.py
            ──→ iceberg_utils.py (CREATE TABLE)
            ──→ merge_utils.py (row_checksum + MERGE)
            ──→ file_tracker.py (log)

Gold jobs ──→ spark_session.py
          ──→ iceberg_utils.py (CREATE TABLE)
          ──→ merge_utils.py (row_checksum + MERGE)
          ──→ gold_job_logger.py (log success/failure)

ML jobs ──→ spark_session.py (hoặc tự tạo SparkSession)
        ──→ iceberg_utils.py (CREATE TABLE)
```

---

## 1. `spark_session.py` — SparkSession Factory

### Chức năng
Tạo SparkSession đã cấu hình sẵn Iceberg catalogs và S3/MinIO connection.

### API

```python
from utils.spark_session import get_spark_session

spark = get_spark_session(app_name="Gold_Dim_Province")
```

### Catalogs được cấu hình

| Catalog | Warehouse | Vai trò |
|---|---|---|
| `lakehouse` (default) | `s3a://bronze/lakehouse` | Bronze layer (legacy name) |
| `silver` | `s3a://silver/lakehouse` | Silver layer |

**Lưu ý**: Catalog `gold` **không được cấu hình** trong `spark_session.py`. Gold jobs dùng config từ `spark-defaults.conf` (đã có catalog `gold`). Một số ML jobs tự tạo SparkSession riêng có gold catalog.

### S3/MinIO Config

| Key | Value |
|---|---|
| `fs.s3a.endpoint` | `http://minio:9000` |
| `fs.s3a.access.key` | `minioadmin` |
| `fs.s3a.secret.key` | `minioadmin123` |
| `fs.s3a.path.style.access` | `true` |
| `fs.s3a.impl` | `S3AFileSystem` |
| SSL | disabled |

### Đánh giá

- **Credentials hardcode** — nên dùng environment variables
- **Thiếu gold catalog** — Gold jobs phụ thuộc `spark-defaults.conf`, không nhất quán

---

## 2. `iceberg_utils.py` — Iceberg Table Creator

### Chức năng
Tạo Iceberg table nếu chưa tồn tại. Xử lý cả trường hợp metadata bị corrupt (auto-recover).

### API

```python
from utils.iceberg_utils import create_iceberg_table_if_not_exists

create_iceberg_table_if_not_exists(
    spark=spark,
    database="gold",
    table_name="dim_province",
    schema=StructType([...]),
    partition_by=["region"],              # optional
    table_properties={                     # optional
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "snappy"
    },
    catalog="gold"                         # default: "lakehouse"
)
```

### Xử lý lỗi (3 cấp)

```
1. spark.catalog.tableExists() → True → return (bảng đã có)
2. CREATE TABLE IF NOT EXISTS → thành công → return
3. Nếu fail (metadata corrupt) → CREATE OR REPLACE TABLE → return
4. Nếu vẫn fail → raise Exception
```

### Warehouse Location Logic

| Catalog | Location |
|---|---|
| `lakehouse` / `bronze` | `s3a://bronze/lakehouse/{db}.db/{table}` |
| `silver` | `s3a://silver/lakehouse/{db}.db/{table}` |
| `gold` | `s3a://gold/lakehouse/{db}.db/{table}` |

### Đánh giá

- **Tự động recovery** khi metadata corrupt — rất hữu ích cho development
- `CREATE OR REPLACE` sẽ **xóa toàn bộ data** nếu bảng đã có — nguy hiểm cho production
- Nên thêm flag `allow_replace=False` để protect production tables

---

## 3. `file_tracker.py` — File Checksum & Ingestion Log

### Chức năng
Cung cấp 3 hàm standalone cho tracking file ingestion:
1. Tính MD5 checksum
2. Kiểm tra file đã ingested chưa
3. Log kết quả vào PostgreSQL

### API

```python
from utils.file_tracker import (
    calculate_file_checksum,
    check_if_file_ingested,
    log_ingestion_to_postgres
)

# 1. Tính checksum
checksum = calculate_file_checksum("/data/raw/booking/hotels.csv")
# → "a1b2c3d4e5f6..."

# 2. Kiểm tra đã ingested chưa
pg_params = {
    "host": "postgres", "port": 5432,
    "database": "metastore_db",
    "user": "lakehouse_user", "password": "lakehouse_pass"
}
already_done = check_if_file_ingested(checksum, pg_params, layer="bronze")
# → True/False

# 3. Log kết quả
log_ingestion_to_postgres(
    file_path="/data/raw/booking/hotels.csv",
    file_checksum=checksum,
    records_ingested=10000,
    table_name="booking_hotels_list_raw",
    status="success",
    postgres_conn_params=pg_params,
    layer="bronze",
    ingestion_details={"tables": [{"name": "raw_table", "records": 10000}]},
    file_size_bytes=5242880
)
```

### PostgreSQL Table: `file_ingestion_log`

```sql
CREATE TABLE file_ingestion_log (
    id SERIAL PRIMARY KEY,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_size_bytes BIGINT,
    file_checksum TEXT NOT NULL,        -- MD5 hash
    layer TEXT NOT NULL,                 -- 'bronze'|'silver'|'gold'
    ingestion_timestamp TIMESTAMP,
    records_ingested INTEGER,
    table_name TEXT,
    status TEXT,                          -- 'success'|'failed'|'in_progress'
    error_message TEXT,
    ingestion_details JSONB,             -- Multi-table breakdown, metadata
    UNIQUE(file_checksum, layer)          -- Same file can exist in different layers
);
```

### Dedup Logic

```
file_checksum + layer → UNIQUE constraint
ON CONFLICT → UPDATE timestamp, records, status
```

Cùng 1 file có thể tồn tại ở bronze, silver, gold với status khác nhau.

### Đánh giá

- **MD5** — nhanh nhưng không an toàn (collision risk). Đủ cho dedup, nhưng SHA-256 tốt hơn
- **PostgreSQL params hardcode** ở mỗi Bronze job — nên centralize
- `calculate_file_checksum()` dùng `open(file_path, "rb")` — chỉ hoạt động với local files, không phải S3

---

## 4. `s3_utf8_uploader.py` — MinIO Upload (UTF-8 Safe)

### Chức năng
Upload file từ local filesystem lên MinIO/S3 với đảm bảo UTF-8 encoding. Dùng `mc cp` (MinIO Client CLI) thay vì Spark writer.

### Tại sao cần module này?

Spark `.csv()` / `.text()` writer không luôn preserve UTF-8 đúng cách cho tiếng Việt. `mc cp` stream trực tiếp, không load vào memory, giữ nguyên encoding.

### API

```python
from utils.s3_utf8_uploader import copy_file_to_bronze_with_utf8

result = copy_file_to_bronze_with_utf8(
    source_path="/data/raw/booking/vietnam_hotels_list.csv",
    bronze_bucket="bronze",
    source_type="booking_hotels_list",
    new_filename="vietnam_hotels_list_20251029_abc123.csv"
)
# → {"status": "success", "location": "s3a://bronze/lakehouse/booking_hotels_list/raw/...", "method": "minio_client_utf8"}
```

### Internal Flow

```
1. Verify file exists locally
2. Set MC_CONFIG_DIR=/tmp/.mc (writable in Spark container)
3. mc alias set minio http://minio:9000 minioadmin minioadmin123
4. mc cp /data/raw/file.csv minio/bronze/lakehouse/{type}/raw/{filename}
5. Return {status, location, method}
```

### Đánh giá

- **Stream mode** — không load file vào RAM, hiệu quả cho files lớn
- `mc` phải được pre-installed trong Spark Docker image (đã có trong Dockerfile)
- **subprocess.run()** — nếu `mc` fail, chỉ return `False`, không raise exception

---

## 5. `merge_utils.py` — MERGE/UPSERT Operations

### Chức năng
Cung cấp 3 hàm cho MERGE operations:
1. Tính row-level MD5 checksum
2. MERGE INTO Bronze (single business key)
3. MERGE INTO Gold dimension (composite business keys)

### API

#### 5.1. `calculate_row_checksum()`

```python
from utils.merge_utils import calculate_row_checksum

df = calculate_row_checksum(df, ["hotel_name", "hotel_url", "province"])
# → df thêm cột "row_checksum" = MD5(hotel_name|hotel_url|province)
```

Logic: concatenate tất cả business columns bằng `|`, NULL → empty string, rồi MD5 hash. Giúp phát hiện row nào thay đổi nội dung giữa 2 lần chạy.

#### 5.2. `merge_into_bronze()`

```python
from utils.merge_utils import merge_into_bronze

stats = merge_into_bronze(
    spark=spark,
    new_data_df=df_new,
    target_table="lakehouse.bronze.raw_hotels",
    business_key="hotel_url",                    # SINGLE key
    business_columns=["hotel_name", "province", "rating"]
)
# → {"inserted": 100, "updated": 5, "skipped": 895, "total_processed": 1000}
```

#### 5.3. `merge_into_gold_dim()`

```python
from utils.merge_utils import merge_into_gold_dim, print_merge_stats

stats = merge_into_gold_dim(
    spark=spark,
    new_data_df=df_new,
    target_table="gold.gold.dim_comment",
    business_keys=["post_url_nk", "stt"],        # COMPOSITE keys
    all_columns=["post_sk", "comment_text", "comment_level", "row_checksum", ...]
)
print_merge_stats(stats)
```

### MERGE Strategy (cả Bronze và Gold)

```sql
MERGE INTO target USING source
ON target.business_key = source.business_key

WHEN MATCHED AND target.row_checksum != source.row_checksum THEN
    UPDATE SET ...                    -- Data thay đổi → cập nhật

WHEN NOT MATCHED THEN
    INSERT (...)                     -- Record mới → thêm

-- (Implicit) WHEN MATCHED AND checksums equal → DO NOTHING (skip)
```

### Thống kê trước khi MERGE

Trước khi chạy SQL MERGE, module tính stats bằng Spark joins:
- **inserted** = LEFT ANTI JOIN (records không có trong target)
- **updated** = INNER JOIN where checksum khác
- **skipped** = total - inserted - updated

### Đánh giá

- **Tên hàm `merge_into_bronze` gây hiểu nhầm** — thực tế Silver cũng dùng (hotels_list, hotels_detail, tiktok_videos). Nên rename thành `merge_into_table` hoặc `merge_with_single_key`
- **Stats tính TRƯỚC MERGE** bằng joins → thêm I/O. Iceberg có `num-rows-updated` metric nhưng chưa được tận dụng
- Surrogate key (`comment_sk`) không bị update trong MERGE (giữ giá trị cũ) — đúng behavior
- `updated_at` luôn set `current_timestamp()` khi UPDATE — đúng SCD Type 1

---

## 6. `gold_job_logger.py` — Gold Layer Job Logger

### Chức năng
Class `GoldJobLogger` log success/failure của Gold layer jobs vào PostgreSQL `file_ingestion_log` table (cùng bảng với Bronze/Silver tracking).

### API

```python
from utils.gold_job_logger import get_gold_logger

logger = get_gold_logger(spark)

# Log thành công
logger.log_job_success(
    source_path="silver.silver.tiktok_post_metadata",
    table_name="gold.gold.dim_author",
    records_processed=1400,
    job_details={
        "job_type": "dimension",
        "execution_time_seconds": 45.2,
        "authors_with_name": 1200
    }
)

# Log thất bại
logger.log_job_failure(
    source_path="silver.silver.tiktok_post_metadata",
    table_name="gold.gold.dim_author",
    error_message="Connection timeout to Hive Metastore"
)
```

### Connection

`get_gold_logger()` factory tạo logger với connection mặc định:
- **JDBC URL**: `jdbc:postgresql://postgres:5432/metastore_db`
- **User**: `lakehouse_user` / `lakehouse_pass`

Class dùng **psycopg2** (direct SQL) thay vì Spark JDBC — vì Spark JDBC không support JSONB cast.

### Checksum Strategy

```python
def _calculate_run_checksum(self, table_name):
    date_str = datetime.now().strftime("%Y%m%d")
    return hashlib.md5(f"{table_name}_{date_str}".encode()).hexdigest()
```

Mỗi job/ngày tạo 1 checksum duy nhất. `ON CONFLICT (file_checksum, layer)` → nếu chạy lại cùng ngày, UPDATE thay INSERT.

### `ingestion_details` (JSONB)

Gold logger lưu thêm metadata chi tiết:
```json
{
    "job_type": "dimension",
    "execution_time_seconds": 45.2,
    "source_type": "silver_table",
    "authors_with_name": 1200,
    "completed_at": "2026-03-11T14:30:00"
}
```

### Đánh giá

- **Error swallowing** — cả `log_job_success` và `log_job_failure` đều `try/except` và chỉ print warning. Nếu PostgreSQL down, job vẫn "thành công" nhưng log bị mất
- **Credentials hardcode** trong `get_gold_logger()` — nên centralize
- **Daily granularity** — cùng job chạy 2 lần/ngày sẽ overwrite log của lần trước

---

## Tổng hợp: Credentials Hardcode

| Module | Credentials |
|---|---|
| `spark_session.py` | MinIO: `minioadmin` / `minioadmin123` |
| `s3_utf8_uploader.py` | MinIO: `minioadmin` / `minioadmin123` |
| `gold_job_logger.py` | PostgreSQL: `lakehouse_user` / `lakehouse_pass` |
| `file_tracker.py` | Nhận params từ caller (tốt hơn) |

**Gợi ý**: Tạo module `utils/config.py` centralize tất cả connection params:
```python
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "metastore_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "lakehouse_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "lakehouse_pass")
```

---

## Cấu trúc sử dụng theo Layer

### Bronze Layer
```python
# 1. Tính checksum file gốc
checksum = calculate_file_checksum(source_path)

# 2. Kiểm tra đã ingested chưa
if check_if_file_ingested(checksum, pg_params, layer="bronze"):
    print("Skip — already ingested")
    return

# 3. Upload lên MinIO (UTF-8 safe)
result = copy_file_to_bronze_with_utf8(source_path, "bronze", source_type, filename)

# 4. Validate bằng Spark (đọc lại từ S3, count rows)
df = spark.read.csv(s3_path)

# 5. Log vào PostgreSQL
log_ingestion_to_postgres(source_path, checksum, df.count(), table_name, "success", pg_params, "bronze")
```

### Silver Layer
```python
# Step 1: Transform (đọc Bronze CSV → Scratch Parquet)
# Step 2: Clean & Load
df = calculate_row_checksum(df, business_columns)

# MERGE (UPSERT) hoặc APPEND + LEFT ANTI JOIN
stats = merge_into_bronze(spark, df, target_table, business_key, columns)

# Log
log_ingestion_to_postgres(file_path, checksum, records, table_name, "success", pg_params, "silver")
```

### Gold Layer
```python
# 1. Create table
create_iceberg_table_if_not_exists(spark, "gold", "dim_province", schema, catalog="gold")

# 2. Transform + Write
df.writeTo("gold.gold.dim_province").overwritePartitions()

# 3. Log (via GoldJobLogger)
logger = get_gold_logger(spark)
logger.log_job_success(source, table, count, details)
```

---

**Last Updated**: March 11, 2026
