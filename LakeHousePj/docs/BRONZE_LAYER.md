# Bronze Layer — Raw Data Ingestion

## Tổng quan

Bronze layer chịu trách nhiệm **copy nguyên bản** (raw, as-is) các file CSV từ thư mục local `/data/raw/` lên MinIO object storage (`s3://bronze/`), kèm theo:
- Versioning bằng **timestamp + checksum** trong tên file
- **Checksum-based deduplication**: skip file đã ingest (so MD5)
- **Tracking** mọi lần ingest vào bảng `file_ingestion_log` trên PostgreSQL

**Nguyên tắc**: Bronze KHÔNG transform, KHÔNG validate schema, KHÔNG thay đổi dữ liệu. Chỉ copy nguyên bản + ghi metadata.

---

## Kiến trúc

```
/data/raw/                                     MinIO (s3://bronze/)
├── booking/                                   ├── lakehouse/
│   ├── vietnam_hotels_list.csv       ──────►  │   ├── booking_hotels_list/raw/*.csv
│   ├── vietnam_hotels_detail.csv     ──────►  │   ├── booking_hotels_detail/raw/*.csv
│   └── vietnam_hotels_reviews.csv    ──────►  │   ├── booking_hotels_reviews/raw/*.csv
└── tiktok/                                    │   ├── tiktok_videos/raw/*.csv
    ├── links/merged_videos.csv       ──────►  │   └── tiktok_comments/raw/*.csv
    └── comments/*.csv (6 files)      ──────►  │
                                               │
                                     PostgreSQL: file_ingestion_log (tracking)
```

---

## Danh sách Jobs

| Job | File | Source | Target | Đặc biệt |
|---|---|---|---|---|
| Booking Hotels List | `raw_ingest_booking_hotels_list.py` | `vietnam_hotels_list.csv` (~10K hotels) | `booking_hotels_list/raw/` | CSV thường |
| Booking Hotels Detail | `raw_ingest_booking_hotels_detail.py` | `vietnam_hotels_detail.csv` (~10K hotels) | `booking_hotels_detail/raw/` | multiLine CSV (descriptions dài) |
| Booking Hotels Reviews | `raw_ingest_booking_hotels_reviews.py` | `vietnam_hotels_reviews.csv` (~1M+ reviews) | `booking_hotels_reviews/raw/` | multiLine CSV (review text), file lớn |
| TikTok Videos | `raw_ingest_tiktok_videos.py` | `merged_videos.csv` (~8K videos) | `tiktok_videos/raw/` | CSV thường |
| TikTok Comments | `raw_ingest_tiktok_comments_batch.py` | `comments/*.csv` (6 files) | `tiktok_comments/raw/` | **Batch mode**: xử lý nhiều file, format đặc biệt (16-line metadata header + CSV data) |

---

## Luồng xử lý (mỗi file)

```
1. Tính MD5 checksum của file
      ↓
2. Kiểm tra file_ingestion_log (PostgreSQL)
   ├── Đã tồn tại → SKIP (return 'skipped')
   └── Chưa tồn tại → tiếp
      ↓
3. Đọc CSV bằng Spark (chỉ để validate + đếm records)
      ↓
4. Tạo filename mới: {base}_{YYYYMMDD_HHMMSS}_{checksum[:8]}.csv
      ↓
5. Upload file GỐC lên MinIO qua MinIO Client (mc cp)
   (không dùng Spark write — giữ nguyên encoding/format)
      ↓
6. Log vào PostgreSQL file_ingestion_log
      ↓
7. Return status (success/skipped/failed)
```

---

## Upload Strategy

Bronze layer dùng **MinIO Client (`mc cp`)** thay vì Spark DataFrame write. Lý do:
- Spark `.csv()` writer có thể phá multiLine format
- Spark `.text()` writer không đảm bảo UTF-8 encoding
- `mc cp` stream trực tiếp, giữ nguyên byte-for-byte

Utility: `utils/s3_utf8_uploader.py`

```python
# Bên trong sử dụng subprocess gọi mc:
subprocess.run(['mc', 'alias', 'set', 'minio', endpoint, access_key, secret_key])
subprocess.run(['mc', 'cp', local_file, f'minio/{bucket}/{key}'])
```

---

## Checksum Tracking (PostgreSQL)

### Bảng: `file_ingestion_log`

| Column | Type | Mô tả |
|---|---|---|
| `id` | SERIAL PK | Auto-increment |
| `file_path` | TEXT | Đường dẫn file gốc |
| `file_name` | TEXT | Tên file |
| `file_size_bytes` | BIGINT | Kích thước file (bytes) |
| `file_checksum` | TEXT | MD5 hash |
| `ingestion_timestamp` | TIMESTAMP | Thời điểm ingest |
| `records_ingested` | INTEGER | Số records |
| `table_name` | TEXT | Tên bảng target |
| `layer` | TEXT | 'bronze' / 'silver' / 'gold' |
| `status` | TEXT | 'success' / 'failed' / 'in_progress' |
| `error_message` | TEXT | Lỗi nếu fail |
| `ingestion_details` | JSONB | Metadata bổ sung |

**Unique constraint**: `(file_checksum, layer)` — cùng file không ingest 2 lần vào cùng layer.

**Upsert**: nếu re-ingest (cùng checksum + layer) → update timestamp + status.

Utility: `utils/file_tracker.py`

---

## Airflow Integration

### DAG: `bronze_raw_ingestion`

**Config**: `airflow/dags/bronze/config.py`

```
health_check → start_task → init_tracking_table
                                    ↓
                    ┌───────────────┼───────────────────┐
                    ↓               ↓                   ↓
            booking_list    booking_detail    booking_reviews   (parallel)
                    ↓                                   
            tiktok_videos → tiktok_comments              (sequential)
                    ↓               ↓                   ↓
                    └───────────────┼───────────────────┘
                                    ↓
                              complete_task
```

- **Booking** 3 jobs chạy **song song** (independent)
- **TikTok** 2 jobs chạy **tuần tự** (videos trước, comments sau)
- **Schedule**: `None` (manual trigger) — đổi sang `@daily` cho automation

### Cách trigger

```bash
docker exec lakehouse_airflow airflow dags trigger bronze_raw_ingestion
```

### Cách chạy từng job riêng lẻ

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=2g \
    /opt/spark/jobs/bronze/raw_ingest_booking_hotels_list.py \
    /data/raw/booking/vietnam_hotels_list.csv \
    bronze \
    booking_hotels_list
```

---

## Xử lý đặc biệt theo nguồn

### Booking Hotels Detail & Reviews: multiLine CSV

File detail/reviews chứa text dài (hotel descriptions, review text) có thể xuống dòng trong cùng 1 cell. Spark đọc với:

```python
spark.read \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv(source_path)
```

Tuy nhiên khi upload lên Bronze, vẫn dùng `mc cp` (copy nguyên bản) chứ không qua Spark write.

### TikTok Comments: Batch + Special Format

- **Batch mode**: quét toàn bộ `*.csv` trong thư mục `/data/raw/tiktok/comments/`
- **Format đặc biệt**: mỗi file có 16 dòng metadata header (thông tin post) + CSV data (comments) phía dưới
- Dùng `glob.glob()` để tìm file, xử lý từng file trong vòng lặp
- Mỗi file được track riêng trong PostgreSQL (mỗi file có checksum riêng)
- Có summary report cuối: success / skipped / failed count

---

## Đánh giá & Gợi ý cải thiện

### Điểm mạnh hiện tại

1. **Checksum deduplication** hoạt động tốt, tránh ingest trùng
2. **Direct file upload** (`mc cp`) giữ nguyên data integrity
3. **PostgreSQL tracking** có đầy đủ metadata cho audit
4. **Error handling** log lỗi vào PostgreSQL khi fail
5. **Command-line args** linh hoạt, override được paths

### Điểm cần cải thiện

#### 1. Code trùng lặp nhiều (DRY violation)

4 file Booking + TikTok Videos gần như giống nhau ~90%. Sự khác biệt duy nhất:
- Default `source_path` / `source_type`
- `multiLine` option (detail/reviews có, list/videos không)

**Gợi ý**: tạo 1 class `BronzeIngester` chung, mỗi job chỉ cần truyền config.

```python
# Thay vì 4 file gần giống nhau:
class BronzeIngester:
    def __init__(self, source_path, bucket, source_type, multiline=False):
        ...
    def ingest(self):
        # Toàn bộ logic: checksum → check dup → validate → upload → log
        ...

# Mỗi job chỉ 5 dòng:
if __name__ == "__main__":
    BronzeIngester(
        source_path="/data/raw/booking/vietnam_hotels_list.csv",
        bucket="bronze",
        source_type="booking_hotels_list"
    ).ingest()
```

#### 2. Credentials hardcode

```python
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',      # Hardcode ở mỗi file
    'password': 'lakehouse_pass'   # Nên đưa vào env hoặc config chung
}
```

Lặp lại ở **mỗi file**. Nên đưa vào 1 file config chung hoặc đọc từ environment variables.

#### 3. Spark session chỉ dùng để validate (đếm records)

Bronze không transform data, chỉ dùng Spark để `df.count()` và lấy `df.columns`. Với file nhỏ (<100MB), có thể dùng Python thuần (`csv.reader`) thay vì khởi động cả Spark session — tiết kiệm 10-15 giây startup.

Tuy nhiên với file lớn (reviews 1M+ rows), Spark validate vẫn hợp lý.

#### 4. Error handling ở reviews job thiếu failure logging

`raw_ingest_booking_hotels_reviews.py` catch exception nhưng chỉ `raise` mà **không log vào PostgreSQL** (thiếu `log_ingestion_to_postgres` trong except block), khác với hotels_list và tiktok_videos.

#### 5. `__init__.py` cần cập nhật

```python
# Hiện tại liệt kê các jobs chưa đúng tên:
#    - ingest_booking_hotels.py       → thực tế: raw_ingest_booking_hotels_list.py
#    - ingest_booking_reviews.py      → thực tế: raw_ingest_booking_hotels_reviews.py
#    - ingest_tiktok_reviews.py       → thực tế: raw_ingest_tiktok_videos.py
```

#### 6. MD5 checksum nên dùng SHA-256

MD5 đã deprecated về mặt bảo mật. Dù ở đây chỉ dùng cho deduplication (không phải security), chuyển sang SHA-256 cho best practice chỉ cần đổi 1 dòng trong `file_tracker.py`.

#### 7. TikTok Comments batch không dùng Spark validate

`raw_ingest_tiktok_comments_batch.py` đếm tổng lines bằng Python (`len(f.readlines())`) thay vì dùng Spark. Đây là hợp lý vì file comments có format đặc biệt (16-line header), nhưng `records_ingested` sẽ = tổng lines (bao gồm header) chứ không phải số comments thật.

---

## Files liên quan

| File | Mô tả |
|---|---|
| `spark/jobs/bronze/raw_ingest_booking_hotels_list.py` | Ingest hotels list CSV |
| `spark/jobs/bronze/raw_ingest_booking_hotels_detail.py` | Ingest hotels detail CSV (multiLine) |
| `spark/jobs/bronze/raw_ingest_booking_hotels_reviews.py` | Ingest reviews CSV (multiLine, file lớn) |
| `spark/jobs/bronze/raw_ingest_tiktok_videos.py` | Ingest TikTok videos CSV |
| `spark/jobs/bronze/raw_ingest_tiktok_comments_batch.py` | Batch ingest TikTok comments (nhiều file) |
| `spark/jobs/utils/file_tracker.py` | Checksum calculation + PostgreSQL tracking |
| `spark/jobs/utils/s3_utf8_uploader.py` | Upload file lên MinIO với UTF-8 |
| `airflow/dags/bronze/dag.py` | DAG definition |
| `airflow/dags/bronze/config.py` | DAG config (paths, schedule, job definitions) |

---

**Last Updated**: March 11, 2026
