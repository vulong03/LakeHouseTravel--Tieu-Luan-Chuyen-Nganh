# Silver Layer — Data Cleaning & Transformation

## Tổng quan

Silver layer chịu trách nhiệm **làm sạch, chuẩn hoá và chuyển đổi** dữ liệu từ Bronze CSV thành các bảng Iceberg có schema rõ ràng, sẵn sàng cho phân tích. Mỗi pipeline gồm **2 bước**:

1. **Step 1 — Transform**: Bronze CSV → Scratch Parquet (validate cơ bản, thêm metadata)
2. **Step 2 — Clean & Load**: Scratch Parquet → Silver Iceberg (cleaning, type casting, dedup, MERGE/APPEND)

**Scratch bucket** (`s3://scratch/`) đóng vai trò vùng đệm trung gian — giúp tách biệt đọc Bronze và ghi Silver, dễ debug, có thể re-run từng step.

---

## Kiến trúc tổng thể

```
Bronze CSV (s3://bronze/)              Scratch Parquet (s3://scratch/)           Silver Iceberg (s3://silver/)
                                                                                
booking_hotels_list/raw/*.csv  ──►  pipeline/silver/hotels_list/run_*/   ──►  silver.silver.hotels_list
booking_hotels_detail/raw/*.csv ──► pipeline/silver/hotels_detail/run_*/ ──►  silver.silver.hotels_detail
booking_hotels_reviews/raw/*.csv──► pipeline/silver/hotels_reviews/run_*/──►  silver.silver.hotels_reviews
tiktok_videos/raw/*.csv        ──►  pipeline/silver/tiktok_videos/run_*/ ──►  silver.silver.tiktok_videos
tiktok_comments/raw/*.csv      ──►  pipeline/silver/tiktok_post_metadata/──► silver.silver.tiktok_post_metadata
                                    pipeline/silver/tiktok_post_comments/──► silver.silver.tiktok_post_comments
```

---

## Danh sách Pipelines

### 1. Hotels List (`hotels_list/`)

| Thuộc tính | Giá trị |
|---|---|
| Source | `booking_hotels_list/raw/*.csv` |
| Target | `silver.silver.hotels_list` |
| Business Key | `hotel_url` |
| Partition | `province` |
| Strategy | **MERGE (UPSERT)** — UPDATE nếu checksum thay đổi, INSERT nếu mới |
| Cleaning | `"0"` và `""` → NULL, trim whitespace, normalize province (Huế→Thừa Thiên Huế, Vũng Tàu→Bà Rịa Vũng Tàu), validate URLs, remove duplicates |
| Quality Rules | Max 5% null, min 100 records, max 1% duplicate |

**Schema Silver:**

| Column | Type | Nullable | Mô tả |
|---|---|---|---|
| hotel_name | STRING | No | Tên khách sạn |
| hotel_url | STRING | No | URL Booking.com (business key) |
| province | STRING | No | Tỉnh (đã chuẩn hoá) |
| row_checksum | STRING | No | MD5 hash business columns |
| ingestion_timestamp | TIMESTAMP | No | Thời điểm ingest |
| source_file | STRING | No | Tên file Bronze nguồn |
| source_file_checksum | STRING | No | Checksum file nguồn |

---

### 2. Hotels Detail (`hotels_detail/`)

| Thuộc tính | Giá trị |
|---|---|
| Source | `booking_hotels_detail/raw/*.csv` |
| Target | `silver.silver.hotels_detail` |
| Business Key | `hotel_url` |
| Partition | `province` |
| Strategy | **MERGE (UPSERT)** |
| Cleaning | multiLine CSV support, `rating_score` String→Double, `review_count_text`→`review_count` (extract số), flatten `rating_breakdown` (dict→string), trim amenities, drop `activities` column, normalize province |

**Schema Silver:**

| Column | Type | Nullable | Mô tả |
|---|---|---|---|
| hotel_name | STRING | No | Tên khách sạn |
| hotel_url | STRING | No | URL (business key) |
| province | STRING | No | Tỉnh |
| description | STRING | Yes | Mô tả khách sạn |
| top_amenities | STRING | Yes | Tiện ích chính (cleaned) |
| rating_score | DOUBLE | Yes | Điểm đánh giá (parsed) |
| review_count | INT | Yes | Số lượng review (extracted) |
| rating_breakdown | STRING | Yes | Điểm chi tiết (flattened) |
| row_checksum | STRING | No | MD5 hash |
| ingestion_timestamp | TIMESTAMP | No | |
| source_file | STRING | No | |
| source_file_checksum | STRING | No | |

---

### 3. Hotels Reviews (`hotels_reviews/`)

| Thuộc tính | Giá trị |
|---|---|
| Source | `booking_hotels_reviews/raw/*.csv` |
| Target | `silver.silver.hotels_reviews` |
| Business Key | 12 business columns (row-level checksum) |
| Partition | Không partition (tránh data skew) |
| Strategy | **APPEND + LEFT ANTI JOIN** — chỉ append records có `row_checksum` chưa tồn tại |
| Cleaning | parse `review_date` (tiếng Việt "ngày DD tháng MM năm YYYY"→DateType), `review_score` String→Double, `stay_date` ("tháng MM/YYYY"→DateType), clean text (lowercase, remove HTML/URLs), filter NULL trên 7 cột quan trọng |

**Đặc biệt:**
- Reviews dùng **APPEND** thay vì MERGE (vì không có single business key)
- Deduplication bằng **LEFT ANTI JOIN trên `row_checksum`** (MD5 của 12 business columns)
- Hỗ trợ **batch mode** với `--year` và `--month` arguments để xử lý theo tháng

**Schema Silver:**

| Column | Type | Nullable | Mô tả |
|---|---|---|---|
| hotel_name | STRING | No | Tên khách sạn |
| hotel_url | STRING | Yes | URL |
| reviewer_name | STRING | Yes | Tên người đánh giá |
| reviewer_country | STRING | Yes | Quốc gia |
| room_type | STRING | Yes | Loại phòng |
| stay_date | DATE | Yes | Tháng lưu trú (day=1) |
| traveler_type | STRING | Yes | Loại khách |
| review_date | DATE | Yes | Ngày đánh giá (parsed) |
| review_title | STRING | Yes | Tiêu đề (cleaned) |
| review_score | DOUBLE | Yes | Điểm (parsed) |
| review_positive | STRING | Yes | Nhận xét tích cực (cleaned) |
| review_negative | STRING | Yes | Nhận xét tiêu cực (cleaned) |
| row_checksum | STRING | No | MD5 hash 12 business columns |
| ingestion_timestamp | TIMESTAMP | No | |
| source_file | STRING | No | |
| source_file_checksum | STRING | No | |
| source_file_size_bytes | LONG | No | |

---

### 4. TikTok Videos (`tiktok_videos/`)

| Thuộc tính | Giá trị |
|---|---|
| Source | `tiktok_videos/raw/*.csv` |
| Target | `silver.silver.tiktok_videos` |
| Business Key | `url` |
| Partition | `region` |
| Strategy | **MERGE (UPSERT)** |
| Cleaning | `posted_date` String→Date (multi-format: M-d-yyyy, MM-dd-yyyy, yyyy-MM-dd), `read_status` 0/1→Boolean, drop `vi_sub` (full NULL), trim whitespace, empty→NULL |

**Schema Silver:**

| Column | Type | Nullable | Mô tả |
|---|---|---|---|
| url | STRING | No | URL video TikTok (business key) |
| posted_date | DATE | Yes | Ngày đăng (parsed) |
| read_status | BOOLEAN | Yes | Trạng thái đọc |
| keyword | STRING | Yes | Keyword tìm kiếm |
| ques_id | STRING | Yes | ID câu hỏi |
| target_type | STRING | Yes | Loại target |
| region | STRING | Yes | Vùng/tỉnh |
| has_sub | STRING | Yes | Có phụ đề? |
| row_checksum | STRING | No | MD5 hash |
| ingestion_timestamp | TIMESTAMP | No | |
| source_file | STRING | No | |
| source_file_checksum | STRING | No | |

---

### 5. TikTok Comments (`tiktok_comments/`)

Pipeline phức tạp nhất — tách 1 Bronze file thành **2 Silver tables**:
- **tiktok_post_metadata**: thông tin bài đăng (16 dòng metadata đầu)
- **tiktok_post_comments**: bình luận (CSV data từ dòng 18+)

| Thuộc tính | Giá trị |
|---|---|
| Source | `tiktok_comments/raw/*.csv` (6 files, format đặc biệt) |
| Target 1 | `silver.silver.tiktok_post_metadata` |
| Target 2 | `silver.silver.tiktok_post_comments` |
| Partition (posts) | `crawl_date` |
| Partition (comments) | `scrape_date` |
| Strategy | **APPEND + batch processing** (30 files/batch) |
| Dedup | Option A: giữ file mới nhất cho mỗi `post_url` |

**Đặc biệt:**
- **Format file đặc biệt**: 16 dòng metadata header (key-value) + CSV comments phía dưới
- **Multiline description**: field "Mô tả của bài đăng" có thể nhiều dòng → parser dùng anchor-based sequential parsing
- **Batch processing** (30 files/batch): giảm Iceberg append operations từ ~2,830 → ~92 (96.7% reduction)
- **TikTok number parsing**: "36.6K"→36600, "1.5M"→1500000, "N/A"→NULL
- **Relative date parsing**: "5 ngày trước", "2 tuần trước", "3 tháng trước" → DateType
- **Continue on batch failure**: batch fail không ảnh hưởng batch khác

**Schema Posts (tiktok_post_metadata):**

| Column | Type | Nullable | Mô tả |
|---|---|---|---|
| post_url | STRING | No | URL bài đăng (PK) |
| author | STRING | Yes | Tên hiển thị |
| author_tag | STRING | Yes | Username (@) |
| author_url | STRING | Yes | URL trang cá nhân |
| post_date | DATE | Yes | Ngày đăng (DD-MM-YYYY→Date) |
| post_description | STRING | Yes | Mô tả/caption |
| likes | INT | Yes | Số lượt thích |
| comments_count | INT | Yes | Số comment hiển thị |
| saves | INT | Yes | Số lượt lưu |
| shares | INT | Yes | Số lượt chia sẻ |
| comments_level1 | INT | Yes | Comment cấp 1 |
| comments_level2 | INT | Yes | Comment cấp 2 (reply) |
| comments_loaded | INT | Yes | Số comment crawl được |
| comments_displayed_tiktok | INT | Yes | Số comment thực |
| comments_difference | INT | Yes | Chênh lệch |
| crawl_time | TIMESTAMP | Yes | Timestamp lúc crawl |
| crawl_date | DATE | Yes | Date (partition key) |
| scrape_timestamp | STRING | Yes | ISO8601 timestamp |
| row_checksum | STRING | No | MD5 hash |
| ingestion_timestamp | TIMESTAMP | No | |
| source_file | STRING | No | |
| source_file_checksum | STRING | No | |
| source_file_size_bytes | LONG | No | |

**Schema Comments (tiktok_post_comments):**

| Column | Type | Nullable | Mô tả |
|---|---|---|---|
| post_url | STRING | No | FK → tiktok_post_metadata |
| stt | INT | Yes | Số thứ tự comment |
| ten | STRING | Yes | Tên người comment |
| tag_ten | STRING | Yes | Username |
| url | STRING | Yes | URL trang cá nhân |
| comment | STRING | No | Nội dung bình luận |
| comment_date | DATE | Yes | Ngày comment (mixed format parsed) |
| likes | INT | Yes | Số lượt thích comment |
| level_comment | STRING | Yes | "Yes"=reply, "No"=direct |
| replied_to_tag_name | STRING | Yes | Username được reply |
| number_of_replies | INT | Yes | Số reply con |
| scrape_timestamp | STRING | Yes | |
| scrape_date | DATE | Yes | (partition key) |
| row_checksum | STRING | No | MD5 hash |
| ingestion_timestamp | TIMESTAMP | No | |
| source_file | STRING | No | |
| source_file_checksum | STRING | No | |
| source_file_size_bytes | LONG | No | |

---

## Luồng xử lý chi tiết

### Step 1: Transform (Bronze → Scratch)

```
1. Tìm file Bronze mới nhất (sort by timestamp trong filename)
      ↓
2. Kiểm tra PostgreSQL tracking (đã xử lý ở Silver chưa?)
   ├── Đã xử lý → SKIP
   └── Chưa → tiếp
      ↓
3. Đọc CSV (encoding UTF-8, multiLine nếu cần)
      ↓
4. Validate NOT NULL (lọc bỏ records có NULL ở cột quan trọng)
      ↓
5. Thêm metadata columns (source_file, checksum, size, timestamp)
      ↓
6. Ghi ra Scratch bucket dạng Parquet
   (path: s3://scratch/pipeline/silver/{table}/run_{YYYYMMDD_HHMMSS}/)
```

### Step 2: Clean & Load (Scratch → Silver)

```
1. Tìm run folder mới nhất trong Scratch
      ↓
2. Đọc Parquet từ Scratch
      ↓
3. Áp dụng cleaning rules (tuỳ từng pipeline):
   - Type casting (String → Date/Double/Int/Boolean)
   - Text cleaning (lowercase, remove HTML/URLs)
   - Province normalization
   - Invalid value → NULL
   - Trim whitespace
      ↓
4. Tính row_checksum (MD5 hash business columns)
      ↓
5. Deduplication:
   ├── MERGE pipelines: UPSERT dựa trên business key
   │   (UPDATE nếu checksum thay đổi, INSERT nếu mới, SKIP nếu giống)
   └── APPEND pipelines: LEFT ANTI JOIN trên row_checksum
       (chỉ append records chưa tồn tại)
      ↓
6. Ghi vào Silver Iceberg table
      ↓
7. Log vào PostgreSQL file_ingestion_log (layer='silver')
```

---

## Chiến lược Deduplication

| Pipeline | Strategy | Key | Lý do |
|---|---|---|---|
| Hotels List | MERGE (UPSERT) | `hotel_url` | Có 1 business key rõ ràng |
| Hotels Detail | MERGE (UPSERT) | `hotel_url` | Có 1 business key rõ ràng |
| TikTok Videos | MERGE (UPSERT) | `url` | Có 1 business key rõ ràng |
| Hotels Reviews | APPEND + LEFT ANTI JOIN | `row_checksum` (12 cols) | Không có single business key, 1 hotel có nhiều reviews |
| TikTok Comments (posts) | APPEND + anti-join on `post_url` | `post_url` | Batch processing, giữ file mới nhất |
| TikTok Comments (comments) | APPEND + validate FK | `post_url` (FK) | Chỉ append comments có post tồn tại |

---

## Airflow Integration

### DAG: `silver_transformation`

**Config**: `airflow/dags/silver/config.py`

```
health_check → start_task
                    ↓
    ┌───────────────────────────────────┐
    │ PHASE 1 (Parallel - Light jobs)   │
    │                                   │
    │  hotels_list_step1 → step2        │
    │  hotels_detail_step1 → step2      │
    │  tiktok_videos_step1 → step2      │
    └───────────────────────────────────┘
                    ↓
    ┌───────────────────────────────────┐
    │ PHASE 2 (Sequential - Heavy jobs) │
    │                                   │
    │  hotels_reviews_step1 → step2     │
    │  tiktok_comments_step1 → step2    │
    └───────────────────────────────────┘
                    ↓
              complete_task
```

- **Phase 1**: Jobs nhẹ chạy song song (hotels_list, hotels_detail, tiktok_videos)
- **Phase 2**: Jobs nặng chạy tuần tự (hotels_reviews ~1M rows, tiktok_comments ~3K files)

---

## Đánh giá & Gợi ý cải thiện

### Điểm mạnh

1. **2-Step pattern** tách biệt Transform và Load — dễ debug, có thể re-run từng step
2. **Scratch bucket** làm vùng đệm — Bronze không bị ảnh hưởng nếu Silver fail
3. **Row-level checksum** phát hiện thay đổi chính xác, tránh update không cần thiết
4. **Batch processing** cho TikTok comments — giảm 96.7% Iceberg append operations
5. **PostgreSQL tracking** chi tiết: merge stats, cleaning stats, per-file logging
6. **Date parsing** mạnh: hỗ trợ tiếng Việt, relative dates, multiple formats

### Điểm cần cải thiện

#### 1. Hàm trùng lặp nhiều

`get_s3_file_size()`, `get_latest_bronze_file()`, `get_latest_scratch_run()`, `validate_data()` lặp lại ở **mỗi pipeline** với logic gần giống nhau. Nên đưa vào `utils/` chung.

#### 2. `get_latest_bronze_file()` có 2 cách implement khác nhau

- hotels_list / hotels_detail: dùng `spark.read.text()` + parse filename thủ công
- hotels_reviews / tiktok_videos / tiktok_comments: dùng `spark.read.format("binaryFile")` + regex pattern

Nên thống nhất 1 cách (recommend `binaryFile` + regex vì mạnh hơn).

#### 3. Hàm `merge_into_bronze()` đặt tên gây nhầm lẫn

Hàm trong `utils/merge_utils.py` có tên `merge_into_bronze` nhưng thực tế merge vào **Silver**. Nên đổi tên thành `merge_into_table()` hoặc `upsert_iceberg_table()`.

#### 4. POSTGRES_CONN hardcode và trùng lặp

Xuất hiện ở **mỗi config.py** + một số `step_02_clean_load.py` (tiktok_videos khai báo lại thay vì import từ config). Nên đưa vào 1 nơi duy nhất.

#### 5. Scratch bucket không tự dọn dẹp

Mỗi lần chạy tạo folder `run_YYYYMMDD_HHMMSS` mới trong Scratch nhưng **không bao giờ xóa** folders cũ. Lâu dần sẽ tích tụ dữ liệu trung gian không cần thiết.

**Gợi ý**: thêm step cuối trong DAG để xoá Scratch runs cũ hơn 7 ngày.

#### 6. TikTok Comments step_02 quá phức tạp (~1,100 dòng)

File `tiktok_comments/step_02_clean_load.py` chứa cả:
- Table creation (2 tables)
- TikTok number parsing
- Post date parsing
- Crawl time parsing
- Comment time parsing (mixed format)
- Posts cleaning
- Comments cleaning
- Batch processing orchestration
- PostgreSQL logging

Nên tách thành modules nhỏ hơn (đã có `partition_utils.py`, `file_processor.py`, `batch_processor.py` nhưng chưa dùng hết — `batch_processor.py` dường như là version cũ).

#### 7. `__init__.py` không phản ánh đúng code hiện tại

```python
# Liệt kê jobs sai tên:
#    - clean_hotels.py → thực tế: hotels_list/, hotels_detail/
#    - clean_reviews.py → thực tế: hotels_reviews/
#    - merge_reviews.py → không tồn tại
```

#### 8. Thiếu data validation metrics

Step 2 in ra thống kê cleaning (records removed, nulls found) nhưng **không lưu** metrics này vào PostgreSQL tracking một cách nhất quán. Hotels_list có `cleaning_stats` trong `ingestion_details`, nhưng format khác nhau giữa các pipelines.

#### 9. Hotels Reviews: LEFT ANTI JOIN hiệu suất kém khi Silver table lớn

Mỗi lần load, phải đọc toàn bộ `row_checksum` từ Silver table hiện có. Khi table vượt 10M+ records, anti-join sẽ chậm đáng kể.

**Gợi ý**: Sử dụng Iceberg snapshot-based approach hoặc bloom filter thay vì full table scan.

---

## Cấu trúc thư mục

```
spark/jobs/silver/
├── __init__.py
├── hotels_list/
│   ├── __init__.py
│   ├── config.py                    # Table metadata, paths, quality rules
│   ├── step_01_transform.py         # Bronze CSV → Scratch Parquet
│   └── step_02_clean_load.py        # Scratch → Silver (MERGE/UPSERT)
├── hotels_detail/
│   ├── __init__.py
│   ├── config.py
│   ├── step_01_transform.py
│   └── step_02_clean_load.py
├── hotels_reviews/
│   ├── __init__.py
│   ├── config.py
│   ├── step_01_transform.py
│   └── step_02_clean_load.py        # APPEND + LEFT ANTI JOIN
├── tiktok_videos/
│   ├── __init__.py
│   ├── config.py                    # Cleaning config (date format, boolean conversion)
│   ├── step_01_transform.py
│   └── step_02_clean_load.py
└── tiktok_comments/
    ├── __init__.py
    ├── config.py                    # 2 tables config, batch size, retry
    ├── step_01_transform.py         # Parse 16-line metadata + CSV, batch processing
    ├── step_02_clean_load.py        # Batch processing (30 files/batch), TikTok number parsing
    ├── partition_utils.py           # List partitions, dedup (keep latest), filter unprocessed
    ├── file_processor.py            # Per-file atomic processing + create_batches()
    └── batch_processor.py           # Batch read → union → clean → append (legacy?)
```

---

## Dependencies (Utility modules)

| Module | Chức năng | Dùng ở |
|---|---|---|
| `utils/spark_session.py` | Tạo SparkSession (Iceberg + S3/MinIO) | Tất cả |
| `utils/file_tracker.py` | `check_if_file_ingested()`, `log_ingestion_to_postgres()` | Tất cả |
| `utils/iceberg_utils.py` | `create_iceberg_table_if_not_exists()` | Step 2 (tạo table) |
| `utils/merge_utils.py` | `calculate_row_checksum()`, `merge_into_bronze()` | Step 2 (MERGE/UPSERT) |

---

**Last Updated**: March 11, 2026
