# TikTok Silver Layer — Tài Liệu Hoàn Chỉnh

> **Cập nhật lần cuối:** 2026-05-28  
> **Layer:** Silver (Cleaned & Transformed)  
> **Catalog:** `lakehouse` (Iceberg + Hive)

---

## Mục lục

1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Các bảng Silver TikTok](#2-các-bảng-silver-tiktok)
   - [2.1 tiktok_videos](#21-tiktok_videos)
   - [2.2 tiktok_post_metadata](#22-tiktok_post_metadata)
   - [2.3 tiktok_post_comments](#23-tiktok_post_comments)
3. [Pipeline: TikTok Videos](#3-pipeline-tiktok-videos)
4. [Pipeline: TikTok Comments](#4-pipeline-tiktok-comments)
5. [Data Quality Checks](#5-data-quality-checks)
6. [Các vấn đề đã biết & quyết định thiết kế](#6-các-vấn-đề-đã-biết--quyết-định-thiết-kế)
7. [Tracking & Audit Trail](#7-tracking--audit-trail)
8. [File Structure](#8-file-structure)

---

## 1. Tổng quan kiến trúc

```
Bronze (S3 raw CSV)
       │
       ▼
  Step 1: Transform
  (Bronze → Scratch Parquet)
       │
       ▼
  Step 2: Clean & Load
  (Scratch Parquet → Silver Iceberg)
       │
       ▼
Silver Iceberg Tables
  ├── lakehouse.silver.tiktok_videos
  ├── lakehouse.silver.tiktok_post_metadata
  └── lakehouse.silver.tiktok_post_comments
       │
       ▼
  Data Quality Checks (DQ)
  (Spark + PostgreSQL dq_results)
```

**Catalog:** `lakehouse` — Iceberg catalog tích hợp với Hive Metastore.  
**Storage:** MinIO (tương thích S3 API), bucket `s3a://silver/`.  
**Scratch bucket:** `s3a://scratch/pipeline/silver/` — lưu dữ liệu trung gian dạng Parquet.  
**Tracking:** PostgreSQL (`metastore_db`) — bảng `ingestion_log` và `dq_results`.

---

## 2. Các bảng Silver TikTok

### 2.1 `tiktok_videos`

**Tên đầy đủ:** `lakehouse.silver.tiktok_videos`  
**Mô tả:** Metadata các video TikTok thu thập được. Mỗi video là **1 record duy nhất** (UPSERT theo `url`).  
**Partition:** `region` (vùng miền Việt Nam)

#### Schema

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `url` | String | **NOT NULL** | URL TikTok (primary key / business key) |
| `posted_date` | Date | Yes | Ngày đăng video (`yyyy-MM-dd`) |
| `read_status` | Boolean | Yes | Trạng thái đã đọc (0→false, 1→true) |
| `keyword` | String | Yes | Từ khóa crawl |
| `ques_id` | String | Yes | ID câu hỏi liên quan |
| `target_type` | String | Yes | Loại mục tiêu crawl |
| `region` | String | Yes | Vùng miền (partition column) |
| `has_sub` | String | Yes | Có subtitle không |
| `row_checksum` | String | **NOT NULL** | Hash MD5 của business columns (change detection) |
| `ingestion_timestamp` | Timestamp | **NOT NULL** | Thời điểm ghi vào Silver |
| `source_file` | String | **NOT NULL** | Tên file Bronze nguồn |
| `source_file_checksum` | String | **NOT NULL** | Checksum của file Bronze nguồn |

> **Lưu ý:** Cột `vi_sub` (subtitle tiếng Việt) đã bị **DROP** vì toàn bộ giá trị là NULL.

#### Danh sách `region` hợp lệ

| Giá trị | Vùng miền |
|---------|-----------|
| `South_Central_Coast` | Duyên hải Nam Trung Bộ |
| `Mekong_Delta` | Đồng bằng sông Cửu Long |
| `Central_Highlands` | Tây Nguyên |
| `Northeast` | Đông Bắc |
| `North_Central_Coast` | Bắc Trung Bộ |
| `Northwest` | Tây Bắc |
| `Red_River_Delta` | Đồng bằng sông Hồng |
| `Southeast` | Đông Nam Bộ |

> **Tại sao có bước clean region?** Tên vùng miền trong Bronze CSV có thể chứa ký tự đặc biệt không hợp lệ cho S3 partition name (`+`, `/`, `\`, `:`...). Step 1 sẽ xóa những ký tự này.

---

### 2.2 `tiktok_post_metadata`

**Tên đầy đủ:** `lakehouse.silver.tiktok_post_metadata`  
**Mô tả:** Metadata của từng bài đăng TikTok (post). Mỗi bài là 1 record, load theo chiến lược **APPEND** (không UPSERT).  
**Partition:** `crawl_date` (ngày crawl)

#### Schema

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `post_url` | String | **NOT NULL** | URL bài đăng TikTok |
| `author` | String | **NOT NULL** | Tên người đăng |
| `author_tag` | String | Yes | Tag TikTok (`@username`) |
| `author_url` | String | Yes | URL trang cá nhân tác giả |
| `post_date` | Date | Yes | Ngày đăng bài (Date type, có thể NULL) |
| `post_description` | String | Yes | Mô tả bài đăng (có thể nhiều dòng) |
| `likes` | Integer | Yes | Số lượt thích |
| `comments_count` | Integer | Yes | Số comment hiển thị trên TikTok |
| `saves` | Integer | Yes | Số lượt lưu |
| `shares` | Integer | Yes | Số lượt chia sẻ |
| `comments_level1` | Integer | Yes | Số comment cấp 1 (bình luận trực tiếp) |
| `comments_level2` | Integer | Yes | Số comment cấp 2 (phản hồi) |
| `comments_loaded` | Integer | Yes | Số comment tool thực tế crawl được |
| `comments_displayed_tiktok` | Integer | Yes | Số comment TikTok hiển thị (ground truth) |
| `comments_difference` | Integer | Yes | Chênh lệch (`displayed - loaded`) |
| `crawl_time` | Timestamp | Yes | Thời điểm crawl (parsed từ `"Sat Sep 27 2025 00:52:04 GMT+0700"`) |
| `crawl_date` | Date | **NOT NULL** | Ngày crawl (partition column, extract từ `crawl_time`) |
| `scrape_timestamp` | String | Yes | Timestamp thô từ tên file |
| `row_checksum` | String | **NOT NULL** | Hash của business columns |
| `ingestion_timestamp` | Timestamp | **NOT NULL** | Thời điểm ghi vào Silver |
| `source_file` | String | Yes | Tên file Bronze nguồn |
| `source_file_checksum` | String | Yes | Checksum file nguồn |
| `source_file_size_bytes` | Long | Yes | Kích thước file nguồn (bytes) |

> **Business key:** `post_url` (dùng để tính `row_checksum`)

#### Lưu ý về `post_date`

`post_date` có thể NULL hợp lệ trong các trường hợp:
- Bài đăng mới — TikTok hiển thị dạng tương đối (`"2 giờ trước"`, `"vừa xong"`) và pipeline chuyển đổi relative→absolute không thành công.
- Sau khi parse relative date không khớp, sẽ thử **enrich từ `tiktok_videos.posted_date`** bằng `LEFT JOIN` trên `post_url = url`.
- Threshold DQ: **NULL ≤ 15%** mới PASS (ngưỡng nới lỏng, không FAIL cứng).

---

### 2.3 `tiktok_post_comments`

**Tên đầy đủ:** `lakehouse.silver.tiktok_post_comments`  
**Mô tả:** Dữ liệu comment của từng bài đăng TikTok. Một bài có thể có nhiều comment. Load theo chiến lược **APPEND**.  
**Partition:** `scrape_date` (ngày scrape)

#### Schema

| Cột | Kiểu | Nullable | Mô tả |
|-----|------|----------|-------|
| `post_url` | String | **NOT NULL** | URL bài đăng (foreign key → `tiktok_post_metadata`) |
| `stt` | String | Yes | Số thứ tự comment trong file crawl |
| `ten` | String | **NOT NULL** | Tên người bình luận |
| `tag_ten` | String | Yes | Tag TikTok của người bình luận |
| `url` | String | Yes | URL profile người bình luận |
| `comment` | String | **NOT NULL** | Nội dung bình luận |
| `comment_date` | Date | Yes | Ngày bình luận (Date type) |
| `likes` | String | Yes | Số like của comment |
| `level_comment` | String | Yes | Cấp comment: `"Yes"` (cấp 1) / `"No"` (cấp 2) |
| `replied_to_tag_name` | String | Yes | Tag người được reply (nếu là reply) |
| `number_of_replies` | String | Yes | Số lượng phản hồi |
| `scrape_timestamp` | String | Yes | Timestamp crawl thô |
| `scrape_date` | Date | **NOT NULL** | Ngày crawl (partition column) |
| `row_checksum` | String | **NOT NULL** | Hash của business columns |
| `ingestion_timestamp` | Timestamp | **NOT NULL** | Thời điểm ghi vào Silver |
| `source_file` | String | Yes | Tên file Bronze nguồn |
| `source_file_checksum` | String | Yes | Checksum file nguồn |
| `source_file_size_bytes` | Long | Yes | Kích thước file (bytes) |

> **Business columns** (dùng tính `row_checksum`): `post_url`, `stt`, `ten`, `comment`, `comment_date`

---

## 3. Pipeline: TikTok Videos

### Tổng quan

```
Bronze: s3a://bronze/lakehouse/tiktok_videos/raw/
        merged_videos_YYYYMMDD_HHMMSS_<checksum>.csv
                │
    ┌───────────┴───────────┐
    │     Step 1: Transform  │  (Bronze → Scratch)
    └───────────┬───────────┘
                │
    ┌───────────┴───────────┐
    │  Step 2: Clean & Load │  (Scratch → Silver MERGE/UPSERT)
    └───────────┬───────────┘
                │
    Silver: lakehouse.silver.tiktok_videos
```

### Step 1: Transform (Bronze → Scratch)

**File:** `spark/jobs/silver/tiktok_videos/step_01_transform.py`

1. **Tìm file Bronze mới nhất** theo timestamp trong tên file (`merged_videos_YYYYMMDD_HHMMSS_<checksum>.csv`).
2. **Kiểm tra tracking** — nếu `file_checksum` đã có trong PostgreSQL (`ingestion_log`, layer='silver') thì **skip**.
3. **Đọc CSV** (header=true, encoding=UTF-8, inferSchema=false — tất cả là String).
4. **Validate NOT NULL** trên cột `url` — drop các record NULL url.
5. **Clean region** — xóa ký tự đặc biệt không hợp lệ với S3 partition name.
6. **Thêm metadata columns**: `source_file`, `source_file_checksum`, `source_file_size_bytes`, `extraction_timestamp`.
7. **Ghi Parquet** vào Scratch, partition by `region`, path: `s3a://scratch/pipeline/silver/tiktok_videos/run_YYYYMMDD_HHMMSS/`.

### Step 2: Clean & Load (Scratch → Silver)

**File:** `spark/jobs/silver/tiktok_videos/step_02_clean_load.py`

1. **Đọc Scratch run mới nhất** (folder `run_*` sort theo tên).
2. **Drop cột `vi_sub`** (toàn NULL, không có giá trị).
3. **Trim whitespace** trên tất cả String columns.
4. **Parse `posted_date`** (String → DateType):

   ```
   Priority (coalesce):
   M/d/yyyy    →  6/9/2025, 9/3/2024    [format chính trong merged_videos.csv]
   MM/dd/yyyy  →  11/30/2024
   M/dd/yyyy   →  6/30/2024
   MM/d/yyyy   →  11/3/2024
   yyyy-MM-dd  →  2025-04-17
   M-d-yyyy    →  6-9-2025  (dash, fallback)
   MM-dd-yyyy  →  11-30-2024
   M-dd-yyyy   →  6-30-2024
   MM-d-yyyy   →  11-3-2024
   d/M/yyyy    →  9/6/2025  (Vietnamese style, last resort)
   dd/MM/yyyy  →  30/11/2024
   d-M-yyyy    →  9-6-2025
   dd-MM-yyyy  →  30-11-2024
   ```

   > **Quan trọng:** Format trong Bronze CSV hiện tại là **US slash** (`M/d/yyyy`). NULL sau parse = trường trống thực sự trong CSV gốc (không phải lỗi parse).

5. **Convert `read_status`** (String → Boolean): `"1"/"true"/"True"` → `true`, `"0"/"false"/"False"` → `false`, else → `null`.
6. **Replace empty string → NULL** cho các optional fields: `keyword`, `ques_id`, `target_type`, `has_sub`.
7. **Tính `row_checksum`** (MD5 của business columns).
8. **MERGE vào Silver** (UPSERT): so sánh `url` → UPDATE nếu checksum thay đổi, INSERT nếu mới, SKIP nếu không đổi.
9. **Log vào PostgreSQL** (`ingestion_log`) — không crash nếu log lỗi.

---

## 4. Pipeline: TikTok Comments

### Tổng quan

```
Bronze: s3a://bronze/lakehouse/tiktok_comments/raw/
        tiktok_comments_[optimized_]YYYY-MM-DDTHH-MM-SS_YYYYMMDD_HHMMSS_<checksum>.csv
                │          (mỗi file = 1 bài đăng TikTok)
    ┌───────────┴───────────┐
    │     Step 1: Transform  │  (Bronze → Scratch Parquet, batch 30 files)
    └───────────┬───────────┘
                │
    ┌───────────┴───────────┐
    │  Step 2: Clean & Load │  (Scratch Parquet → Silver APPEND, batch 30 files)
    └───────────┬───────────┘
                │
    Silver:
      ├── lakehouse.silver.tiktok_post_metadata
      └── lakehouse.silver.tiktok_post_comments
```

### Định dạng file Bronze

Mỗi file CSV Bronze có **cấu trúc đặc biệt** (không phải CSV thông thường):

```
Line 1:   Thời gian cào: Sat Sep 27 2025 00:52:04 GMT+0700 (Indochina Time)
Line 2:   Post URL: https://www.tiktok.com/@username/video/12345
Line 3:   Người đăng: Tên tác giả
Line 4:   Tag người đăng: @username
Line 5:   URL người đăng: https://www.tiktok.com/@username
Line 6:   Thời gian đăng: 8-9-2025
Line 7:   Số lượt tym: 36.6K
Line 8:   Số lượt comment: 855
Line 9:   Số lượt lưu: 1.2K
Line 10:  Số lượt share: 234
Line 11:  Mô tả của bài đăng: Nội dung mô tả bài đăng
          (có thể nhiều dòng)
...       [tiếp tục mô tả nếu multiline]
Line N:   Số bình luận cấp 1: 823
Line N+1: Số bình luận cấp 2: 32
Line N+2: Tổng số bình luận thực tế đã load: 855
Line N+3: Số bình luận TikTok hiển thị: 855
Line N+4: Chênh lệch số bình luận: 0
Line N+5: STT,Tên,Tag tên,URL,Comment,Time,Likes,Level Comment,Replied To Tag Name,Number of Replies
Line N+6: 1,Tên A,@tena,...,nội dung comment,...
...       [các dòng comment]
```

> **Vấn đề phức tạp:** `Mô tả của bài đăng` có thể **nhiều dòng** → header CSV không nằm cố định ở dòng 17. Pipeline dùng **dynamic header detection** (tìm dòng bắt đầu bằng `STT,Tên,Tag tên,...`).

### Step 1: Transform (Bronze → Scratch)

**File:** `spark/jobs/silver/tiktok_comments/step_01_transform.py`

1. **List tất cả file Bronze** (không chỉ file mới nhất — khác với tiktok_videos).
2. **Filter unprocessed**: check PostgreSQL tracking theo `file_checksum = scrape_timestamp` (đã xử lý → skip).
3. **Process từng file** (driver — không distribute):
   - Đọc file dạng text lines.
   - **Parse metadata** (dòng 1 đến trước `STT,...`) bằng sequential parsing với 2 anchors: `"Mô tả của bài đăng:"` và `"Số bình luận cấp 1:"`.
   - **Parse comments** (từ dòng header `STT,...` trở đi) bằng Python `csv.DictReader`.
   - Bỏ record nếu `post_url` là NULL (critical field).
4. **Batch processing** (30 files/batch) để tránh OOM:
   - Thu thập data vào list Python → `spark.createDataFrame()`.
   - Validate NOT NULL (`post_url` cho posts, `post_url`+`comment` cho comments).
   - **Ghi Parquet** vào Scratch, partition by `post_url`, mode `overwrite` (batch 1) / `append` (batch 2+).
5. **Output:** 2 Scratch folders:
   - `s3a://scratch/pipeline/silver/tiktok_post_metadata/run_YYYYMMDD_HHMMSS/`
   - `s3a://scratch/pipeline/silver/tiktok_post_comments/run_YYYYMMDD_HHMMSS/`

### Step 2: Clean & Load (Scratch → Silver)

**File:** `spark/jobs/silver/tiktok_comments/step_02_clean_load.py`

Pipeline xử lý **2 luồng song song**: Posts và Comments.

#### Luồng Posts (`tiktok_post_metadata`)

1. **Đọc Scratch** posts từ `run_*` mới nhất.
2. **Parse `post_date`** (String → DateType):
   - Absolute: `dd-MM-yyyy`, `d-M-yyyy`, `yyyy-MM-dd`
   - Relative: `"X ngày trước"`, `"X tuần trước"`, `"X tháng trước"`, `"X giờ trước"`, `"X phút trước"` → tính lại từ `scrape_timestamp`
   - Rất gần: `"vừa xong"` → ngày của `scrape_timestamp`
3. **Enrich `post_date` NULL** từ `silver.silver.tiktok_videos`:
   ```python
   LEFT JOIN tiktok_videos ON post_url = url
   post_date = COALESCE(post_date, tiktok_videos.posted_date)
   ```
4. **Parse `crawl_time`** (String → Timestamp): xử lý format `"Sat Sep 27 2025 00:52:04 GMT+0700 (Indochina Time)"` → xóa phần `GMT...`, parse `"EEE MMM dd yyyy HH:mm:ss"`.
5. **Extract `crawl_date`** từ `crawl_time` (partition column).
6. **Convert metrics** (String → Integer): xử lý format TikTok số học:
   - `"36.6K"` → `36600`
   - `"22K"` → `22000`
   - `"1.5M"` → `1500000`
   - `"1234"` → `1234`
   - `"N/A"` / null → `NULL`
7. **Tính `row_checksum`**.
8. **APPEND** vào `lakehouse.silver.tiktok_post_metadata`.
9. **Log** từng file vào PostgreSQL.

#### Luồng Comments (`tiktok_post_comments`)

1. **Đọc Scratch** comments.
2. **Parse `comment_date`** (String → DateType): cùng logic absolute + relative như `post_date`.
3. **Tính `row_checksum`**.
4. **APPEND** vào `lakehouse.silver.tiktok_post_comments`.
5. **Log** vào PostgreSQL.

#### Deduplication Strategy (Option A)

Khi cùng 1 `post_url` bị crawl nhiều lần → **chỉ giữ file MỚI NHẤT** (theo `scrape_timestamp`). File cũ bị skip qua tracking database. Giả định: crawl mới nhất có đầy đủ dữ liệu nhất (comment cũ + comment mới).

---

## 5. Data Quality Checks

### Kiến trúc DQ

- **Engine:** PySpark (đọc Silver Iceberg table, kiểm tra từng rule).
- **Storage:** PostgreSQL `dq_results` (lưu kết quả từng check).
- **Shared utilities:** `spark/jobs/dataqualify/dq_utils.py` — dùng chung cho tất cả DQ scripts.

### PostgreSQL `dq_results` Schema

```sql
CREATE TABLE dq_results (
    id                  SERIAL PRIMARY KEY,
    run_timestamp       TIMESTAMP,
    table_name          TEXT,
    check_name          TEXT,
    check_category      TEXT,         -- Completeness / Validity / Uniqueness / Integrity / Distribution
    status              TEXT,         -- PASS / WARN / FAIL
    is_critical         BOOLEAN,      -- TRUE → exit(1) nếu FAIL
    metric_value        DOUBLE,       -- Giá trị đo được
    threshold_value     DOUBLE,       -- Ngưỡng so sánh
    details             JSONB         -- Thông tin bổ sung
);
```

### 5.1 DQ: `tiktok_videos`

**Script:** `spark/jobs/dataqualify/tiktok_videos/tiktok_videos_dq_check.py`

| Check | Category | Critical | Threshold | Logic |
|-------|----------|----------|-----------|-------|
| `min_records` | Completeness | ✅ | ≥ 100 records | `total >= 100` |
| `null_url` | Completeness | ✅ | 0% NULL | `NULL url = 0` |
| `null_keyword` | Completeness | ✅ | 0% NULL | `NULL keyword = 0` |
| `null_region` | Completeness | ✅ | 0% NULL | `NULL region = 0` |
| `null_row_checksum` | Completeness | ✅ | 0% NULL | `NULL checksum = 0` |
| `null_ingestion_timestamp` | Completeness | ✅ | 0% NULL | `NULL ts = 0` |
| `null_source_file` | Completeness | ✅ | 0% NULL | `NULL source = 0` |
| `url_format` | Validity | ✅ | 0 invalid | `url.contains("tiktok.com")` |
| `region_consistency` | Consistency | ✅ | 0 invalid | `region IN (8 vùng miền)` |
| `duplicate_url` | Uniqueness | ✅ | ≤ 1.0% | `(total - distinct_url) / total * 100` |
| `future_posted_date` | Validity | ⚠️ WARN | 0 | `posted_date > current_date()` |

### 5.2 DQ: `tiktok_post_metadata`

**Script:** `spark/jobs/dataqualify/tiktok_post_metadata/tiktok_post_metadata_dq_check.py`

| Check | Category | Critical | Threshold | Ghi chú |
|-------|----------|----------|-----------|---------|
| `min_records` | Completeness | ✅ | ≥ 100 records | |
| `null_post_url` | Completeness | ✅ | ≤ 0.1% | |
| `null_author` | Completeness | ✅ | ≤ 0.1% | |
| `null_crawl_date` | Completeness | ✅ | ≤ 0.1% | |
| `null_row_checksum` | Completeness | ✅ | ≤ 0.1% | |
| `null_ingestion_timestamp` | Completeness | ✅ | ≤ 0.1% | |
| `null_post_date` | Completeness | ⚠️ WARN | ≤ 15% | NULL hợp lệ (relative date + coverage limit) |
| `null_likes_opt` | Completeness | ⚠️ WARN | ≤ 20% | Optional metrics |
| `null_comments_count_opt` | Completeness | ⚠️ WARN | ≤ 20% | Optional metrics |
| `null_saves_opt` | Completeness | ⚠️ WARN | ≤ 20% | Optional metrics |
| `null_shares_opt` | Completeness | ⚠️ WARN | ≤ 20% | Optional metrics |
| `negative_likes` | Validity | ✅ | 0 | `likes >= 0` |
| `negative_comments_count` | Validity | ✅ | 0 | `comments_count >= 0` |
| `negative_saves` | Validity | ✅ | 0 | `saves >= 0` |
| `negative_shares` | Validity | ✅ | 0 | `shares >= 0` |
| `url_format` | Validity | ✅ | 0 | `post_url.contains("tiktok.com")` |
| `duplicate_url` | Uniqueness | ✅ | ≤ 1.0% | |
| `metrics_stats` | Distribution | ⚠️ Info | — | avg_likes, avg_shares |

### 5.3 DQ: `tiktok_post_comments`

**Script:** `spark/jobs/dataqualify/tiktok_post_comments/tiktok_post_comments_dq_check.py`

| Check | Category | Critical | Threshold | Ghi chú |
|-------|----------|----------|-----------|---------|
| `min_records` | Completeness | ✅ | ≥ 1,000 records | |
| `null_post_url` | Completeness | ✅ | ≤ 0.1% | |
| `null_comment` | Completeness | ✅ | ≤ 0.1% | |
| `null_ten` | Completeness | ✅ | ≤ 0.1% | |
| `null_scrape_date` | Completeness | ✅ | ≤ 0.1% | |
| `null_row_checksum` | Completeness | ✅ | ≤ 0.1% | |
| `null_ingestion_timestamp` | Completeness | ✅ | ≤ 0.1% | |
| `empty_comments` | Validity | ⚠️ WARN | 0 | `LENGTH(TRIM(comment)) > 0` |
| `level_enum_validity` | Validity | ✅ | 0 invalid | `level_comment IN ('Yes','No') AND NOT NULL` |
| `orphan_comments` | Integrity | ⚠️ WARN | ≤ 5% | `post_url NOT IN tiktok_post_metadata` |

> **Bug đã fix:** `~isin()` trong Spark trả về `NULL` khi cột là `NULL` → phải dùng `isNull() | ~isin(...)` để bắt cả NULL và giá trị không hợp lệ.

---

## 6. Các vấn đề đã biết & quyết định thiết kế

### 6.1 `posted_date` NULL trong `tiktok_videos`

**Vấn đề:** Một số record có `posted_date = NULL` trong Silver.  
**Nguyên nhân:** Trường `posted_date` thực sự trống trong file Bronze CSV (`merged_videos.csv`) — không phải lỗi parse.  
**Quyết định:** Giữ NULL, không filter. DQ WARN nếu > 15%.

### 6.2 `post_date` NULL trong `tiktok_post_metadata`

**Vấn đề:** Nhiều bài TikTok được crawl khi vừa đăng → TikTok hiển thị `"2 giờ trước"` thay vì ngày cụ thể.  
**Pipeline giải quyết:**
1. Convert relative date (`"2 giờ trước"` → ngày dựa trên `scrape_timestamp`).
2. Nếu vẫn NULL → `LEFT JOIN tiktok_videos.posted_date` (enrich).
3. Nếu cả 2 đều NULL → giữ NULL.  

**DQ threshold:** ≤ 15% (WARN, không FAIL) — không nên FAIL cứng vì có NULL hợp lệ.

### 6.3 Level Comment ENUM

**Schema từ crawl tool:** Cột `level_comment` nhận giá trị `"Yes"` hoặc `"No"`.  
- `"Yes"` = comment cấp 1 (bình luận trực tiếp vào bài đăng)  
- `"No"` = comment cấp 2 (reply của reply)

**Bug fix (Spark quirk):** `~isin(["Yes", "No"])` trả về `NULL` nếu giá trị là `NULL` (không phải `True`). Phải dùng:
```python
F.col("level_comment").isNull() | ~F.col("level_comment").isin(["Yes", "No"])
```

### 6.4 Metrics format TikTok (`K`, `M`)

TikTok hiển thị số lớn theo format rút gọn:  
- `"36.6K"` = 36,600  
- `"1.5M"` = 1,500,000  
- `"N/A"` = không có thông tin → `NULL`

Pipeline dùng `parse_tiktok_number()` với regex để handle các case này.

### 6.5 Multiline `post_description`

Mô tả bài đăng TikTok có thể chứa xuống dòng, làm cho CSV header không ở dòng cố định.  
**Giải pháp:** Dùng 2 anchor strings để xác định vị trí description, và tìm header CSV (`STT,...`) **động** bằng pattern matching.

### 6.6 Catalog name

**Đúng:** `lakehouse` (không phải `silver`, không phải `hive_metastore`)  
**Pattern truy vấn:** `spark.table("lakehouse.silver.tiktok_videos")`  
**Xác nhận từ log Airflow:** `INFO:utils.spark_session: Catalog: lakehouse (Iceberg + Hive)`

---

## 7. Tracking & Audit Trail

### PostgreSQL `ingestion_log`

Mỗi file Bronze sau khi load thành công được ghi vào bảng `ingestion_log`:

| Field | Mô tả |
|-------|-------|
| `file_path` | Đường dẫn đầy đủ file Bronze trên S3 |
| `file_checksum` | Checksum từ tên file (dùng để skip duplicate) |
| `records_ingested` | Số record được insert/update |
| `table_name` | Tên Silver table đích |
| `status` | `"success"` / `"failed"` |
| `layer` | `"silver"` |
| `ingestion_details` | JSON: merge_stats, cleaning_stats, scratch_run |
| `file_size_bytes` | Kích thước file Bronze |

### Flow kiểm tra Skip

```
Step 1 khởi động
    → Check PostgreSQL: file_checksum IN ingestion_log (layer='silver')?
        → YES: Skip (đã xử lý rồi)
        → NO: Tiến hành transform
```

---

## 8. File Structure

```
spark/jobs/
├── silver/
│   ├── tiktok_videos/
│   │   ├── __init__.py
│   │   ├── config.py              ← Cấu hình: paths, business key, partitions
│   │   ├── step_01_transform.py   ← Bronze → Scratch
│   │   └── step_02_clean_load.py  ← Scratch → Silver (UPSERT/MERGE)
│   │
│   └── tiktok_comments/
│       ├── __init__.py
│       ├── config.py              ← Cấu hình: 2 tables, paths, batch size
│       ├── step_01_transform.py   ← Bronze CSV parse → Scratch
│       ├── step_02_clean_load.py  ← Scratch → Silver (APPEND)
│       ├── batch_processor.py     ← Batch processing utilities
│       ├── file_processor.py      ← Per-file processing logic
│       └── partition_utils.py     ← Scratch partition listing & mapping
│
└── dataqualify/
    ├── dq_utils.py                ← Shared: ensure_dq_table, write_dq_result
    ├── tiktok_videos/
    │   └── tiktok_videos_dq_check.py
    ├── tiktok_post_metadata/
    │   └── tiktok_post_metadata_dq_check.py
    └── tiktok_post_comments/
        └── tiktok_post_comments_dq_check.py
```

### Cấu hình quan trọng

| Pipeline | Key Config | Giá trị |
|----------|-----------|---------|
| tiktok_videos | `BUSINESS_KEY` | `url` |
| tiktok_videos | `PARTITION_COLUMNS` | `["region"]` |
| tiktok_videos | Bronze pattern | `merged_videos_YYYYMMDD_HHMMSS_<8hex>.csv` |
| tiktok_comments | `BATCH_SIZE` | `30` files/batch |
| tiktok_comments | `PARTITION_COLUMNS_POSTS` | `["crawl_date"]` |
| tiktok_comments | `PARTITION_COLUMNS_COMMENTS` | `["scrape_date"]` |
| tiktok_comments | Bronze pattern | `tiktok_comments_[optimized_]YYYY-MM-DDTHH-MM-SS_YYYYMMDD_HHMMSS_<hex>.csv` |

---

## Phụ lục: Quan hệ giữa 3 bảng

```
tiktok_videos
  └── url  ─────────────────────────────────────────┐
                                                     │ (enrich post_date)
tiktok_post_metadata                                 │
  ├── post_url ◄── JOIN ────────────────────────────-┘
  └── post_url ─────────────────────────────────────┐
                                                     │ (foreign key)
tiktok_post_comments                                 │
  └── post_url ◄── JOIN (orphan check DQ) ──────────┘
```

- `tiktok_videos.url` → `tiktok_post_metadata.post_url`: enrich `post_date` bị NULL.
- `tiktok_post_metadata.post_url` → `tiktok_post_comments.post_url`: referential integrity (DQ kiểm tra orphan với threshold 5% WARN).

---

*Tài liệu này được tổng hợp từ source code pipeline và được cập nhật lần cuối: 2026-05-28.*
