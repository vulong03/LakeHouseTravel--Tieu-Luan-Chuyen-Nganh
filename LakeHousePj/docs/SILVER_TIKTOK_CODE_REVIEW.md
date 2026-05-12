# Silver TikTok Comments — Code Review Report

> Tài liệu mô tả chi tiết hiện trạng code của pipeline `silver/tiktok_comments`, các vấn đề thiết kế / bug logic / dead code / hiệu năng / observability đã phát hiện, kèm vị trí dòng cụ thể và mức ưu tiên fix.

**Phạm vi**: 6 file trong `spark/jobs/silver/tiktok_comments/`
- `__init__.py`
- `config.py`
- `partition_utils.py`
- `step_01_transform.py`
- `step_02_clean_load.py`
- `file_processor.py`
- `batch_processor.py`

**Ngày review**: 2026-05-10

---

## 1. Tổng quan kiến trúc hiện tại

```
Bronze CSV (s3a://bronze/lakehouse/tiktok_comments/raw/*.csv)
        │
        ▼  step_01_transform.py
Scratch Parquet (s3a://scratch/.../run_YYYYMMDD_HHMMSS/)
   - tiktok_post_metadata (partition: post_url)
   - tiktok_post_comments (partition: post_url)
        │
        ▼  step_02_clean_load.py
Silver Iceberg
   - silver.silver.tiktok_post_metadata (partition: crawl_date)
   - silver.silver.tiktok_post_comments  (partition: scrape_date)
```

Pipeline đi đúng pattern Medallion 3 lớp, có tracking PostgreSQL ở cả 2 step.

---

## 2. Mâu thuẫn thiết kế (quan trọng nhất)

### 2.1 Step 1 docstring nói "NO data cleaning" nhưng thực tế DROP

`step_01_transform.py` line 15 ghi:

```
- NO deduplication, NO data cleaning (preserve Bronze as-is)
```

Nhưng `validate_posts_data` (line 422–445) và `validate_comments_data` (line 448–471) FILTER NULL ngay tại Scratch.

→ **Hệ quả**: Scratch không còn là "preserve Bronze as-is". Khi debug NULL ở Silver, không truy được nguyên nhân vì Scratch đã mất row.

### 2.2 `BUSINESS_COLUMNS_POSTS = ["post_url"]` → row_checksum mất ý nghĩa

`config.py` line 47–50:

```
BUSINESS_COLUMNS_POSTS = [
    "post_url"
]
```

Hệ quả:
- 2 lần crawl cùng `post_url` (likes khác, shares khác) → `row_checksum` GIỐNG NHAU.
- Step 2 line 748 anti-join theo `post_url` → bản crawl mới bị skip im lặng.
- Mọi update engagement metric sẽ mất.

→ Anti-join đó cũng không cần row_checksum (đang join trực tiếp theo `post_url`). Vậy `row_checksum` cho posts hiện tại là **noise field**, không phục vụ logic nào.

### 2.3 Import dead code

`step_02_clean_load.py` line 75–78:

```
from silver.tiktok_comments.file_processor import (
    create_batches,
    process_single_post_url
)
```

`process_single_post_url` được import nhưng KHÔNG bao giờ được gọi.
Tương tự, `process_posts_batch`, `process_comments_batch`, `read_partitions_batch` ở `batch_processor.py` được định nghĩa nhưng **không được sử dụng ở đâu cả**.

→ **~700 dòng dead code**. Maintainer mới có thể tưởng pipeline có 3 strategy song song.

### 2.4 `decode_partition_value` / `encode_partition_value` cũng dead code

`partition_utils.py` line 75–112 — 2 hàm này không có chỗ nào dùng.
Comment trong `encode_partition_value` đã thừa nhận:

```
This function kept for backward compatibility but returns the filter format.
```

---

## 3. Bug logic ngầm (gây mất data hoặc logic sai)

### 3.1 Mâu thuẫn giữa "Option A" và `filter_unprocessed_partitions`

`partition_utils.py` line 44–51 (Option A — chọn bản crawl mới nhất):

```
window_spec = Window.partitionBy("post_url").orderBy(F.desc("source_file_checksum"))
df_latest = df_all_combinations \
    .withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .select("post_url", "source_file_checksum")
```

Nhưng `filter_unprocessed_partitions` line 251–254:

```
if post_url in existing_post_urls:
    skipped_by_post_url += 1
    print(f"Skipping {post_url[:50]}... (already in Silver)")
    continue
```

→ Hai logic ĐÁ NHAU.
- Option A nói: "lấy bản crawl mới nhất".
- Filter nói: "post_url đã có trong Silver thì bỏ".

Kết quả: **mọi crawl mới của post cũ đều bị bỏ**, kể cả khi có comment mới.

### 3.2 Step 1 check tracking `layer='silver'` để quyết định ghi Scratch

`step_01_transform.py` line 499–504:

```
for file_path, file_name, scrape_timestamp in all_files:
    file_checksum = scrape_timestamp.replace('-', '').replace('T', '')
    if not check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
        unprocessed.append((file_path, file_name, scrape_timestamp))
```

→ Step 1 dựa vào tracking của layer SILVER để biết "file nào chưa xử lý".
- Nếu Step 2 chưa chạy → file vẫn được coi là "chưa xử lý" → Step 1 ghi lại vào Scratch run mới.
- Step 2 sau đó chỉ lấy run mới nhất → orphan run cũ + lãng phí I/O.

### 3.3 `parse_comment_time` drop column không tồn tại

`step_02_clean_load.py` line 336–337:

```
.drop("time", "_parts", "_day", "_month", "_year", "_absolute_date",
      "_relative_num", "_relative_unit", "_relative_date", "_scrape_date", "_recent_date")
```

Các cột `_parts, _day, _month, _year` KHÔNG ĐƯỢC TẠO trong hàm này. Spark sẽ silently ignore drop column không tồn tại — nhưng đây là dấu hiệu code copy-paste từ phiên bản cũ chưa dọn.

### 3.4 `BRONZE_FILE_PATTERN` capture thiếu

`config.py` line 43:

```
BRONZE_FILE_PATTERN = r'tiktok_comments_(?:optimized_)?(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})_\d{8}_\d{6}_[a-f0-9]{8}\.csv'
```

Phần `[a-f0-9]{8}` là MD5 8 ký tự đầu của Bronze (Bronze đã hash file thật). Nhưng pattern KHÔNG capture vào group → Silver Step 1 vứt bỏ luôn checksum thật, tự chế ra checksum giả từ scrape_timestamp (line 394, 501).

→ Mất lineage Bronze ↔ Silver theo identity thật.

---

## 4. Hiệu năng (đang chậm hơn cần thiết)

### 4.1 Step 2 count per file × 30 = O(900) Spark count action mỗi batch

`step_02_clean_load.py` line 859–881:

```
for item in batch_items:
    post_url = item["post_url"]
    ...
    posts_check = df_posts_cleaned.filter(F.col("post_url") == post_url).count()
    ...
    comments_for_file = df_comments_validated \
        .filter(F.col("post_url") == post_url) \
        .count()
```

→ 30 file × 2 count = 60 Spark action mỗi batch.
~92 batch = **~5,520 Spark count actions** chỉ để log số bản ghi.

→ Có thể thay bằng 1 lần `groupBy("post_url").count().collectAsMap()`.

### 4.2 Đọc lại Silver mỗi batch không cache

- `df_silver_posts` (line 747): đọc Silver để anti-join.
- `df_valid_posts` (line 818): đọc Silver lần nữa để validate orphan comments.

Cả 2 đều có thể `cache()` 1 lần khi vào Step 2.

### 4.3 Đọc full Scratch + filter mỗi batch

`step_02_clean_load.py` line 721–722, 793–794:

```
df_all_posts = spark.read.parquet(scratch_path_posts) \
    .filter(F.col("post_url").isin(batch_post_urls))
```

→ ~92 batch × (read posts + read comments) = nhiều I/O thừa.

### 4.4 `partitionBy("post_url")` ở Scratch — anti-pattern

`step_01_transform.py` line 599, 610:

```
.partitionBy("post_url") \
.parquet(output_path_posts)
```

- Mỗi `post_url` → 1 directory + 1 file Parquet rất nhỏ.
- 1.500 posts = 1.500 directories trên S3/MinIO.
- URL đặc biệt cần URL-encode → list/scan rất chậm.

→ Nên đổi sang `partitionBy("scrape_date")` hoặc không partition.

---

## 5. Vấn đề "silent NULL" — gốc rễ

### 5.1 `author = "N/A"` literal (đo được 448 row)

- `extract_value` (`step_01_transform.py` line 62–86): chỉ trim quote, KHÔNG normalize "N/A".
- `clean_and_transform_posts` (`step_02` line 350–422): KHÔNG hề chạm vào cột `author` — chỉ parse date/number.
- Kết quả: `"N/A"` đi nguyên vào Silver.

→ Cột `author`, `post_description`, `replied_to_tag_name` cần normalize ở Step 2.

### 5.2 `parse_crawl_time` chỉ 1 pattern duy nhất (fragile)

`step_02_clean_load.py` line 261–265:

```
.withColumn("crawl_time_parsed",
    F.to_timestamp(F.col("crawl_time_cleaned"), "EEE MMM dd yyyy HH:mm:ss")
)
```

Hiện tại 0 NULL — may mắn mọi file dùng locale `en_US` (`Sat Sep 27`).
Nếu hệ thống crawl đổi locale `vi_VN` (`T7 Th9 27`) → toàn bộ batch NULL ngay.

### 5.3 Không có `*_parse_status`

Các parser đều "fail silently → NULL". Không phân biệt được:
- NULL do nguồn TikTok thật sự không có giá trị.
- NULL do parser chưa cover format.
- NULL do row bị shift cột.

→ Khi NULL tăng, không biết debug hướng nào.

---

## 6. Bảng tóm tắt theo độ ưu tiên

### P0 — Bug logic gây mất data

| # | Vấn đề | File / dòng | Hành động |
|---|---|---|---|
| 1 | `BUSINESS_COLUMNS_POSTS` chỉ có `post_url` | `config.py` L48 | Bổ sung engagement metric vào checksum HOẶC bỏ checksum cho posts |
| 2 | "Option A" mâu thuẫn `filter_unprocessed_partitions` | `partition_utils.py` L251-254 | Bỏ check `post_url in existing_post_urls`, chỉ giữ check theo file checksum. Hoặc dùng MERGE thay anti-join. |
| 3 | `"N/A"` literal trong text fields | `step_02_clean_load.py` `clean_and_transform_posts` | Normalize `"N/A"` / `"---"` → NULL cho `author`, `post_description`, `replied_to_tag_name` |
| 4 | Step 1 check layer='silver' để biết file mới | `step_01_transform.py` L499-504 | Đổi sang check layer='bronze' (hoặc tracking riêng cho Scratch) |

### P0 — Dead code (giảm bẫy maintainer)

| # | Vấn đề | File / dòng | Hành động |
|---|---|---|---|
| 5 | `process_single_post_url` import nhưng không gọi | `step_02_clean_load.py` L75-78 | Xóa import |
| 6 | `process_posts_batch`, `process_comments_batch`, `read_partitions_batch` không ai dùng | `batch_processor.py` toàn file | Xóa file hoặc refactor Step 2 để dùng nó |
| 7 | `decode_partition_value`, `encode_partition_value` không ai dùng | `partition_utils.py` L75-112 | Xóa |
| 8 | Drop column không tồn tại `_parts, _day, _month, _year` | `step_02_clean_load.py` L336-337 | Xóa khỏi list drop |

### P1 — Hiệu năng

| # | Vấn đề | File / dòng | Hành động |
|---|---|---|---|
| 9 | 30 lần `count()` mỗi batch để log per-file | `step_02_clean_load.py` L859-881 | Thay bằng 1 lần `groupBy("post_url").count().collectAsMap()` |
| 10 | `df_silver_posts`, `df_valid_posts` không cache | `step_02_clean_load.py` L747, L818 | `.cache()` ở đầu run |
| 11 | `partitionBy("post_url")` ở Scratch | `step_01_transform.py` L599, L610 | Đổi sang `partitionBy("scrape_date")` hoặc không partition |

### P1 — Observability

| # | Vấn đề | Hành động |
|---|---|---|
| 12 | Không có `*_parse_status` | Thêm cờ cho `post_date`, `comment_date`, `crawl_time`, `parse_tiktok_number` |
| 13 | Không có quarantine table | Tạo `silver.silver._quarantine_tiktok_*` cho row bị filter (level_comment shift, comment empty, shares > 5M) |
| 14 | Step 1 ghi tracking nhưng dùng checksum giả | Sửa để dùng MD5 thật từ `BRONZE_FILE_PATTERN` (capture thêm group `[a-f0-9]{8}`) |

### P2 — Robustness

| # | Vấn đề | Hành động |
|---|---|---|
| 15 | `parse_crawl_time` chỉ 1 pattern locale | Thêm fallback locale (vi_VN) |
| 16 | `get_latest_scratch_run` chỉ lấy 1 run | Lặp tất cả run chưa track, không chỉ latest |
| 17 | Schema không explicit khi `createDataFrame` | Truyền schema vào `spark.createDataFrame(batch_posts, schema=...)` |

---

## 7. Mặt mạnh của code hiện tại

Để cân bằng, code có một số phần làm rất tốt:

- **Parser metadata theo anchor "Mô tả của bài đăng:"** (step_01 line 154–162): robust với multiline description.
- **Tìm CSV header theo full pattern matching** (step_01 line 270–281): tránh false positive khi description có chứa từ "STT,".
- **`parse_tiktok_number`** (step_02 line 121–168): xử lý đầy đủ K/M/N/A, dùng `coalesce` rõ ràng.
- **Tracking PostgreSQL per-file**: granular, dễ debug từng file.
- **Idempotent theo file checksum**: chạy lại không double-write.
- **Iceberg với partition `crawl_date` / `scrape_date`** ở Silver: tốt cho query downstream.
- **Validate orphan comment** trước khi append: 0 orphan như đo được.

---

## 8. Khuyến nghị thứ tự thực hiện

Mình đề xuất 3 sprint nhỏ, mỗi cái khoảng 0.5-1 ngày:

### Sprint 1 (1 ngày) — Sửa bug mất data + dọn dead code
- P0 #1, #2, #3, #4 (bug logic)
- P0 #5, #6, #7, #8 (dead code)
- → Có thể giảm ~700 dòng + chặn data loss.

### Sprint 2 (0.5 ngày) — Hiệu năng
- P1 #9, #10, #11
- → Step 2 nhanh hơn 5–10 lần.

### Sprint 3 (1 ngày) — Observability + Robustness
- P1 #12, #13, #14
- P2 #15, #16, #17
- → Khi production fail, biết ngay nguyên nhân.

---

## 9. Tham chiếu file

- Code: `spark/jobs/silver/tiktok_comments/`
- Config DQ: `spark/jobs/dataqualify/tiktok_post_*/`
- Báo cáo dữ liệu (đã đo): `docs/SILVER_TIKTOK_DATA_REPORT.md`
- Báo cáo gửi sếp ngắn: `docs/SILVER_TIKTOK_COMMENTS_STATUS_REPORT.md`

---

**Last updated**: 2026-05-10
**Người review**: AI assistant (đã đọc 6 file, kiểm chứng số liệu trên Iceberg Silver thực tế).
