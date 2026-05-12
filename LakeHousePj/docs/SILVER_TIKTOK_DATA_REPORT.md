# Silver TikTok Data — Detailed Quality Report

> Tài liệu mô tả hiện trạng dữ liệu TikTok ở tầng Silver tại thời điểm đo đạc, bao gồm: schema, số lượng, chất lượng, lineage, vấn đề đã phát hiện và phân loại nguyên nhân (bug pipeline vs chính sách nguồn).
>
> **Snapshot ngày**: 2026-05-10 (theo `max(ingestion_timestamp)` = 2025-12-03 12:49:56)
> **Catalog**: `lakehouse.silver`
> **Bảng**: `tiktok_post_metadata`, `tiktok_post_comments`, `tiktok_videos`

---

## 1. Tổng quan

Silver chứa 3 bảng TikTok phục vụ pipeline NLP + DL forecasting:

| Bảng | Vai trò | Grain | Số dòng | Partition |
|---|---|---|---:|---|
| `tiktok_videos` | Nguồn URL crawl (input) | 1 row = 1 video URL | 8,417 | `region` |
| `tiktok_post_metadata` | Thông tin chi tiết bài viết đã crawl | 1 row = 1 post | 6,348 | `crawl_date` |
| `tiktok_post_comments` | Comments cấp 1 + cấp 2 | 1 row = 1 comment | 885,318 | `scrape_date` |

Quan hệ:

```
tiktok_videos (URL nguồn)
   │  url
   ▼  inner join 75.4%
tiktok_post_metadata (post)
   │  post_url
   ▼  one-to-many
tiktok_post_comments (comment)
```

---

## 2. Schema từng bảng

### 2.1 `tiktok_videos` (8,417 rows)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `url` | string | Khoá tự nhiên — URL TikTok video |
| `posted_date` | date | Ngày video đăng (parse từ raw) |
| `read_status` | boolean | Đã đọc/crawl hay chưa |
| `keyword` | string | Từ khoá crawl |
| `ques_id` | string | ID câu hỏi/khóa search |
| `target_type` | string | Loại mục tiêu |
| `region` | string | 1 trong 8 vùng Việt Nam (partition) |
| `has_sub` | string | Cờ phụ trợ |
| `row_checksum`, `ingestion_timestamp`, `source_file`, `source_file_checksum` | metadata lineage | |

### 2.2 `tiktok_post_metadata` (6,348 rows)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `post_url` | string | Khoá nghiệp vụ (unique) |
| `author`, `author_tag`, `author_url` | string | Thông tin tác giả |
| `post_date` | date | Ngày đăng (đã parse) |
| `post_description` | string | Caption + hashtag |
| `likes`, `comments_count`, `saves`, `shares` | int | Engagement metrics |
| `comments_level1`, `comments_level2` | int | Số comment theo cấp |
| `comments_loaded`, `comments_displayed_tiktok`, `comments_difference` | int | So sánh số comment crawler load được vs số TikTok hiển thị |
| `crawl_time`, `crawl_date` | timestamp / date | Thời điểm crawl (partition theo `crawl_date`) |
| `scrape_timestamp` | string | Timestamp dạng ISO từ tên file |
| `row_checksum`, `ingestion_timestamp`, `source_file`, `source_file_checksum`, `source_file_size_bytes` | metadata lineage | |

### 2.3 `tiktok_post_comments` (885,318 rows)

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `post_url` | string | FK đến `tiktok_post_metadata.post_url` |
| `stt` | int | Số thứ tự comment trong post |
| `ten`, `tag_ten`, `url` | string | Tên hiển thị / username / link người bình luận |
| `comment` | string | Nội dung bình luận |
| `comment_date` | date | Ngày bình luận (đã parse) |
| `likes` | int | Số tym của comment |
| `level_comment` | string | `Yes` = reply (level 2), `No` = comment gốc (level 1) |
| `replied_to_tag_name` | string | Username người được reply |
| `number_of_replies` | int | Số reply con |
| `scrape_timestamp`, `scrape_date` | string / date | Thời điểm scrape (partition theo `scrape_date`) |
| `row_checksum`, `ingestion_timestamp`, `source_file`, `source_file_checksum`, `source_file_size_bytes` | metadata lineage | |

---

## 3. Số liệu chất lượng đo được

### 3.1 `tiktok_post_comments`

| Chỉ tiêu | Giá trị | Nhận xét |
|---|---:|---|
| Tổng số dòng | 885,318 | |
| `post_url` NULL | 0 | OK |
| `comment` NULL hoặc rỗng | 0 | OK |
| `ten` NULL hoặc rỗng | 0 | OK |
| `level_comment` ngoài {Yes, No} | 0 | OK — không còn dấu hiệu CSV column-shift |
| `likes` NULL | 0 | OK |
| `comment_date` NULL | 746 (0.08%) | Parser fail nhỏ |
| `comment_date` ở tương lai | 0 | OK |
| Duplicate theo `row_checksum` | 0 | OK |
| Orphan (không có post_url tương ứng ở metadata) | 0 | FK nguyên vẹn |
| `distinct post_url` | 5,416 | |
| Range `comment_date` | 2019-03-15 → 2025-11-30 | |
| `max(ingestion_timestamp)` | 2025-12-03 12:49:56 | Freshness tốt |

### 3.2 `tiktok_post_metadata`

| Chỉ tiêu | Giá trị | Nhận xét |
|---|---:|---|
| Tổng số post | 6,348 | |
| `post_url` distinct | 6,348 | Unique 100% |
| `source_file` distinct | 6,348 | 1 post = 1 file |
| `post_url` NULL | 0 | OK |
| `author` NULL | 0 | **Cảnh báo**: 448 dòng có `author = "N/A"` (literal string, không phải NULL) |
| `crawl_time` NULL | 0 | OK |
| `likes`, `comments_count` NULL | 0 / 0 | OK |
| `post_date` NULL | 485 (7.64%) | Do nguồn TikTok ẩn, không phải parser fail |
| `saves` NULL | 71 (1.12%) | Nhẹ |
| **`shares` NULL** | **4,039 (63.6%)** | Do TikTok ngừng công khai số share |
| Future `post_date` | 0 | OK |
| Duplicate `row_checksum` | 0 | OK |
| Range `post_date` | 2019-03-14 → 2025-09-26 | |
| `post_description = "N/A"` (literal) | 6 | Cần normalize → NULL |

### 3.3 `tiktok_videos`

| Chỉ tiêu | Giá trị | Nhận xét |
|---|---:|---|
| Tổng số URL | 8,417 | |
| `url` distinct | 8,417 | OK |
| `url` không phải tiktok.com | 0 | OK |
| `region` NULL | 0 | OK |
| `keyword` NULL hoặc rỗng | 0 | OK |
| `posted_date` NULL | 526 (6.25%) | Cùng nguyên nhân với `post_date` |
| Duplicate `row_checksum` | 0 | OK |
| Range `posted_date` | 2019-03-14 → 2025-09-18 | |

#### Phân phối `region`:

| Region | Số video |
|---|---:|
| Mekong_Delta | 1,750 |
| Red_River_Delta | 1,451 |
| Northeast | 1,102 |
| South_Central_Coast | 1,092 |
| Northwest | 817 |
| North_Central_Coast | 790 |
| Southeast | 753 |
| Central_Highlands | 662 |

→ 8 vùng đầy đủ, balanced khá tốt.

---

## 4. Cross-table integrity

| Mối quan hệ | Số liệu |
|---|---:|
| `tiktok_videos` → `tiktok_post_metadata` (đã crawl chi tiết) | 6,348 / 8,417 = **75.4%** |
| Video URL chưa crawl chi tiết | 2,069 (24.6%) |
| Post không có comment nào | 932 / 6,348 = **14.7%** |
| Orphan comments (không có post tương ứng) | 0 |

---

## 5. Phân phối thời gian — Quan trọng cho DL

### 5.1 Bảng `tiktok_post_metadata` theo `post_date` (year-month)

| Giai đoạn | Số post/tháng | Đánh giá DL |
|---|---|---|
| 2019-03 → 2021-12 | 1–10 / tháng (rời rạc, có gap) | Không train được |
| 2022-01 → 2022-12 | 4–29 / tháng | Còn yếu |
| 2023-01 → 2023-06 | 9–23 / tháng | Yếu |
| 2023-07 → 2024-03 | 59–107 / tháng | Bắt đầu khả dụng |
| 2024-04 → 2025-09 | 107–887 / tháng | **Vùng vàng để train DL** |

→ Khuyến nghị **training window: 2023-07 → 2025-09** (~26 tháng có ≥50 post/tháng).

### 5.2 Phân phối NULL theo `crawl_date`

| Crawl YM | Total | NULL `shares` | % NULL shares | NULL `post_date` |
|---|---:|---:|---:|---:|
| 2025-09 | 461 | 13 | **2.8%** | 37 |
| 2025-10 | 298 | 78 | 26.2% | 67 |
| 2025-11 | 5,015 | 3,374 | 67.3% | 381 |
| 2025-12 | 574 | 574 | **100%** | 0 |

**Pattern rõ ràng**: Từ tháng 10/2025, TikTok dần dần ẩn `share count`, đến 12/2025 ẩn hoàn toàn.

---

## 6. Phân loại vấn đề (bug pipeline vs chính sách nguồn)

| # | Vấn đề | Số lượng | Loại | Hành động |
|---|---|---:|---|---|
| 1 | `shares` NULL trong metadata | 4,039 (63.6%) | **Source policy** — TikTok ẩn share count | Document + thêm cờ `shares_available`. Không nên dùng làm feature DL. |
| 2 | `post_date` NULL trong metadata | 485 (7.64%) | **Source policy** — Crawler không lấy được do TikTok ẩn | Có thể fallback từ comment_date sớm nhất. |
| 3 | `posted_date` NULL trong videos | 526 (6.25%) | **Source policy** | Tương tự #2. |
| 4 | `comment_date` NULL | 746 (0.08%) | Có thể parser fail format mới | Cần sample raw để xác minh. |
| 5 | `author = "N/A"` literal | 448 | **Pipeline bug** — chưa normalize "N/A" → NULL | Sửa ở `step_02_clean_load.py`. |
| 6 | `post_description = "N/A"` literal | 6 | **Pipeline bug** — như #5 | Sửa cùng patch với #5. |
| 7 | 24.6% video chưa crawl chi tiết | 2,069 | **Coverage gap** — crawler chưa chạy hết | Trigger backfill. |
| 8 | 14.7% post không có comment | 932 | Hỗn hợp — vừa do post thật không có, vừa do crawler load thiếu | Phân biệt qua `comments_displayed_tiktok` vs `comments_loaded`. |
| 9 | Pre-2023 quá thưa cho DL | — | **Source coverage** | Cắt training window. |

---

## 7. Mặt mạnh

- **Khoá nghiệp vụ và toàn vẹn**: 0 orphan, 0 duplicate theo checksum, `post_url` unique 100%.
- **Lineage đầy đủ**: mỗi row Silver có `source_file`, `source_file_checksum`, `source_file_size_bytes`, `ingestion_timestamp`.
- **Văn bản sạch**: `comment`, `ten` 0% NULL/empty — pipeline làm sạch văn bản tốt.
- **Không còn CSV column-shift sót**: `level_comment` 0 invalid.
- **Freshness tốt**: ingest đến 2025-12-03, gần thời điểm crawl mới nhất.
- **Region balanced**: 8 vùng đầy đủ trong `tiktok_videos`.

---

## 8. Mặt yếu

### 8.1 Pipeline-level

1. **Không phân biệt được "NULL do nguồn ẩn" vs "NULL do parser fail"**. Cả hai đều ra NULL → khó debug, khó alert.
2. **Lưu literal `"N/A"`/`"---"` thay vì NULL** cho cột text → làm méo distinct count, group by, join.
3. **Không có quarantine table** — row lỗi parse bị drop im lặng (hiện tại không có nhưng không có cách đo lường khi tương lai có).
4. **Không có cờ `*_parse_status`** — không biết NULL đến từ branch parser nào.

### 8.2 Source-level (ngoài tầm pipeline)

5. TikTok ẩn `share count` ngày càng nhiều (đến 12/2025: 100%).
6. TikTok ẩn `posted_date` cho ~7-8% post.
7. Coverage crawler còn 24.6% URL nguồn chưa crawl chi tiết.

### 8.3 ML/DL-level

8. Pre-2023-07: data quá thưa, không tạo được sequence DL.
9. Feature `shares` không còn đủ tin cậy cho training trên data 2025-Q4 trở đi.

---

## 9. Khuyến nghị hành động (theo độ ưu tiên)

### P0 — Fix ngay tuần này (pipeline)

1. **Normalize `"N/A"` và `"---"` → NULL** ở `step_02_clean_load.py`:
   - `author`, `post_description` (metadata)
   - `replied_to_tag_name` (comments — `"---"` literal khi không reply)
2. **Thêm cột `shares_available BOOLEAN`** ở Silver `tiktok_post_metadata` (hoặc compute on-the-fly khi build Gold).
3. **Document chính sách `shares` mới** trong `docs/GOLD_LAYER.md` để các bước Gold/ML không hiểu nhầm.

### P1 — Trong 1-2 tuần (observability + ML readiness)

4. **Thêm `*_parse_status`** cho các parser quan trọng (`post_date`, `comment_date`, `posted_date`, `parse_tiktok_number`).
5. **Tạo quarantine table** `silver.silver._quarantine_tiktok_*` để giữ row fail parse với cột `quarantine_reason`.
6. **Cắt training window** trong `fact_province_month_dl_features`: chỉ lấy `year_month >= 202307`.
7. **Bỏ feature `avg_shares_per_post`** khỏi DL feature set (hoặc dùng coalesce 0 + flag).

### P2 — Phối hợp crawler team

8. **Backfill 2,069 URL chưa crawl chi tiết**.
9. **Phân biệt 14.7% post không comment**: dùng `comments_displayed_tiktok > 0` để xác định post bị load thiếu vs post thực sự không có ai comment.
10. **Lưu thêm raw `share_text`** trong file gốc khi TikTok trả về N/A — để sau này đối chiếu với data từ TikTok API chính thức (nếu có).

---

## 10. Kết luận tổng quan

- **Silver đã sạch ở mức row-level**: NULL/empty/invalid của các cột text + FK gần như bằng 0.
- **Vấn đề lớn không phải pipeline bẩn**, mà là:
  1. **Pipeline thiếu observability** (NULL không phân loại nguyên nhân).
  2. **Source data thay đổi** (TikTok policy về share/post_date).
  3. **Coverage chưa đủ** (24.6% URL chưa crawl).
- **Pipeline có 2 bug nhỏ thực sự** cần fix ngay: literal "N/A" trong text fields.
- **Đối với DL**: chốt training window 2023-07 → 2025-09, bỏ `shares` feature, chấp nhận ~7% post không có `post_date`.

---

## 11. Tham chiếu

- File parser chính: `spark/jobs/silver/tiktok_comments/step_01_transform.py`, `step_02_clean_load.py`
- Config: `spark/jobs/silver/tiktok_comments/config.py`
- DQ checks hiện có: `spark/jobs/dataqualify/tiktok_post_metadata/`, `tiktok_post_comments/`, `tiktok_videos/`
- Feature ML downstream: `spark/jobs/gold/fact_dl_features/`
- Tài liệu trước đó: `docs/SILVER_TIKTOK_COMMENTS_STATUS_REPORT.md`, `docs/SILVER_LAYER.md`

---

**Last updated**: 2026-05-10
**Đo đạc bằng**: `docker exec lakehouse_spark_master /opt/spark/bin/spark-sql` (catalog `lakehouse.silver`, Iceberg via Hive metastore).
