# Gold Layer — Analytics & ML Feature Engineering

## Tổng quan

Gold layer xây dựng **mô hình Star Schema** với các bảng **Dimension** và **Fact** từ Silver, phục vụ:
- Phân tích kinh doanh (BI/analytics)
- Feature engineering cho ML training
- Chuẩn bị dữ liệu cho dự đoán "province hotness"

Gold sử dụng **Iceberg tables** với surrogate keys, SCD Type 1 metadata, và `GoldJobLogger` cho traceability.

---

## Mô hình Star Schema

```
                                    ┌─────────────────┐
                                    │   dim_province   │
                                    │  province_sk (PK)│
                                    │  province_name   │
                                    │  province_name_  │
                                    │    afterLaw      │
                                    │  region          │
                                    │  is_city         │
                                    └────────┬─────────┘
                                             │
    ┌──────────────┐    ┌──────────────┐     │     ┌──────────────────┐
    │  dim_author   │    │   dim_date    │     │     │    dim_hotel      │
    │ author_sk(PK) │    │ date_sk (PK)  │     │     │  hotel_sk (PK)   │
    │ author_tag    │    │ full_date     │     │     │  province_sk(FK) │
    │ author_name   │    │ year/month/   │     │     │  hotel_url       │
    │ author_url    │    │ quarter/week  │     │     │  rating_score    │
    └───────┬───────┘    └──────┬───────┘     │     └────────┬─────────┘
            │                   │             │              │
    ┌───────┴───────────────────┴─────────────┴──────────────┴───────────┐
    │                      FACT TABLES                                    │
    │                                                                     │
    │  fact_province_content_engagement (grain: 1 post)                   │
    │  fact_comment_nlp_engagement (grain: 1 comment, NLP features)       │
    │  fact_hotel_review_daily (grain: 1 review)                          │
    │  province_month_features (grain: province-month, ML training)       │
    └───────┬───────────────────┬──────────────────────┬─────────────────┘
            │                   │                      │
    ┌───────┴──────┐    ┌──────┴──────┐    ┌──────────┴─────────┐
    │  dim_post     │    │ dim_comment  │    │  dim_country        │
    │ post_sk (PK)  │    │ comment_sk   │    │  country_sk (PK)   │
    │ author_sk(FK) │    │ post_sk (FK) │    │  country_name      │
    │ province_sk   │    │ comment_text │    │  region             │
    │ keyword       │    │ comment_level│    └────────────────────┘
    └──────────────┘    └─────────────┘
                                           ┌──────────────────┐
    ┌──────────────────┐                   │  dim_destination  │
    │ dim_room_type     │                   │  destination_sk   │
    │ room_type_sk (PK) │                   │  province_sk (FK) │
    │ room_type_name    │                   │  destination_type │
    └──────────────────┘                   │  latitude/longitude│
    ┌──────────────────┐                   └──────────────────┘
    │ dim_travel_type   │
    │ traveler_type_sk  │
    │ traveler_type_name│
    └──────────────────┘
```

---

## Dimension Tables (10 bảng)

| # | Dimension | Source | Business Key | Records | Write Mode | FK Dependencies |
|---|---|---|---|---|---|---|
| 1 | `dim_province` | CSV (`list_of_provinces_of_vietnam.csv`) | `province_name` | 63 | OVERWRITE | — |
| 2 | `dim_date` | PostgreSQL `date_db.dim_date` (JDBC) | `date_sk` | ~3.6K | OVERWRITE | — |
| 3 | `dim_destination` | CSV (`List_Destination.csv`) | `destination_name + province_name` | ~200+ | OVERWRITE | `dim_province` |
| 4 | `dim_author` | `silver.tiktok_post_metadata` | `author_tag` | ~1.4K | OVERWRITE | — |
| 5 | `dim_hotel` | `silver.hotels_detail` | `hotel_url` | ~10K | OVERWRITE | `dim_province` |
| 6 | `dim_country` | `silver.hotels_reviews` (distinct) | `country_name` | ~100+ | OVERWRITE | — |
| 7 | `dim_room_type` | `silver.hotels_reviews` (distinct) | `room_type_name` | ~50+ | OVERWRITE | — |
| 8 | `dim_travel_type` | `silver.hotels_reviews` (distinct) | `traveler_type_name` | ~10 | OVERWRITE | — |
| 9 | `dim_post` | `silver.tiktok_post_metadata` + `silver.tiktok_videos` | `post_url` | ~1.4K | OVERWRITE | `dim_author`, `dim_province`, `dim_date` |
| 10 | `dim_comment` | `silver.tiktok_post_comments` | `post_url_nk + stt` | ~465K | MERGE (incremental) | `dim_post`, `dim_date` |

### Pattern chung của Dimension Jobs

```
1. Create Gold database (CREATE NAMESPACE IF NOT EXISTS)
2. Create Iceberg table (if not exists, schema + properties)
3. Load source data (CSV / JDBC / Silver table)
4. Validate & normalize (trim, filter NULL, remove duplicates)
5. Join with parent dimensions (FK lookup: province_sk, author_sk, date_sk...)
6. Transform to dimension format:
   - Surrogate key (row_number() OVER ORDER BY business_key)
   - SCD metadata: created_at, updated_at, is_active
7. Write to Gold Iceberg (OVERWRITE hoặc MERGE)
8. Validate results (count, sample, distribution)
9. Log to PostgreSQL via GoldJobLogger
```

### Đặc biệt theo từng Dimension

**dim_province**: Map `province_name_afterLaw` theo luật sáp nhập hành chính 2025 (63 tỉnh → 38 đơn vị mới). Đánh cờ `is_city` cho 5 thành phố trực thuộc trung ương.

**dim_date**: Đọc từ PostgreSQL qua JDBC, chứa 20 columns calendar (year, quarter, month, week, day_of_week, is_weekend, is_month_start...).

**dim_author**: Normalize `author_tag` (trim + bỏ prefix `@`). Dedup bằng window function giữ bản ghi mới nhất theo `crawl_time`.

**dim_hotel**: Join với `dim_province` để lấy `province_sk`. Chứa `rating_score` (Double), `review_count` (Int), `rating_breakdown` (flattened string).

**dim_country**: Map country_name → region bằng manual dict (~200 quốc gia). UDF dùng unicode normalization để matching chính xác (bỏ dấu, lowercase).

**dim_post**: Job phức tạp nhất — join 5 bảng: `tiktok_post_metadata` + `tiktok_videos` (INNER JOIN on `read_status=1`) + `dim_author` + `dim_province` (extract từ keyword "du lịch {province}") + `dim_date` (2 FK: crawl_date_sk, post_date_sk). Convert `has_sub` yes/no → Boolean.

**dim_comment**: Duy nhất dùng **MERGE incremental** thay vì OVERWRITE (vì volume lớn ~465K). Composite business key `(post_url_nk, stt)`. Convert `level_comment` Yes/No → Int 1/2.

**dim_room_type, dim_travel_type**: Simple dimensions — distinct values từ Silver, dedup, surrogate key.

**dim_destination**: Danh sách địa điểm du lịch curated (temple, beach, mountain...) với lat/long. Strict FK validation — fail nếu có province_name không match `dim_province`.

---

## Fact Tables (3 bảng + 1 ML aggregation)

### 1. `fact_hotel_review_daily`

| Thuộc tính | Giá trị |
|---|---|
| Source | `silver.hotels_reviews` |
| Grain | 1 row = 1 review |
| Write | **APPEND** |
| Partition | Không |
| FK Joins | `dim_hotel` (by hotel_url, fallback hotel_name), `dim_travel_type`, `dim_room_type`, `dim_country`, `dim_date` (stay_date) |

**Columns**: `fact_id`, `hotel_sk`, `country_sk`, `stay_date_sk`, `traveler_type_sk`, `room_type_sk`, `review_score`, `reviewer_name`, `review_title`, `review_positive`, `review_negative`

**Đặc biệt**: Hotel join dùng 2 bước — match by `hotel_url` trước, fallback by `hotel_name` nếu URL không match.

---

### 2. `fact_comment_nlp_engagement`

| Thuộc tính | Giá trị |
|---|---|
| Source | `gold.dim_comment` + `gold.dim_post` + `silver.tiktok_post_comments` |
| Grain | 1 row = 1 comment |
| Write | **MERGE** (incremental) |
| Partition | `province_sk` |
| NLP Engine | Pandas UDF + `underthesea` (sentiment) + `emoji` library |

**8 NLP Features được trích xuất:**

| Feature | Type | Mô tả |
|---|---|---|
| `word_count` | LONG | Số tokens (underthesea word_tokenize) |
| `unique_word_ratio` | DOUBLE | Unique tokens / total tokens |
| `exclamation_count` | LONG | Số dấu `!` |
| `sentiment_score` | DOUBLE | -1.0 (negative) → 0.0 (neutral) → 1.0 (positive) |
| `sentiment_label` | STRING | 'positive' / 'neutral' / 'negative' |
| `emoji_count` | LONG | Tổng số emoji |
| `positive_emoji_count` | LONG | Emoji tích cực (manual dict 60+ emojis) |
| `negative_emoji_count` | LONG | Emoji tiêu cực (manual dict 40+ emojis) |

**Text preprocessing**: remove URLs, remove @mentions, normalize whitespace, lowercase, truncate >5000 chars.

**Error handling**: mọi exception trong NLP → trả về giá trị neutral mặc định (không fail cả batch).

---

### 3. `fact_province_content_engagement`

| Thuộc tính | Giá trị |
|---|---|
| Source | `gold.dim_post` + `silver.tiktok_post_metadata` |
| Grain | 1 row = 1 post |
| Write | **OVERWRITE** (full refresh) |
| Partition | `province_sk` |

**Columns**: `post_sk`, `province_sk`, `date_sk`, `author_sk`, `likes`, `comments`, `comments_crawled`, `saves`, `shares`, `level1_comments`, `level2_comments`, `engagement_score`

**`engagement_score`** = likes + comments + saves + shares

**Đặc biệt**: Dedup post metrics bằng window function — giữ crawl mới nhất cho mỗi `post_url`.

---

### 4. `fact_province_month_dl_features` (ML Deep Learning — NEW)

| Thuộc tính | Giá trị |
|---|---|
| Source | `fact_comment_nlp_engagement` + `fact_province_content_engagement` + `fact_hotel_review_daily` |
| Grain | 1 row = 1 province x 1 month |
| Write | **OVERWRITE** (full refresh) |
| Partition | `year` |
| Records | ~3K province-months |
| Features | **~40 curated** (6 groups) |

**6 Feature Groups:**

| Group | Count | Features |
|---|---|---|
| Volume & Activity | 6 | total_posts, total_comments, total_hotel_reviews, unique_authors, comments_per_post, post_frequency |
| TikTok Engagement | 7 | avg/median/p90 likes, saves, shares, viral_post_ratio, engagement_score |
| NLP & Sentiment | 10 | avg_sentiment, sentiment_std, positive/negative_ratio, sentiment_polarity, word stats, emoji_sentiment_ratio, reply_ratio |
| Hotel / Booking | 7 | avg_hotel_score, hotel_score_std, high/low_score_ratio, unique_reviewer_countries, domestic_review_ratio |
| Temporal | 4 | month_sin, month_cos, is_peak_season, hotness_score (pre-computed) |
| Time-Series Lags | 6 | hotness_lag_1/2/3/12, hotness_rolling_3m, hotness_momentum |

**Khác biệt so với `province_month_features`:**
- Thêm STDDEV (sentiment_std, word_count_std, hotel_score_std) — distribution shape
- Thêm percentiles (median, p90) và viral_post_ratio — outlier detection
- Tích hợp sẵn hotel data (7 features) — không cần join inline trong ML
- Hotness score + lags + momentum tính sẵn — ML script chỉ cần read → scale → train
- reply_ratio, domestic_review_ratio, is_peak_season — domain features mới

---

### 5. `province_month_features` (ML Training — Legacy)

| Thuộc tính | Giá trị |
|---|---|
| Source | `fact_comment_nlp_engagement` + `fact_province_content_engagement` + `dim_date` + `dim_post` |
| Grain | 1 row = 1 province × 1 month |
| Write | **OVERWRITE** (full refresh) |
| Partition | `year` |
| Records | ~3K province-months |

**28 aggregated features** chia thành 6 nhóm:

| Nhóm | Features |
|---|---|
| Volume | `total_comments`, `total_posts` |
| NLP SUM | `total_words`, `total_exclamations`, `total_emojis`, `total_positive_emojis`, `total_negative_emojis` |
| NLP AVG | `avg_unique_word_ratio`, `avg_sentiment_score`, `avg_words_per_comment` |
| Sentiment Distribution | `positive_comments`, `neutral_comments`, `negative_comments`, `positive_ratio`, `negative_ratio` |
| Comment Engagement | `total_comment_likes`, `avg_comment_likes` |
| Post Engagement | `total_post_likes/saves/shares`, `avg_post_likes/saves/shares` |

**Đặc biệt**:
- Aggregate theo **POST date** (không phải comment date) — vì mỗi post thuộc 1 tháng cụ thể
- Filter bỏ tháng hiện tại (dữ liệu chưa đầy đủ)
- Export song song ra **Parquet** cho ML training: `s3://gold/ml_training/province_month_features.parquet`

---

## Airflow Integration

### DAG: `gold_aggregation`

**Config**: `airflow/dags/gold/config.py`

```
health_check → start_task
                    ↓
    ┌──────────────────────────────────────┐
    │ PHASE 1: Independent Dimensions      │
    │ (Parallel)                           │
    │                                      │
    │  dim_province                        │
    │  dim_date                            │
    │  dim_author                          │
    │  dim_country                         │
    │  dim_room_type                       │
    │  dim_travel_type                     │
    └──────────────────┬───────────────────┘
                       ↓
    ┌──────────────────────────────────────┐
    │ PHASE 2: Dependent Dimensions        │
    │ (Sequential)                         │
    │                                      │
    │  dim_hotel (needs dim_province)       │
    │  dim_destination (needs dim_province) │
    │  dim_post (needs author, province,   │
    │            date)                     │
    └──────────────────┬───────────────────┘
                       ↓
    ┌──────────────────────────────────────┐
    │ PHASE 3: Fact Tables                 │
    │ (Sequential)                         │
    │                                      │
    │  dim_comment (needs dim_post, date)  │
    │  fact_hotel_review_daily             │
    │  fact_province_content_engagement    │
    │  fact_comment_nlp_engagement         │
    └──────────────────┬───────────────────┘
                       ↓
    ┌──────────────────────────────────────┐
    │ PHASE 3b: DL Feature Aggregation     │
    │                                      │
    │  fact_dl_features (NEW)              │
    │  (aggregate 3 facts → ~40 features)  │
    └──────────────────┬───────────────────┘
                       ↓
    ┌──────────────────────────────────────┐
    │ PHASE 4: ML Training                 │
    │                                      │
    │  province_month_features (legacy)    │
    │  train_xgboost / random_forest       │
    │  train_lstm (Deep Learning)          │
    └──────────────────────────────────────┘
                       ↓
                 complete_task
```

---

## Đánh giá & Gợi ý cải thiện

### Điểm mạnh

1. **Star Schema chuẩn** — tách biệt rõ ràng Dimension và Fact, surrogate keys, SCD metadata
2. **GoldJobLogger** — mọi job đều log success/failure vào PostgreSQL, có execution time, record counts, job details
3. **Province mapping** bao gồm luật sáp nhập 2025 — forward-looking data model
4. **NLP Feature Engineering** mạnh — Pandas UDF + underthesea + emoji dict, error-safe
5. **Province-month aggregation** sẵn sàng cho ML training, export Parquet song song
6. **FK validation** tốt — nhiều job in warning khi FK miss, dim_destination fail nếu province không match

### Điểm cần cải thiện

#### 1. `create_gold_database()` lặp lại ở mỗi job

Mỗi file có hàm `create_gold_database(spark)` gần giống nhau. Nên đưa vào `utils/` hoặc gọi 1 lần ở DAG level.

#### 2. Surrogate key dùng `row_number()` — không ổn định giữa các lần chạy

```python
window_spec = Window.orderBy("hotel_url")
df = df.withColumn("hotel_sk", F.row_number().over(window_spec))
```

Nếu data thay đổi giữa 2 lần OVERWRITE, cùng 1 entity sẽ nhận SK khác → phá vỡ FK ở Fact tables. Nên dùng **deterministic hashing** (`md5(business_key)` truncated) hoặc **max(existing_sk) + row_number** cho incremental.

#### 3. `fact_hotel_review_daily` dùng APPEND không dedup

Mỗi lần chạy APPEND thêm records mới mà **không kiểm tra** records đã tồn tại → chạy 2 lần = duplicate toàn bộ. Nên thêm dedup (LEFT ANTI JOIN hoặc MERGE) giống `fact_comment_nlp_engagement`.

#### 4. `dim_comment` MERGE nhưng dim khác OVERWRITE — inconsistent FK

Khi `dim_post` OVERWRITE (reset SK), `dim_comment.post_sk` trỏ đến SK cũ → FK broken. Cần đảm bảo **tất cả dim chạy trước fact**, và SK stable (xem điểm 2).

#### 5. Thiếu `dim_destination` trong Fact tables

`dim_destination` được build nhưng **không có Fact table nào join** với nó. Nên tận dụng cho phân tích cấp địa điểm hoặc bỏ nếu chưa dùng.

#### 6. `__init__.py` outdated

```python
# Liệt kê sai:
# - hotel_ratings_agg.py → không tồn tại
# - sentiment_analysis.py → không tồn tại
```

#### 7. NLP chỉ có 3 mức sentiment (positive/neutral/negative)

`underthesea.sentiment()` trả binary (positive/negative/None). Không có confidence score — mọi positive đều = 1.0, negative = -1.0. Có thể cải thiện bằng fine-tuned model hoặc PhoBERT.

#### 8. Country mapping dict quá lớn (~290 entries) hardcode trong config

`dim_country/config.py` có ~290 entries mapping country→region. Nên chuyển sang CSV hoặc lookup table riêng.

#### 9. Thiếu data freshness check

Không job nào kiểm tra Silver data đã được update chưa trước khi chạy Gold. Nếu Silver chưa update mà Gold chạy → Gold output cũ.

---

## Cấu trúc thư mục

```
spark/jobs/gold/
├── __init__.py
│
├── dim_province/          # CSV → Iceberg (63 tỉnh, mapping sáp nhập)
├── dim_date/              # PostgreSQL JDBC → Iceberg (calendar)
├── dim_author/            # Silver tiktok_post_metadata → dedup by author_tag
├── dim_hotel/             # Silver hotels_detail + dim_province join
├── dim_country/           # Silver hotels_reviews → distinct countries + region UDF
├── dim_post/              # Silver posts + videos + 3 dim joins
├── dim_comment/           # Silver comments + dim_post + dim_date (MERGE)
├── dim_room_type/         # Silver hotels_reviews → distinct room types
├── dim_travel_type/       # Silver hotels_reviews → distinct traveler types
├── dim_destination/       # CSV → Iceberg (POIs with lat/long)
│
├── fact_hotel_review/     # Silver reviews + 5 dim joins (APPEND)
├── fact_comment_nlp_engagement/  # NLP Pandas UDF + MERGE (465K comments)
├── fact_province_content_engagement/  # Post engagement metrics (OVERWRITE)
│
├── fact_dl_features/      # NEW: ML-optimized ~40 features (aggregate 3 facts)
│   ├── config.py
│   └── fact_dl_features_job.py
│
└── TrainingModel/         # Province-month aggregation for ML training (legacy)
    ├── config.py
    └── train_province_model.py  # Aggregate 465K comments → 3K province-months
```

Mỗi thư mục chứa:
- `config.py` — Source/target paths, business keys, mappings
- `*_job.py` — Main job logic
- `__init__.py` — (mostly empty)

---

## Dependencies

| Module | Chức năng |
|---|---|
| `utils/spark_session.py` | Tạo SparkSession (Iceberg + S3/MinIO) |
| `utils/iceberg_utils.py` | `create_iceberg_table_if_not_exists()` |
| `utils/merge_utils.py` | `calculate_row_checksum()`, `merge_into_gold_dim()` |
| `utils/gold_job_logger.py` | `GoldJobLogger` — log success/failure to PostgreSQL |
| `underthesea` | Vietnamese NLP (sentiment, word_tokenize) |
| `emoji` | Emoji detection library |

---

**Last Updated**: April 12, 2026
