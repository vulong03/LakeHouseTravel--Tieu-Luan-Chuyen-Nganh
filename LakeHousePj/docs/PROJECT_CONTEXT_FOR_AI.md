# PROJECT CONTEXT — LakeHouse Du Lịch Việt Nam
> **Mục đích:** Tài liệu này giúp AI mới nắm toàn bộ ngữ cảnh dự án mà không cần đọc lại từng file code.
> **Cập nhật lần cuối:** 2026-05-30
> **Workspace:** `d:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\LakeHousePj`

---

## 0. Thông tin đề tài

**Tên đề tài:** Xây dựng Lakehouse và mô hình Deep Learning dự báo xu hướng du lịch từ dữ liệu mạng xã hội và đánh giá trực tuyến

**Các thành phần chính cần đáp ứng:**
1. Xây dựng **Lakehouse** (Bronze → Silver → Gold, Iceberg + MinIO)
2. **Deep Learning model** (LSTM forecast + PhoBERT NLP)
3. **Dự báo xu hướng du lịch** (province-level, 12 tháng tiếp theo)
4. Dữ liệu **mạng xã hội** (TikTok)
5. Dữ liệu **đánh giá trực tuyến** (Booking.com hotel reviews)

---

## 1. Kiến trúc tổng quan

```
Bronze (S3/MinIO raw CSV)
    │
    ▼
Silver (Iceberg, cleaned & typed)
    ├── lakehouse.silver.tiktok_videos
    ├── lakehouse.silver.tiktok_post_metadata
    ├── lakehouse.silver.tiktok_post_comments
    ├── silver.silver.hotels_list
    ├── silver.silver.hotels_detail
    └── silver.silver.hotels_reviews
    │
    ▼
Gold (Iceberg, Star Schema)
    ├── Dimensions (10 bảng): dim_province, dim_date, dim_author, dim_post,
    │                          dim_comment, dim_hotel, dim_country, dim_room_type,
    │                          dim_travel_type, dim_destination
    ├── Facts (3 bảng):        fact_hotel_review_daily
    │                          fact_province_content_engagement
    │                          fact_comment_nlp_engagement (v1, underthesea)
    │                          fact_comment_nlp_v2 (PhoBERT - chạy riêng)
    └── ML Feature Table:      fact_province_month_dl_features (~40 features, grain: province×month)
    │
    ▼
ML Layer
    ├── NLP Pipeline:   nlp/weak_labeling.py → nlp/train_phobert.py → nlp/inference_phobert.py
    └── LSTM Forecast:  ml/train_lstm_forecast.py → gold.gold.province_month_forecast_lstm_next12
```

**Catalog mapping:**
- `lakehouse` → Silver TikTok tables (Iceberg + Hive Metastore)
- `silver` → Silver Hotel tables
- `gold` → Gold tables (Iceberg, warehouse: `s3a://gold/lakehouse`)
- Storage: MinIO (S3-compatible), `http://minio:9000`
- Tracking: PostgreSQL `metastore_db`
- MLflow: PostgreSQL `mlflow_db`

---

## 2. Silver Layer — Dữ liệu nguồn

### 2.1 TikTok (catalog: `lakehouse`, database: `silver`)

| Bảng | Grain | Timeline | Strategy | Ghi chú |
|---|---|---|---|---|
| `tiktok_videos` | 1 video | `posted_date` | MERGE (url) | Region partition |
| `tiktok_post_metadata` | 1 bài đăng | `post_date`, `crawl_date` | APPEND | ~1.4K posts |
| `tiktok_post_comments` | 1 comment | `comment_date`, `scrape_date` | APPEND | ~465K comments |

**Key columns TikTok:**
- `tiktok_post_comments`: `post_url`, `stt`, `ten`, `tag_ten`, `comment`, `comment_date`, `level_comment` ("Yes"=cấp 1/"No"=cấp 2), `replied_to_tag_name`, `likes`
- `tiktok_post_metadata`: `post_url`, `post_date`, `likes`, `comments_count`, `saves`, `shares`, `author`, `author_tag`
- `tiktok_videos`: `url`, `posted_date`, `region` (8 vùng miền VN), `keyword`

**Quan trọng:** `level_comment` "Yes" = comment gốc (cấp 1), "No" = reply (cấp 2) — ngược với tên gọi thông thường.

### 2.2 Hotel Booking.com (catalog: `silver`, database: `silver`)

| Bảng | Grain | Timeline | Strategy | Ghi chú |
|---|---|---|---|---|
| `hotels_list` | 1 khách sạn | ❌ Không | MERGE (hotel_url) | Chỉ tên + URL + tỉnh |
| `hotels_detail` | 1 khách sạn | ❌ Không (snapshot) | MERGE (hotel_url) | Rating, amenities, rating_breakdown |
| `hotels_reviews` | 1 review | ✅ `stay_date` (month/year), `review_date` | APPEND + ANTI JOIN | ~nhiều nghìn reviews, có traveler_type |

**Key columns hotels_reviews (timeline data):**
- `hotel_url`, `hotel_name` — join key
- `stay_date` — DATE, "tháng MM/YYYY" → ngày lưu trú → **TRỤC THỜI GIAN của hotel data**
- `review_date` — DATE, ngày viết review
- `review_score` — DOUBLE (0-5 scale trên Booking.com)
- `traveler_type` — STRING: `"Gia đình"`, `"Cặp đôi"`, `"Một mình"`, `"Công tác"`, `"Nhóm bạn"`
- `room_type` — loại phòng
- `reviewer_country` — quốc gia người đánh giá
- `review_positive`, `review_negative` — text (có thể NLP)
- `review_title` — tiêu đề

**Key columns hotels_detail (static):**
- `hotel_url`, `province`
- `rating_score` — DOUBLE (Booking.com aggregated score)
- `review_count` — INT (tổng số review, snapshot hiện tại)
- `top_amenities` — STRING (danh sách tiện ích)
- `rating_breakdown` — STRING (điểm chi tiết theo category: cleanliness/comfort/location/facilities/staff/value)

---

## 3. Gold Layer — Star Schema

### 3.1 Dimensions (các job chính)

```
spark/jobs/gold/
├── dim_province/    → 63 tỉnh, map province_name_afterLaw (luật sáp nhập 2025: 63→38 đơn vị)
├── dim_date/        → JDBC từ PostgreSQL, ~3.6K rows, 20 calendar columns
├── dim_author/      → từ tiktok_post_metadata, dedup by author_tag
├── dim_post/        → JOIN phức tạp: tiktok_post_metadata + tiktok_videos + dim_author + dim_province + dim_date
│                      dim_province join bằng keyword "du lịch {province_name}"
├── dim_comment/     → từ tiktok_post_comments, MERGE incremental (465K), business_key=(post_url_nk, stt)
├── dim_hotel/       → từ hotels_detail + dim_province join
└── dim_country/     → distinct countries từ hotels_reviews, map country→region bằng dict ~290 entries
```

**dim_post — Job phức tạp nhất:**
- INNER JOIN `tiktok_post_metadata` với `tiktok_videos` (chỉ lấy `read_status=1`)
- Extract tỉnh từ `keyword` dạng "du lịch {tỉnh}" → lookup `dim_province`
- 2 FK ngày: `crawl_date_sk` và `post_date_sk`
- Convert `has_sub` "yes"/"no" → Boolean

**dim_comment — Đặc biệt:**
- Duy nhất dùng **MERGE incremental** (không OVERWRITE) vì volume 465K
- Composite business key `(post_url_nk, stt)`, convert `level_comment` "Yes"/"No" → int 2/1

### 3.2 Fact Tables

**`fact_hotel_review_daily`** (grain: 1 review)
- Source: `silver.hotels_reviews`
- Join: `dim_hotel` (by hotel_url, fallback by hotel_name), `dim_travel_type`, `dim_room_type`, `dim_country`, `dim_date` (stay_date)
- Write: **APPEND** — ⚠️ BUG: không dedup, chạy 2 lần = duplicate toàn bộ

**`fact_province_content_engagement`** (grain: 1 post)
- Source: `gold.dim_post` + `silver.tiktok_post_metadata`
- Columns: `post_sk`, `province_sk`, `date_sk`, `author_sk`, `likes`, `comments`, `saves`, `shares`, `engagement_score`
- Write: **OVERWRITE**

**`fact_comment_nlp_engagement`** (grain: 1 comment, v1 - underthesea)
- NLP bằng Pandas UDF + underthesea (sentiment binary) + emoji library
- 8 NLP features: `word_count`, `unique_word_ratio`, `exclamation_count`, `sentiment_score`, `sentiment_label`, `emoji_count`, `positive_emoji_count`, `negative_emoji_count`
- Write: **MERGE** incremental

**`fact_comment_nlp_v2`** (grain: 1 comment, PhoBERT - chạy riêng)
- Source: `gold.dim_comment` + `gold.dim_post` + `silver.tiktok_post_comments`
- Kết quả PhoBERT inference: `sentiment_score` (0-1 continuous), `sentiment_label`, 6 aspect scores, `intent_label`, `intent_confidence`
- Write: **OVERWRITE**
- ⚠️ Chạy riêng biệt, KHÔNG nằm trong DAG chính `gold_aggregation`

### 3.3 ML Feature Table: `fact_province_month_dl_features`

**File:** `spark/jobs/gold/fact_dl_features/fact_dl_features_job.py`
**Grain:** 1 province × 1 month
**~46 features** chia 6 nhóm + metadata

| Nhóm | Count | Nguồn data |
|---|---|---|
| Volume & Activity | 6 | TikTok posts + comments count |
| TikTok Engagement | 7 | likes/saves/shares avg, p90, viral_ratio |
| NLP & Sentiment | 10 | sentiment score/std, ratios, word stats, emoji, reply_ratio |
| Aspect Scores (NLP v2) | 6 | avg_aspect_scenery/food/price/service/transport/accommodation |
| Hotel / Booking | 7 | avg_hotel_score, hotel_score_std, review_volume, high/low_score_ratio, unique_reviewer_countries, domestic_review_ratio |
| Temporal + Lags | 10 | month_sin, month_cos, is_peak_season, hotness_score, lag_1/2/3/12, rolling_3m, momentum |

**Hotness Score Formula** (target variable cho LSTM):
```python
# Dùng percent_rank() trong cùng year_month window → normalize 0-1
hotness_score = (
    norm_volume    * 0.25  # posts + comments percent_rank
    + norm_engagement * 0.35  # likes/saves/comment_likes percent_rank
    + norm_sentiment  * 0.20  # positive_ratio + avg_sentiment - negative_ratio
    + norm_emoji      * 0.05  # emoji_sentiment_ratio
    + norm_nlp        * 0.15  # word_count + unique_word_ratio
)
# clamp [0, 1]
```

⚠️ **Vấn đề với hotness_score:** Đây là composite index tự thiết kế từ `percent_rank()` — **không phải số liệu du lịch thực tế** (lượt khách/doanh thu). Mô hình đang "tự tạo target rồi dự báo target đó" — cần defend rõ trong báo cáo.

**Logic chọn NLP source:**
```python
use_v2 = _try_nlp_v2(spark)  # check fact_comment_nlp_v2 exists + has data
if use_v2:
    # Dùng PhoBERT: continuous sentiment + 6 aspect scores
else:
    # Fallback v1 (underthesea): 3-level sentiment, aspect scores = 0.0
```

---

## 4. ML Pipeline

### 4.1 NLP Pipeline (`spark/jobs/ml/nlp/`)

**Bước 1 — Weak Labeling** (`weak_labeling.py`):
- Input: `gold.dim_comment` + `silver.tiktok_post_comments` (lấy `comment_likes`)
- Pandas UDF áp dụng 4 signals: keyword matching, emoji polarity, exclamation count, underthesea
- Output: confidence score → filter ≥ 0.6 → export `s3a://gold/ml_training/nlp_weak_labeled.parquet`
- Labels: `sentiment_label` (3 class), `aspects` (6 multi-label), `intent_label` (4 class)

**Bước 2 — Fine-tune PhoBERT** (`train_phobert.py`):
- Model: `vinai/phobert-base-v2` + 3 heads (sentiment/aspect/intent)
- Optimizations: Layer Freeze (8/12 layers), FP16 AMP, Gradient Accumulation (4 steps), Early Stopping (patience=2), Differential LR (backbone: 2e-5, heads: 1e-4)
- Stratified sampling: up to 27K/class (~80K total)
- Loss: `CrossEntropy(sentiment)*0.4 + BCE(aspect)*0.3 + CrossEntropy(intent)*0.3`
- Register: MLflow model registry → `tourism_comment_nlp`

**Bước 3 — Inference** (`inference_phobert.py`):
- Load model từ MLflow registry
- Pandas UDF per-executor (model loaded once per executor)
- Output: `gold.gold.fact_comment_nlp_v2` (partition by `province_sk`)
- Sentiment score: continuous `neg*0.0 + neu*0.5 + pos*1.0`
- Aspect: sigmoid probabilities per category
- Intent: softmax argmax + confidence

**Config** (`nlp/config.py`):
- 30+ positive keywords, 20+ negative keywords (tiếng Việt + English)
- 60+ positive emojis, 40+ negative emojis
- 6 aspects: `scenery`, `food`, `price`, `service`, `transport`, `accommodation`
- 4 intents: `recommend`, `complain`, `question`, `share`

### 4.2 LSTM Forecast (`spark/jobs/ml/train_lstm_forecast.py`)

**Architecture:**
```python
LSTMForecaster:
    LSTM(input_size=26, hidden_size=32, num_layers=1, batch_first=True)
    → LayerNorm(32)
    → TemporalAttention(32)  # Linear(32→1) → softmax → weighted sum
    → FC: Linear(32→16) → ReLU → Dropout(0.3) → Linear(16→1)
```

**26 input features** (từ `fact_province_month_dl_features`):
- 2 temporal: `month_sin`, `month_cos`
- 6 lag: `hotness_lag_1/2/3/12`, `hotness_rolling_3m`, `hotness_momentum`
- 5 volume: `total_posts`, `total_comments`, `total_hotel_reviews`, `unique_authors`, `comments_per_post`
- 5 engagement: `avg_likes_per_post`, `avg_saves_per_post`, `avg_shares_per_post`, `viral_post_ratio`, `engagement_score`
- 8 NLP: `avg_sentiment`, `sentiment_std`, `positive_ratio`, `negative_ratio`, `avg_word_count`, `avg_unique_word_ratio`, `emoji_sentiment_ratio`, `reply_ratio`
- 5 hotel: `avg_hotel_score`, `hotel_score_std`, `hotel_review_volume`, `high_score_ratio`, `domestic_review_ratio`

**Training config:**
- `SEQUENCE_LENGTH = 4` (4 tháng liên tiếp → predict tháng kế tiếp)
- `TRAIN_TEST_SPLIT = 0.7` (time-based split by `year_month` quantile)
- `EPOCHS = 150`, `PATIENCE = 20` (early stopping)
- `BATCH_SIZE = 32`, `LEARNING_RATE = 0.001`
- Loss: `HuberLoss(delta=0.5)`
- Scheduler: `ReduceLROnPlateau(factor=0.5, patience=7, min_lr=1e-5)`
- Scaler: `MinMaxScaler()` fit on **train only** (no data leakage)

**Forecast:** Autoregressive recursive — forecast 12 tháng tiếp theo per province, cập nhật lag features mỗi bước.

**Output:**
- MLflow: model `province_hotness_forecaster_lstm`
- Iceberg: `gold.gold.province_month_forecast_lstm_next12`
- Parquet: `s3a://gold/ml_forecast/province_hotness_forecast_lstm_*.parquet`

---

## 5. Airflow DAG: `gold_aggregation`

```
health_check → start_task
    │
    ├── PHASE 1 (Parallel - Independent Dims):
    │   dim_province, dim_date, dim_author, dim_country, dim_room_type, dim_travel_type
    │
    ├── PHASE 2 (Sequential - Dependent Dims):
    │   dim_hotel → dim_destination → dim_post
    │
    ├── PHASE 3 (Fact Tables):
    │   dim_comment → fact_hotel_review_daily
    │               → fact_province_content_engagement
    │               → fact_comment_nlp_engagement (v1)
    │
    ├── PHASE 3b:
    │   fact_dl_features (aggregate 3 facts → ~40 features)
    │
    └── PHASE 4 (ML):
        province_month_features (legacy) → train_xgboost / random_forest / train_lstm

⚠️ NLP v2 (PhoBERT inference) KHÔNG nằm trong DAG này — chạy riêng biệt
```

---

## 6. Vấn đề hiện tại & Phân tích

### 6.1 Vấn đề học thuật: `hotness_score` là self-constructed target

**Mô tả:** `hotness_score` được tính bằng `percent_rank()` trên TikTok signals (volume + engagement + sentiment). Không có Ground Truth từ số liệu du lịch thực tế.

**Rủi ro trong báo cáo:** Reviewer có thể hỏi "Tại sao lại dùng công thức này? Có validate không?"

**Hướng xử lý:**
1. Trình bày rõ công thức và justify từng weight trong báo cáo
2. Hoặc bổ sung thêm `hotel_review_volume_growth` (từ Booking.com `stay_date`) như một proxy du lịch độc lập để corroborate

### 6.2 PhoBERT chưa được tích hợp vào pipeline chính

**Mô tả:** `fact_dl_features_job.py` có logic `_try_nlp_v2()` — nếu `fact_comment_nlp_v2` chưa tồn tại thì 6 aspect features = 0.0. NLP v2 chạy ngoài DAG.

**Hậu quả:** LSTM có thể đang train với aspect features = 0 toàn bộ nếu PhoBERT inference chưa chạy.

**Fix cần làm:** Thêm `run_phobert_inference` task vào DAG trước `fact_dl_features`.

### 6.3 `fact_hotel_review_daily` dùng APPEND không dedup

**Mô tả:** Mỗi lần chạy APPEND thêm records mới mà không kiểm tra đã tồn tại.

**Hậu quả:** Chạy DAG 2 lần = duplicate toàn bộ fact table.

**Fix:** Thêm LEFT ANTI JOIN hoặc đổi sang MERGE.

### 6.4 Surrogate key không ổn định

Các dim dùng `row_number() OVER ORDER BY business_key` → mỗi lần OVERWRITE SK có thể thay đổi → FK ở Fact tables bị broken.

### 6.5 Dữ liệu hotel chưa tận dụng đủ timeline

Hotel data có timeline qua `hotels_reviews.stay_date`, nhưng hiện tại chỉ aggregate:
- `avg_hotel_score`, `hotel_review_volume`, `high/low_score_ratio`, `unique_reviewer_countries`, `domestic_review_ratio`

**Bị bỏ sót (có timeline, có thể thêm vào):**
- `traveler_type` distribution per month: couple_ratio, family_ratio, business_ratio, solo_ratio
- Hotel review volume growth (month-over-month change)
- International attention trend (% international reviews theo tháng)
- Avg hotel score trend (so sánh với 3 tháng trước)

**Không có timeline (static, không nên dùng cho LSTM):**
- `hotels_detail.review_count` — tổng tích lũy, không phải theo tháng
- `hotels_detail.rating_score` — snapshot
- `hotels_list.*` — static list
- `hotels_detail.top_amenities` — static

---

## 7. Cải tiến được thảo luận (CHƯA implement)

### Priority 1: Thêm Traveler Type features (time-varying từ hotels_reviews)

```python
# Thêm vào aggregate_hotel_reviews() trong fact_dl_features_job.py
agg = df.groupBy("province_sk", "year_month").agg(
    ...
    # Traveler type distribution
    (F.sum(F.when(F.col("traveler_type").contains("Cặp đôi"), 1).otherwise(0))
     / F.count("fact_id")).alias("couple_ratio"),
    (F.sum(F.when(F.col("traveler_type").contains("Gia đình"), 1).otherwise(0))
     / F.count("fact_id")).alias("family_ratio"),
    (F.sum(F.when(F.col("traveler_type").contains("Công tác"), 1).otherwise(0))
     / F.count("fact_id")).alias("business_traveler_ratio"),
    (F.sum(F.when(F.col("traveler_type").contains("Một mình"), 1).otherwise(0))
     / F.count("fact_id")).alias("solo_traveler_ratio"),
    
    # Volume growth signal (cần thêm window function hoặc lag bên ngoài)
    F.count("fact_id").alias("hotel_review_volume"),  # đã có
)
```

### Priority 2: Thêm hotel_review_volume_growth (trend signal)

```python
# Sau khi aggregate hotel reviews, thêm lag
window_hotel = Window.partitionBy("province_sk").orderBy("year_month")
df_hotel_agg = df_hotel_agg.withColumn(
    "hotel_review_vol_lag1",
    F.lag("hotel_review_volume", 1).over(window_hotel)
).withColumn(
    "hotel_review_vol_growth",
    F.when(F.col("hotel_review_vol_lag1") > 0,
           (F.col("hotel_review_volume") - F.col("hotel_review_vol_lag1"))
           / F.col("hotel_review_vol_lag1")
    ).otherwise(F.lit(None))
)
```

### Priority 3: NLP đơn giản trên hotel review text (positivity ratio)

```python
# Heuristic: không cần model, dùng độ dài text
df = df.withColumn(
    "review_positivity_ratio",
    F.when(
        (F.length("review_positive") + F.length("review_negative")) > 0,
        F.length("review_positive") /
        (F.length("review_positive") + F.length("review_negative"))
    ).otherwise(0.5)
)
```

### Priority 4: Cải thiện hotness_score formula

```python
# Bổ sung hotel demand signal vào công thức
norm_hotel_demand = percent_rank(hotel_review_volume)  # per year_month window
norm_hotel_quality = percent_rank(avg_hotel_score)

HOTNESS_WEIGHTS = {
    "volume": 0.20,         # TikTok volume (giảm từ 0.25)
    "engagement": 0.25,     # TikTok engagement (giảm từ 0.35)
    "sentiment": 0.15,      # TikTok sentiment
    "hotel_demand": 0.20,   # Hotel review volume (NEW - timeline signal)
    "hotel_quality": 0.10,  # Hotel official score (NEW)
    "intl_attention": 0.10, # % international reviewers (proxy toàn cầu)
}
```

---

## 8. Cấu trúc thư mục quan trọng

```
spark/jobs/
├── silver/
│   ├── tiktok_videos/          → step_01_transform.py, step_02_clean_load.py
│   └── tiktok_comments/        → batch_processor, file_processor, partition_utils
│
├── gold/
│   ├── dim_province/
│   ├── dim_date/               → JDBC from PostgreSQL
│   ├── dim_author/
│   ├── dim_post/               → phức tạp nhất, 5 join sources
│   ├── dim_comment/            → MERGE incremental, 465K records
│   ├── dim_hotel/
│   ├── dim_country/
│   ├── dim_room_type/
│   ├── dim_travel_type/
│   ├── dim_destination/
│   ├── fact_hotel_review/
│   ├── fact_comment_nlp_engagement/  → underthesea NLP (v1)
│   ├── fact_province_content_engagement/
│   ├── fact_dl_features/       → ML feature engineering (aggregate 3 facts)
│   │   ├── config.py           → HOTNESS_WEIGHTS, source tables, feature lists
│   │   └── fact_dl_features_job.py  → 5 steps: aggregate posts/NLP/hotels/join/write
│   └── TrainingModel/          → train_province_model.py (LEGACY, XGBoost/RF)
│
├── ml/
│   ├── nlp/
│   │   ├── config.py           → keywords, emojis, labels, PhoBERT hyperparams
│   │   ├── weak_labeling.py    → Bước 1: auto-label 465K comments
│   │   ├── train_phobert.py    → Bước 2: fine-tune PhoBERT multi-task
│   │   └── inference_phobert.py → Bước 3: score all comments → fact_comment_nlp_v2
│   └── train_lstm_forecast.py  → LSTM + Attention, forecast 12 months
│
└── utils/
    ├── spark_session.py        → get_spark_session()
    ├── iceberg_utils.py        → create_iceberg_table_if_not_exists()
    ├── merge_utils.py          → calculate_row_checksum(), merge_into_gold_dim()
    └── gold_job_logger.py      → GoldJobLogger, log success/failure to PostgreSQL

docs/
├── SILVER_TIKTOK_COMPLETE_DOCS.md   → Chi tiết Silver TikTok tables + pipeline
├── SILVER_LAYER.md                  → Chi tiết Silver Hotel tables
├── GOLD_LAYER.md                    → Star Schema, dim/fact descriptions, known issues
├── NLP_PIPELINE.md                  → PhoBERT pipeline docs
├── LSTM_FORECAST_MODEL.md           → LSTM model docs
└── PROJECT_CONTEXT_FOR_AI.md        → FILE NÀY (context cho AI mới)
```

---

## 9. Công nghệ stack

| Component | Technology |
|---|---|
| Processing | Apache Spark (PySpark) |
| Table Format | Apache Iceberg |
| Object Storage | MinIO (S3-compatible) |
| Metastore | Hive Metastore (thrift://hive-metastore:9083) |
| Orchestration | Apache Airflow |
| ML Tracking | MLflow (backend: PostgreSQL) |
| Database | PostgreSQL (tracking, dim_date source, MLflow) |
| NLP | underthesea (Vietnamese), PhoBERT (vinai/phobert-base-v2), emoji library |
| DL Framework | PyTorch |
| Query Layer | Dremio (port 9047) |
| Environment | Docker containers |

---

## 10. Điểm cần nhớ khi làm việc với codebase

1. **Catalog TikTok là `lakehouse`** (không phải `silver`): `spark.table("lakehouse.silver.tiktok_videos")`
2. **Catalog Hotel là `silver`**: `spark.table("silver.silver.hotels_reviews")`
3. **Catalog Gold là `gold`**: `spark.table("gold.gold.dim_post")`
4. **`level_comment` "Yes" = cấp 1** (trực tiếp), "No" = cấp 2 (reply) — tên ngược logic
5. **PhoBERT inference chưa trong DAG** — aspect features có thể = 0 nếu chưa chạy riêng
6. **`fact_hotel_review_daily` có APPEND bug** — duplicate nếu chạy >1 lần
7. **`hotness_score` là self-constructed index** — không phải KPI du lịch thực tế
8. **LSTM cần ≥ 12 tháng lịch sử/tỉnh** (`hotness_lag_12`) — tỉnh có ít data bị drop
9. **Surrogate keys không stable** giữa các lần OVERWRITE — potential FK issue
10. **Chỉ `hotels_reviews.stay_date`** là timeline thực sự từ hotel data — `hotels_detail` và `hotels_list` là static

---

## 11. Lệnh hay dùng trong hệ thống (Ops Commands)

### 11.1 Vào Spark SQL (interactive shell)

```bash
# Bước 1: Vào container spark master
docker exec -it lakehouse_spark_master bash

# Bước 2: Khởi động Spark SQL với Iceberg + lakehouse catalog
/opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  --conf spark.hadoop.fs.s3a.metrics.enabled=false
```

### 11.2 Chạy SQL một lần (non-interactive, với full config)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --master spark://spark-master:7077 \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,/opt/spark/jars/postgresql-42.7.2.jar \
  -e "SQL_QUERY_HERE"
```

**Ví dụ thực tế:**
```bash
# Đếm records trong Silver
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  -e "SELECT COUNT(*) FROM lakehouse.silver.tiktok_post_comments;"

# Xem schema bảng Silver TikTok
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  -e "DESCRIBE TABLE lakehouse.silver.tiktok_videos;"

# Xem toàn bộ bảng trong lakehouse.silver
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  -e "USE lakehouse.silver; SHOW TABLES;"

# Xem chi tiết bảng (format, partitions, location S3)
# -e "DESCRIBE FORMATTED silver.silver.tiktok_post_metadata;"
# -e "SHOW TBLPROPERTIES lakehouse.bronze.raw_booking_hotels_list;"
# -e "SHOW PARTITIONS lakehouse.bronze.raw_booking_hotels_list;"

# Kiểm tra null trong tiktok_post_comments
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  -e "USE silver; SELECT COUNT(*) as total, SUM(CASE WHEN post_url IS NULL THEN 1 ELSE 0 END) as null_post_url, SUM(CASE WHEN comment IS NULL THEN 1 ELSE 0 END) as null_comment FROM tiktok_post_comments;"
```

### 11.3 Gold catalog queries

```bash
# Drop Gold table
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  -e "DROP TABLE IF EXISTS gold.gold.dim_province"
```

### 11.4 Vào Hive Metastore

```bash
docker exec -it lakehouse_hive_metastore bash
hive
```

### 11.5 Thao tác MinIO (S3 object storage)

```bash
# Xem danh sách buckets
docker exec lakehouse_minio mc ls local/

# Xem nội dung trong Gold
docker exec lakehouse_minio mc ls local/gold/lakehouse/gold.db/

# Xóa data trong bronze (reset)
docker exec lakehouse_minio mc rm --recursive --force minio/bronze/

# Xóa 1 table cụ thể trong Silver
docker exec lakehouse_minio mc rm --recursive --force local/silver/lakehouse/silver.db/tiktok_post_metadata/
docker exec lakehouse_minio mc rm --recursive --force local/silver/lakehouse/silver.db/tiktok_post_comments/
```

### 11.6 PostgreSQL (Metastore + Tracking)

```bash
# Xóa toàn bộ file tracking log
docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db \
  -c "TRUNCATE TABLE file_ingestion_log;"

# Kiểm tra còn bao nhiêu log
docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db \
  -c "SELECT COUNT(*) as remaining_logs FROM file_ingestion_log;"

# Xóa log của 1 table cụ thể
echo "DELETE FROM file_ingestion_log WHERE table_name = 'hotels_list';" \
  | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db

# Import dim_date từ SQL file
docker exec lakehouse_postgres psql -U lakehouse_user -d date_db -f /tmp/dim_date.sql
```

### 11.7 Xóa 1 table khỏi Hive Metastore (manual cleanup)

```bash
# Bước 1: Lấy TBL_ID của table
echo 'SELECT "TBL_ID", "TBL_NAME" FROM "TBLS" WHERE "TBL_NAME" = '\''tiktok_post_metadata'\'';' \
  | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db

# Bước 2: Xóa theo TBL_ID (thay 434 bằng ID thực)
echo 'DELETE FROM "TABLE_PARAMS" WHERE "TBL_ID" = 434;
      DELETE FROM "PARTITION_KEYS" WHERE "TBL_ID" = 434;
      DELETE FROM "TBLS" WHERE "TBL_ID" = 434;' \
  | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db

# Bước 3: Xóa file MinIO tương ứng (xem 11.5)
```

*Document này được tạo ngày 2026-05-30 để tóm tắt toàn bộ ngữ cảnh dự án từ các cuộc thảo luận phân tích code và architecture.*
