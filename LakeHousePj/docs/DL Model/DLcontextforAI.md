# DL Model Context for AI — LakeHouse Tourism Forecasting

> **Mục đích tài liệu**: Cung cấp toàn bộ context kỹ thuật về pipeline Deep Learning
> trong hệ thống LakeHouse Tourism để AI assistant có thể hỗ trợ debugging, tuning,
> và mở rộng mà không cần đọc lại toàn bộ codebase.
>
> **Cập nhật lần cuối**: 2026-06-11

---

## 1. Tổng quan hệ thống

Pipeline DL gồm 2 mô hình:
1. **PhoBERT** — NLP multi-task: phân tích sentiment + aspect detection cho TikTok comments
2. **LSTM** — Time series forecasting: dự báo lượng hotel review (proxy đo lường hotness du lịch) theo tỉnh/tháng

```
TikTok Raw Data  ──► PhoBERT Inference ──► NLP Features
Booking.com Data ──────────────────────► Hotel Features
                                                 │
                                    fact_province_month_dl_features
                                                 │
                                           LSTM Forecasting
                                                 │
                                    province_month_forecast_lstm_next12
```

---

## 2. Data Lineage: `fact_province_month_dl_features`

**Grain**: 1 row = 1 tỉnh × 1 tháng  
**Tổng rows**: 1,525  
**Tổng features**: ~65 cột (50 dùng cho LSTM training)

### 2.1 Nguồn dữ liệu (3 nguồn chính)

```
fact_province_month_dl_features
│
├── [Step 1] gold.gold.fact_province_content_engagement
│            (grain: 1 post, 5,616 rows)
│            → likes, shares, saves, comments per post
│            + dim_post (post_date_sk)
│            + dim_date  (year_month)
│
├── [Step 2] gold.gold.fact_comment_nlp_v2          ← ƯU TIÊN (PhoBERT)
│     hoặc  gold.gold.fact_comment_nlp_engagement   ← fallback (underthesea)
│            (grain: 1 comment, 861,113 rows in v2)
│            → sentiment_score, sentiment_label, aspects
│            + dim_post
│            + dim_date
│
└── [Step 3] gold.gold.fact_hotel_review_daily
│            (grain: 1 review/ngày)
│            → avg_hotel_score, hotel_review_volume, traveler_type_ratio
│            + dim_hotel (province_sk)
│            + dim_date
│            + dim_country (domestic vs foreign)
│            + dim_travel_type (couple/family/solo/business)
```

### 2.2 JOIN strategy

```python
# TikTok engagement → LEFT JOIN NLP → LEFT JOIN Hotel
# Tỉnh có TikTok nhưng không có Booking → hotel cols = fillna(0)
# Tỉnh có TikTok nhưng không có NLP   → NLP cols   = fillna(0)
```

> **Lưu ý**: Bảng lấy theo TikTok làm trục chính. Nếu tỉnh nào tháng đó không có bài
> TikTok nào → không có row trong DL features table.

### 2.3 Tên bảng trong Dremio vs Spark

| Spark | Dremio (hive_metastore) |
|---|---|
| `gold.gold.fact_province_content_engagement` | `"hive_metastore".gold."fact_province_content_engagement"` |
| `gold.gold.fact_hotel_review_daily` | `"hive_metastore".gold."fact_hotel_review_daily"` |
| `gold.gold.fact_comment_nlp_v2` | `"hive_metastore".gold."fact_comment_nlp_v2"` |
| `gold.gold.fact_province_month_dl_features` | `"hive_metastore".gold."fact_province_month_dl_features"` |

---

## 3. Data Quality Issues đã phát hiện (2026-06-05)

### 3.1 `avg_shares_per_post` — phân phối cực lệch

| Metric | Giá trị |
|---|---|
| Min | 0 |
| **P50 (median)** | **0** (68.3% posts có shares=0) |
| P90 | 77,863 |
| P99 | 476,424 |
| Max | **2,181,850** |

**Nguyên nhân**: TikTok đếm shares cả cross-platform (Facebook, Zalo, WhatsApp...).
Một post có thể có 20 likes trong app nhưng 2.8M cross-platform shares.

**Trường hợp bất thường nhất**:
| Tỉnh | Tháng | Likes | Shares | Ratio |
|---|---|---|---|---|
| Bà Rịa Vũng Tàu | 202410 | 217 | 1,500,000 | 6,900x |
| Kon Tum | 202403 | 39 | 444,700 | 11,400x |
| Vĩnh Phúc | 202409 | 22 | 550,200 | 25,000x |

**Xử lý trong LSTM v5**:
```python
# shares đã được loại bỏ hoàn toàn khỏi ENGAGEMENT_FEATURES do không đồng nhất và kém tin cậy
# Chỉ giữ lại likes, saves, viral_post_ratio, engagement_score và hotness_score
```

### 3.2 `likes` trong fact_province_content_engagement

- `likes_null = 0` → không có NULL (đầy đủ)
- `likes_zero = 1` → chỉ 1 post duy nhất có 0 likes (không đáng kể)
- **Kết luận**: likes = 0 không phải nguyên nhân gây distortion. Nguyên nhân thực là shares >> likes.

---

## 4. PhoBERT Pipeline

### 4.1 Mô hình

- **Base**: `vinai/phobert-base-v2`
- **Task**: Multi-task learning
  - Sentiment (positive/neutral/negative) + continuous score
  - Aspect detection (scenery, food, price, service, transport, accommodation)
  - Intent classification (recommend/complain/question/share)
- **File**: `phobert_multi_task.pt` (trained locally, inference on Colab)

### 4.2 Training data

- **81,000 comments** được label bằng thư viện rule-based (underthesea + từ điển)
- **Split**: 3-way split (75% train / 10% val / 15% test)
- **Lý do chọn 81k**: đủ đa dạng nhưng vừa với RAM local training

### 4.3 Inference pipeline (Google Colab)

```
[Local Spark]                    [Google Colab GPU]
export_comments_for_colab.py  →  colab_inference_phobert.py
→ 861,113 comments.parquet    →  → 861,113 results.parquet
                                         ↓
[Local Spark] import_colab_results.py → gold.gold.fact_comment_nlp_v2
```

- **Tốc độ**: ~1,340 rows/sec trên T4 GPU → ~11 phút cho 861k comments
- **File export path**: `LakeHousePj/data/GoogleColab/comments_to_score.parquet`

---

## 5. LSTM Forecasting Model

### 5.1 Target variable

```python
TARGET = "hotel_review_volume"
# Log transform trước khi train:
df[TARGET] = np.log1p(df[TARGET])
```

**Lý do dùng hotel_review_volume**: Proxy tốt cho lưu lượng du lịch thực tế.

### 5.2 50 Features cho LSTM (v5.0)

| Nhóm | Số features | Ví dụ |
|---|---|---|
| Temporal | 2 | month_sin, month_cos |
| Hotel Lag | 6 | hotel_vol_lag_1/2/3/12, rolling_3m, momentum |
| Hotness Lag | 6 | hotness_lag_1/2/3/12, rolling_3m, momentum |
| Volume | 5 | total_posts, total_comments, total_hotel_reviews, unique_authors, comments_per_post |
| Engagement | 5 | avg_likes, avg_saves, viral_ratio, engagement_score, hotness_score |
| NLP | 8 | avg_sentiment, sentiment_std, positive_ratio, negative_ratio, avg_word_count, unique_word_ratio, emoji_ratio, reply_ratio |
| Aspect | 6 | avg_aspect_scenery, avg_aspect_food, avg_aspect_price, avg_aspect_service, avg_aspect_transport, avg_aspect_accommodation |
| Hotel Quality | 9 | avg_hotel_score, hotel_score_std, high_score_ratio, domestic_ratio, couple/family/business/solo_ratio, hotel_vol_growth |
| Custom | 3 | social_to_booking_ratio, sentiment_polarity_change, hotel_vol_std_rolling_3m |

### 5.3 Architecture (v5 — hiện tại)

```python
LSTMForecaster(
    input_size  = 50,
    hidden_size = 48,      # 2 LSTM layers
    num_layers  = 2,
    dropout     = 0.43
)
# Sau LSTM:
LayerNorm(48)
→ TemporalAttention(48)   # softmax attention over sequence
→ Linear(48) → GELU → Dropout(0.43)
→ Linear(16) → ReLU
→ Linear(1)               # output: log1p(hotel_review_volume)
```

### 5.4 Training config (v5 — tối ưu nhất)

```python
SEQUENCE_LENGTH = 3       # 3 tháng look-back
HIDDEN_SIZE     = 48
NUM_LAYERS      = 2
DROPOUT         = 0.43
LEARNING_RATE   = 0.0005
EPOCHS          = 200
BATCH_SIZE      = 32
PATIENCE        = 25
weight_decay    = 7e-4

# BatchNorm1d: Removed (prevents batch-size-1 recursive forecast mismatch)
# Loss: HybridLoss = 70% HuberLoss(delta=0.5) + 30% SMAPELoss
# Scaler: RobustScaler (median/IQR)
# Scheduler: CosineAnnealingWarmRestarts(T_0=30, T_mult=2, eta_min=1e-6)
# Split: 3-way (70/15/15 time-based)
```

### 5.5 Lịch sử tuning & kết quả

| Phiên bản | seq | test_r2 | gap(train-test) | RMSE | MAPE (Actual) |
|---|---|---|---|---|---|
| v2 (baseline) | 4 | 0.811 | 0.100 | 0.734 | 90.03% |
| v3 (MLflow refactor) | 4 | 0.839 | 0.108 | 0.677 | 88.29% |
| v4 (Round 6 - No BN) | 3 | 0.9281 | 0.0473 | 0.4501 | 40.84% |
| **v5 (Current Production)** | **3** | **0.9312** | **0.0450** | **0.4350** | **38.90%** |

---

## 6. Sequence Count theo SEQUENCE_LENGTH

Tổng: **61 tỉnh**, trung bình **12.9 tháng/tỉnh** (min=3, max=20)

| seq_len | Sequences | Tỉnh valid |
|---|---|---|
| 3 | 608 | 59/61 |
| 4 | 547 | 58/61 |
| 5 | 489 | 55/61 |
| 6 | 434 | 55/61 |

---

## 7. Files quan trọng

| File | Mô tả |
|---|---|
| `spark/jobs/dl/train_province_lstm_v5.py` | LSTM training + forecast (v5, current production) |
| `spark/jobs/dl/train_lstm_forecast.py` | LSTM training + forecast (v4 baseline) |
| `spark/jobs/dl/train_gru_forecast.py` | GRU baseline model |
| `spark/jobs/gold/fact_dl_features/fact_dl_features_job.py` | ETL tạo DL features table |
| `spark/jobs/gold/fact_dl_features/config.py` | Tên bảng nguồn và target |
| `spark/jobs/dl/nlp/GoogleColab/export_comments_for_colab.py` | Export comments → Parquet |
| `spark/jobs/dl/nlp/GoogleColab/colab_inference_phobert.py` | PhoBERT GPU inference trên Colab |
| `spark/jobs/dl/nlp/GoogleColab/import_colab_results.py` | Import kết quả NLP về Iceberg |

---

## 8. MLflow & Infrastructure

| Service | URL | Ghi chú |
|---|---|---|
| MLflow UI | `http://localhost:5001` | Experiment: `province_hotel_volume_forecasting_lstm` |
| MLflow API | `http://mlflow:5000` | Dùng trong Spark container |
| MinIO | `http://localhost:9001` | Lưu model artifacts, scaler, plots |
| Spark Master | `lakehouse_spark_master` | `docker exec lakehouse_spark_master bash -c "..."` |

**Command train lại**:
```bash
docker exec lakehouse_spark_master bash -c \
  "/opt/spark/bin/spark-submit /opt/spark/jobs/dl/train_province_lstm_v5.py"
```

**Model registry**: `province_hotel_volume_forecaster_lstm_v5` (phiên bản mới nhất = v5)

---

## 9. Forecast Output

**Bảng**: `gold.gold.province_month_forecast_lstm_next12`  
**Grain**: 1 tỉnh × 1 tháng forecast (12 bước tiếp theo)  
**Model version tag**: `lstm_v5`

| Cột | Mô tả |
|---|---|
| `predicted_hotel_volume` | log1p scale (raw model output) |
| `predicted_hotel_volume_actual` | Actual scale (expm1 của cột trên) |
| `predicted_growth_pct` | % tăng trưởng so với tháng cuối có dữ liệu |
| `horizon_month` | 1–12 (tháng dự báo thứ mấy) |

**Forecast method**: Recursive autoregressive — dùng prediction của bước trước làm input lag cho bước tiếp theo. Cập nhật: lag_1/2/3, rolling_3m, momentum, month_sin/cos, hotel_vol_growth.
Lọc bỏ các tính toán shares đã lỗi thời.


