# DL Model Context for AI — LakeHouse Tourism Forecasting

> **Mục đích tài liệu**: Cung cấp toàn bộ context kỹ thuật về pipeline Deep Learning
> trong hệ thống LakeHouse Tourism để AI assistant có thể hỗ trợ debugging, tuning,
> và mở rộng mà không cần đọc lại toàn bộ codebase.
>
> **Cập nhật lần cuối**: 2026-06-05

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
**Tổng features**: ~65 cột (36 dùng cho LSTM training)

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

**Xử lý trong LSTM v4**:
```python
df_pd["avg_shares_per_post"] = np.log1p(df_pd["avg_shares_per_post"])  # log compress
df_pd["engagement_score"]    = np.log1p(df_pd["engagement_score"])
# Sau đó clip tại 99th percentile
```

### 3.2 `likes` trong fact_province_content_engagement

- `likes_null = 0` → không có NULL (đầy đủ)
- `likes_zero = 1` → chỉ 1 post duy nhất có 0 likes (không đáng kể)
- **Kết luận**: likes = 0 không phải nguyên nhân gây distortion. Nguyên nhân thực là shares >> likes.

---

## 4. PhoBERT Pipeline

### 4.1 Mô hình

- **Base**: `vinai/phobert-base`
- **Task**: Multi-task learning
  - Sentiment (positive/neutral/negative) + continuous score
  - Aspect detection (scenery, food, price, service, transport, accommodation)
  - Intent classification (share/query/review/complaint)
- **File**: `phobert_multi_task.pt` (trained locally, inference on Colab)

### 4.2 Training data

- **81,000 comments** được label bằng thư viện rule-based (underthesea + từ điển)
- **Split**: 80% train / 20% val
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

**Lý do dùng hotel_review_volume**: Proxy tốt cho lưu lượng du lịch thực tế
(người đi du lịch mới để lại review Booking.com).

### 5.2 36 Features cho LSTM

| Nhóm | Số features | Ví dụ |
|---|---|---|
| Temporal | 2 | month_sin, month_cos |
| Lag | 6 | hotel_vol_lag_1/2/3/12, rolling_3m, momentum |
| Volume | 5 | total_posts, total_comments, total_hotel_reviews, unique_authors, comments_per_post |
| Engagement | 6 | avg_likes, avg_saves, avg_shares, viral_ratio, engagement_score, hotness_score |
| NLP | 8 | avg_sentiment, sentiment_std, positive_ratio, negative_ratio, avg_word_count, unique_word_ratio, emoji_ratio, reply_ratio |
| Hotel | 9 | avg_hotel_score, hotel_score_std, high_score_ratio, domestic_ratio, couple/family/business/solo_ratio, hotel_vol_growth |

### 5.3 Architecture (v4 — hiện tại)

```python
LSTMForecaster(
    input_size  = 36,
    hidden_size = 48,      # 2 LSTM layers
    num_layers  = 2,
    dropout     = 0.4
)
# Sau LSTM:
LayerNorm(48)
→ TemporalAttention(48)   # softmax attention over sequence
→ BatchNorm1d(48)
→ Linear(48) → GELU → Dropout(0.4)
→ Linear(16) → ReLU
→ Linear(1)               # output: log1p(hotel_review_volume)
```

### 5.4 Training config (v4 — tối ưu nhất)

```python
SEQUENCE_LENGTH = 3       # 3 tháng look-back (tốt hơn seq=4)
HIDDEN_SIZE     = 48
NUM_LAYERS      = 2
DROPOUT         = 0.4
LEARNING_RATE   = 0.0005
EPOCHS          = 200
BATCH_SIZE      = 16
PATIENCE        = 25
weight_decay    = 2e-4

# Loss: HybridLoss = 70% HuberLoss(delta=0.5) + 30% SMAPELoss
# Scaler: RobustScaler (median/IQR, robust to outlier provinces)
# Scheduler: CosineAnnealingWarmRestarts(T_0=30, T_mult=2, eta_min=1e-6)
```

### 5.5 Lịch sử tuning & kết quả

| Phiên bản | seq | test_r2 | gap(train-test) | RMSE | MAPE |
|---|---|---|---|---|---|
| v2 (baseline) | 4 | 0.811 | 0.100 | 0.734 | 90.03% |
| v3 (MLflow refactor) | 4 | 0.839 | 0.108 | 0.677 | 88.29% |
| v4 seq=4 | 4 | 0.904 | 0.076 | 0.522 | 49.52% |
| **v4 seq=3 (BEST)** | **3** | **0.911** | **0.068** | **0.499** | **52.95%** |

**Thay đổi quyết định từ v3 → v4**:
- HuberLoss → **HybridLoss (Huber + SMAPE)** → MAPE giảm từ 88% xuống 49-53%
- MinMaxScaler → **RobustScaler** → ít bị ảnh hưởng bởi tỉnh viral
- 1 LSTM layer → **2 layers** + FC sâu hơn với GELU
- Clip thêm 6 engagement features tại p99

**Lý do SEQUENCE_LENGTH=3 tốt hơn 4**:
```
seq=3: 424 train seqs, 183 test seqs → nhiều data hơn
seq=4: 382 train seqs, 165 test seqs → ít hơn ~10%
Kết quả: test_r2 cao hơn (0.911 vs 0.904), gap nhỏ hơn (0.068 vs 0.076)
```

### 5.6 Vấn đề MAPE cao — giải thích đúng

MAPE = 52.95% nhưng **không phải model kém**:

1. **R² đo trên log-scale** → R²=0.91 bị inflate vì log compress variance
2. **Time series autocorrelation cao** → lag_1 feature gần như "cho model biết" đáp án
3. **MAPE bị kéo lên bởi tỉnh nhỏ** (y gần 0): 5 reviews thực → predict 12 → MAPE = 140%
4. **Naive baseline (lag-1 only)** đã đạt R²~0.80-0.85 → LSTM chỉ thêm ~5-10%

**Khi báo cáo**: Dùng SMAPE (10.65%) thay vì MAPE, hoặc nhấn mạnh so sánh với baseline.

---

## 6. Sequence Count theo SEQUENCE_LENGTH

Tổng: **61 tỉnh**, trung bình **12.9 tháng/tỉnh** (min=3, max=20)

| seq_len | Sequences | Tỉnh valid |
|---|---|---|
| 3 | 608 | 59/61 |
| **4 (ban đầu)** | **547** | 58/61 |
| 5 | 489 | 55/61 |
| 6 | 434 | 55/61 |

---

## 7. Files quan trọng

| File | Mô tả |
|---|---|
| `spark/jobs/ml/train_lstm_forecast.py` | LSTM training + forecast (v4, current best) |
| `spark/jobs/gold/fact_dl_features/fact_dl_features_job.py` | ETL tạo DL features table |
| `spark/jobs/gold/fact_dl_features/config.py` | Tên bảng nguồn và target |
| `spark/jobs/ml/nlp/GoogleColab/export_comments_for_colab.py` | Export 861k comments → Parquet |
| `spark/jobs/ml/nlp/GoogleColab/colab_inference_phobert.py` | PhoBERT GPU inference trên Colab |
| `spark/jobs/ml/nlp/GoogleColab/import_colab_results.py` | Import kết quả NLP về Iceberg |
| `spark/jobs/ml/check_sequences.py` | Kiểm tra số sequences theo seq_len |
| `spark/jobs/ml/check_dl_features_dq.py` | DQ check cho engagement features |

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
  "/opt/spark/bin/spark-submit /opt/spark/jobs/ml/train_lstm_forecast.py"
```

**Model registry**: `province_hotel_volume_forecaster_lstm` (phiên bản mới nhất = v4)

---

## 9. Forecast Output

**Bảng**: `gold.gold.province_month_forecast_lstm_next12`  
**Grain**: 1 tỉnh × 1 tháng forecast (12 bước tiếp theo)  
**Model version tag**: `lstm_v4_volume`

| Cột | Mô tả |
|---|---|
| `predicted_hotel_volume` | log1p scale (raw model output) |
| `predicted_hotel_volume_actual` | Actual scale (expm1 của cột trên) |
| `predicted_growth_pct` | % tăng trưởng so với tháng cuối có dữ liệu |
| `horizon_month` | 1–12 (tháng dự báo thứ mấy) |

**Forecast method**: Recursive autoregressive — dùng prediction của bước trước làm input lag cho bước tiếp theo. Cập nhật: lag_1/2/3, rolling_3m, momentum, month_sin/cos, hotel_vol_growth.
