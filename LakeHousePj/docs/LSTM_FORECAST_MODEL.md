# Deep Learning Model — Province Hotness Forecasting (v2.0)

## Tổng quan

Model GRU + Attention dự báo "hotness score" (mức độ hot du lịch) của 63 tỉnh/thành Việt Nam trong 12 tháng tiếp theo, sử dụng dữ liệu đa nguồn từ TikTok (engagement + NLP) và Booking.com (hotel reviews).

**File**: `spark/jobs/ml/train_lstm_forecast.py`

### v2.0 Fixes (so với v1.0)

| # | Vấn đề v1.0 | Fix v2.0 |
|---|---|---|
| 1 | **Data leakage**: target = f(input), current features chứa thông tin target | Current metrics **LAG 1 tháng** (`prev_*` thay vì `norm_*`) |
| 2 | **Scaler leak**: MinMaxScaler fit trên cả test | Scaler **fit trên train only** |
| 3 | **Forecast scale sai**: month_sin/cos raw, lag dùng unscaled predicted | Tất cả giá trị **scale đúng** qua `_scale_single_value()` |
| 4 | **rolling_avg/lag_12 không update**: giữ giá trị cũ suốt 12 bước | **Update mỗi bước** forecast |
| 5 | **Feature trùng**: `month` (1-12 linear) + sin/cos | Bỏ `month`, chỉ giữ **sin/cos** |
| 6 | **LSTM quá lớn cho small data**: 2 layers × 64 hidden = ~35K params | **GRU 1 layer × 32 hidden** (~8K params) + LayerNorm + Attention |
| 7 | **Loss function**: MSE nhạy outliers | **HuberLoss** (delta=0.5) cho robustness |
| 8 | **Chưa có Gradio**: MODELS dict thiếu LSTM | **Đã thêm** LSTM entry vào Gradio |

---

## Vị trí trong kiến trúc Lakehouse

```
Bronze (Raw CSV)
  → Silver (Clean Iceberg tables)
    → Gold (Dimension + Fact tables)
      → province_month_features (28 features, aggregate by province + month)
      → fact_hotel_review_daily (Booking reviews with dimension FKs)
        → train_lstm_forecast.py  ← BẠN ĐANG Ở ĐÂY
          → gold.province_month_forecast_lstm_next12 (output)
          → MinIO parquet (Gradio đọc)
          → MLflow (model registry + artifacts)
```

---

## So sánh 3 models

| Tiêu chí | XGBoost Reduced | Random Forest | GRU Deep Learning |
|---|---|---|---|
| File | `train_and_forecast.py` | `train_random_forest_model.py` | `train_lstm_forecast.py` |
| Loại | Tree-based (Boosting) | Tree-based (Bagging) | Deep Learning (Sequence) |
| Số features | 8 | 8 | **15** |
| Temporal features | 3 (month, sin, cos) | 3 | 2 (sin, cos only) |
| Lag features | 5 (lag_1/2/3/12, rolling) | 5 | 5 |
| Current metrics | **Không dùng** | **Không dùng** | **8 features** (LAGGED 1 tháng) |
| Cross-source | Chỉ TikTok | Chỉ TikTok | **TikTok + Booking** |
| Input format | Tabular (1 row) | Tabular (1 row) | **Sequence (4 tháng)** |
| Data leakage | Không | Không | **Không** (v2.0 fixed) |
| Output table | `forecast_reduced_next12` | `forecast_rf_next12` | `forecast_lstm_next12` |

---

## Data Sources

### 1. TikTok Data (qua `gold.province_month_features`)

Aggregate từ `fact_comment_nlp_engagement` (465K comments) + `fact_province_content_engagement`.

### 2. Booking.com Data (qua `gold.fact_hotel_review_daily`)

Join với `dim_hotel` và `dim_date` để tính `avg_hotel_score` và `hotel_review_count` theo tỉnh/tháng.

---

## 15 Features (v2.0)

### Temporal Features (2) — bỏ `month` raw

| Feature | Mô tả |
|---|---|
| `month_sin` | sin(2π × month / 12) — mã hóa tính tuần hoàn |
| `month_cos` | cos(2π × month / 12) — mã hóa tính tuần hoàn |

### Lag Features (5)

| Feature | Mô tả |
|---|---|
| `hotness_lag_1` | Hotness tháng trước |
| `hotness_lag_2` | Hotness 2 tháng trước |
| `hotness_lag_3` | Hotness 3 tháng trước |
| `hotness_lag_12` | Hotness cùng tháng năm trước (seasonality) |
| `hotness_rolling_avg_3m` | Trung bình hotness 3 tháng **trước đó** (rowsBetween(-3, -1)) |

### Lagged Current Features (8) — LAG 1 tháng để tránh leakage

| Feature | Nguồn | Cách tính (tại tháng t-1) |
|---|---|---|
| `prev_engagement` | TikTok | 0.4 × post_likes + 0.35 × post_saves + 0.25 × comment_likes |
| `prev_sentiment` | TikTok NLP | 0.5 × positive_ratio + 0.3 × avg_sentiment + 0.2 × (1-negative_ratio) |
| `prev_volume` | TikTok | 0.6 × total_posts + 0.4 × total_comments |
| `prev_nlp_richness` | TikTok NLP | 0.45 × avg_words + 0.45 × unique_word_ratio + 0.10 × exclamation_ratio |
| `prev_emoji_vibe` | TikTok NLP | 0.5 × emoji_count + 0.5 × emoji_sentiment |
| `prev_comment_engagement` | TikTok | percent_rank(avg_comment_likes) tháng trước |
| `prev_hotel_score` | **Booking.com** | percent_rank(avg_hotel_score) tháng trước |
| `prev_hotel_volume` | **Booking.com** | percent_rank(hotel_review_count) tháng trước |

**KEY CHANGE v2.0**: Tất cả current features được `F.lag(..., 1)` để sử dụng giá trị **tháng trước**, tránh target leakage (vì `hotness_score` tại tháng t được tính từ chính các norm_* tại tháng t).

---

## Kiến trúc Model (v2.0)

```
Input: (batch_size, sequence_length=4, num_features=15)
  │
  ▼
GRU Layer (input=15, hidden=32, 1 layer)
  │
  ▼
LayerNorm(32)
  │
  ▼
Temporal Attention
  │  scores = Linear(32 → 1) per timestep
  │  weights = softmax(scores)
  │  context = weighted sum of GRU outputs
  ▼
Context Vector: (batch_size, 32)
  │
  ▼
Dense(32 → 16) → ReLU → Dropout(0.3)
  │
  ▼
Dense(16 → 1)
  │
  ▼
Output: predicted hotness_score (scalar, 0-1)
```

### Tại sao đổi từ LSTM → GRU?

| Aspect | LSTM (v1.0) | GRU (v2.0) |
|---|---|---|
| Parameters | ~35,000 (2 layers × 64) | **~8,000** (1 layer × 32) |
| Data samples | ~1,800 sequences | ~1,800 sequences |
| Params/sample ratio | 19:1 (overfit risk) | **4:1** (healthy) |
| Performance | Tương đương trên small data | Tương đương, ít overfit hơn |

### Hyperparameters (v2.0)

| Parameter | v1.0 | v2.0 | Lý do thay đổi |
|---|---|---|---|
| `SEQUENCE_LENGTH` | 6 | **4** | Mỗi province ~48 datapoints, 4 cho nhiều sequences hơn |
| `HIDDEN_SIZE` | 64 | **32** | Giảm params cho small data |
| `NUM_LAYERS` | 2 | **1** | 1 layer đủ cho ~1800 sequences |
| `DROPOUT` | 0.2 | **0.3** | Tăng regularization |
| `LEARNING_RATE` | 0.001 | 0.001 | Giữ nguyên |
| `EPOCHS` | 100 | **150** | Tăng max, early stopping sẽ dừng sớm |
| `PATIENCE` | 15 | **20** | Cho model thêm thời gian converge |
| Loss | MSE | **HuberLoss(δ=0.5)** | Robust hơn với outliers |
| Optimizer | Adam | Adam + **weight_decay=1e-4** | L2 regularization |
| LR Scheduler | factor=0.5, patience=5 | factor=0.5, **patience=7, min_lr=1e-5** | Tránh LR quá nhỏ |

### Training Strategy (v2.0)

- **Scaler**: MinMaxScaler fit trên **TRAIN only**, transform cả train + test
- **Loss**: HuberLoss (δ=0.5) — ít nhạy outliers hơn MSE
- **Gradient Clipping**: max_norm=1.0
- **Early Stopping**: patience=20 epochs
- **Data Split**: time-based split 70/30 (TRƯỚC khi scale)
- **Weight Decay**: 1e-4 (L2 regularization trong optimizer)

---

## Hotness Score

Hotness score được tính từ 14 metrics qua 5 nhóm có trọng số:

```
hotness_score = 0.25 × base_volume
             + 0.35 × engagement
             + 0.20 × sentiment
             + 0.05 × emoji_vibe
             + 0.15 × nlp_richness
```

Logic **giống hệt** XGBoost/RF để đảm bảo so sánh công bằng.

---

## Forecasting Strategy (v2.0 — Fixed)

Dự báo 12 tháng bằng **recursive autoregressive**:

1. Lấy 4 tháng cuối cùng (real data, **đã scaled**) làm context
2. Predict tháng tiếp theo
3. Shift window: bỏ tháng cũ nhất, thêm prediction vào cuối
4. Cập nhật features cho row mới:
   - `month_sin/cos`: tính raw → **scale qua scaler params**
   - `hotness_lag_1`: predicted value → **scale qua scaler params**
   - `hotness_lag_2/3`: shift từ lag_1/2 cũ (đã scaled)
   - `hotness_lag_12`: **cập nhật từ history** (nếu có ≥12 predictions)
   - `hotness_rolling_avg_3m`: **tính lại** từ 3 predictions gần nhất → scale
   - `prev_*` features: giữ giá trị cuối cùng (giả định xu hướng ổn định)
5. Lặp lại bước 2-4 cho 12 tháng

### Key fix: `_scale_single_value()`

```python
def _scale_single_value(value, feature_idx, scaler):
    min_val = scaler.data_min_[feature_idx]
    range_val = scaler.data_range_[feature_idx]
    return (value - min_val) / range_val
```

Đảm bảo mọi giá trị mới đều được scale đúng cách trước khi đưa vào model.

---

## Output

### Bảng Iceberg
- **Table**: `gold.gold.province_month_forecast_lstm_next12`
- **Partition**: `year`, `month`

### Parquet trên MinIO
- **Path**: `s3://gold/ml_forecast/province_hotness_forecast_lstm_YYYYMMDD_HHMMSS/`
- Gradio app đọc folder này (match pattern `province_hotness_forecast_lstm_`)

### MLflow Artifacts
- Model weights (PyTorch GRU)
- Feature scaler (pickle)
- Feature config (JSON)
- Plots: loss curve, actual vs predicted, residuals, time series sample

---

## Cách chạy

### Chạy trực tiếp trên Spark
```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.executor.memory=2g \
    --conf spark.executor.cores=2 \
    /opt/spark/jobs/ml/train_lstm_forecast.py
```

### Chạy qua Airflow
Thêm task vào Gold DAG Phase 4 (ML Training):
```python
train_lstm = BashOperator(
    task_id='train_lstm_model',
    bash_command=build_spark_command(
        'ml/train_lstm_forecast.py',
        resource_level='heavy'
    )
)
```

---

## Dependencies

Cần thêm vào Docker image Spark:
```dockerfile
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu
RUN pip install mlflow[pytorch]
```

---

## Rủi ro & Mitigation

| Rủi ro | Mitigation |
|---|---|
| **Overfitting** (small data ~1800 sequences) | GRU nhỏ (8K params), dropout=0.3, weight_decay, early stopping |
| **Recursive error accumulation** (forecast xa) | Giống XGBoost/RF, update lag12 + rolling avg |
| **Lagged features = delayed signal** | Cần tradeoff: accuracy vs no-leakage |
| **percent_rank thay đổi khi data mới** | Retrain model khi có data mới (monthly) |

---

## Liên quan

- [Airflow DAGs README](../airflow/dags/README.md)
- XGBoost model: `spark/jobs/ml/train_and_forecast.py`
- Random Forest model: `spark/jobs/ml/train_random_forest_model.py`
- Feature aggregation: `spark/jobs/gold/TrainingModel/train_province_model.py`
- NLP extraction: `spark/jobs/gold/fact_comment_nlp_engagement/`
- Gradio app: `gradio/app_forecast.py` (đã thêm LSTM vào MODELS dict)

---

**Last Updated**: March 11, 2026 (v2.0)
