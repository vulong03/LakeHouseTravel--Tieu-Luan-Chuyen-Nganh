# Deep Learning Model — Province Hotel Review Volume Forecasting (v5.0)
========================================================================

## Tổng quan

Model LSTM + Attention dự báo lượng đặt phòng khách sạn (thông qua proxy số lượng review `hotel_review_volume`) của các tỉnh/thành Việt Nam trong 12 tháng tiếp theo, sử dụng dữ liệu đa nguồn từ TikTok (engagement + NLP) và Booking.com (hotel reviews).

**File**: `spark/jobs/dl/train_province_lstm_v5.py`

### v5.0 Fixes & Upgrades (so với v4.0/v3.0)

| # | Vấn đề trước đây | Cải tiến v5.0 |
|---|---|---|
| 1 | **Target/Val/Test Split kém khách quan**: v4 dùng test_loader làm val cho early stopping | **3-way split (70/15/15 time-based)**: train học weights, val dùng early stopping, test chỉ dùng 1 lần report final metrics |
| 2 | **Outlier clipping leakage**: tính p99 trên toàn bộ data trước khi split | **Tính p99 trên train only**, apply chung ngưỡng cho val/test |
| 3 | **Thiếu hotness_lag features**: chỉ dùng hotness_score hiện tại | **Thêm hotness_lag_1/2/3/12, rolling_3m, momentum** (bằng chứng cho tác động trễ của TikTok) |
| 4 | **Thiếu PhoBERT aspect features**: bỏ qua các cột aspect | **Thêm 6 aspect features** (scenery, food, price, service, transport, accommodation) |
| 5 | **Scheduler.step() bug**: scheduler.step(epoch + loss) nhận sai tham số | **Sửa thành scheduler.step(epoch)** đúng API CosineAnnealingWarmRestarts |
| 6 | **growth_pct bug trong forecast**: so sánh growth với tháng cuối lịch sử cố định | **Cập nhật prev_volume mỗi bước horizon** trong loop đệ quy |
| 7 | **Dư thừa lớp BatchNorm1d**: khai báo nhưng bị comment out | **Xóa hoàn toàn self.bn** |
| 8 | **Chưa tích hợp Gradio**: model registry cũ | **Tích hợp dashboard** kết nối MinIO và MLflow |

---

## Vị trí trong kiến trúc Lakehouse

```
Bronze (Raw CSV)
  → Silver (Clean Iceberg tables)
    → Gold (Dimension + Fact tables)
      → fact_province_month_dl_features (50 features + traveler ratios)
        → train_province_lstm_v5.py  ← BẠN ĐANG Ở ĐÂY
          → gold.gold.province_month_forecast_lstm_next12 (output table)
          → MinIO Parquet (Gradio đọc)
          → MLflow Server Registry (model registry + artifacts)
```

---

## So sánh 3 models

| Tiêu chí | XGBoost Reduced | Random Forest | LSTM Deep Learning (v5.0) |
|---|---|---|---|
| File | `train_and_forecast.py` | `train_random_forest_model.py` | `train_province_lstm_v5.py` |
| Loại | Tree-based (Boosting) | Tree-based (Bagging) | **Deep Learning (Sequence)** |
| Số features | 8 | 8 | **50** |
| Target | `hotness_score` | `hotness_score` | **`hotel_review_volume`** (log-scale) |
| Temporal features | 3 (month, sin, cos) | 3 | 2 (sin, cos only) |
| Lag features | 5 (lag_1/2/3/12, rolling) | 5 | 12 (6 hotel + 6 hotness lags) |
| Current metrics | **Không dùng** | **Không dùng** | **engagement + NLP + aspect features** |
| Cross-source | Chỉ TikTok | Chỉ TikTok | **TikTok + Booking** |
| Input format | Tabular (1 row) | Tabular (1 row) | **Sequence (3 tháng)** |
| Output table | `forecast_reduced_next12` | `forecast_rf_next12` | `province_month_forecast_lstm_next12` |

---

## Data Sources

### 1. TikTok Data (qua `gold.gold.fact_province_month_dl_features`)
Tổng hợp từ `fact_comment_nlp_v2` + `fact_province_content_engagement` để trích xuất các đặc trưng tương tác, cảm xúc cộng đồng và phân tích khía cạnh bằng PhoBERT.

### 2. Booking.com Data (qua `gold.gold.fact_hotel_review_daily`)
Join với `dim_hotel`, `dim_date`, và `dim_travel_type` để tính:
- Lượng đặt phòng khách sạn theo tháng (`hotel_review_volume`).
- Tỉ lệ phân bổ các loại du khách (`couple_ratio`, `family_ratio`, `business_ratio`, `solo_ratio`).

---

## 50 Features huấn luyện

### Temporal Features (2)
* `month_sin`: sin(2π × month / 12) — mã hóa tính tuần hoàn
* `month_cos`: cos(2π × month / 12) — mã hóa tính tuần hoàn

### Hotel Lag Features (6)
* `hotel_vol_lag_1`, `hotel_vol_lag_2`, `hotel_vol_lag_3`, `hotel_vol_lag_12`, `hotel_vol_rolling_3m`, `hotel_vol_momentum`

### Hotness Lag Features (6)
* `hotness_lag_1`, `hotness_lag_2`, `hotness_lag_3`, `hotness_lag_12`, `hotness_rolling_3m`, `hotness_momentum`

### Volume Features (5)
* `total_posts`, `total_comments`, `total_hotel_reviews`, `unique_authors`, `comments_per_post`

### Engagement Features (5)
* `avg_likes_per_post`, `avg_saves_per_post`, `viral_post_ratio`, `engagement_score`, `hotness_score`

### NLP Features (8)
* `avg_sentiment`, `sentiment_std`, `positive_ratio`, `negative_ratio`, `avg_word_count`, `avg_unique_word_ratio`, `emoji_sentiment_ratio`, `reply_ratio`

### Aspect Features (6)
* `avg_aspect_scenery`, `avg_aspect_food`, `avg_aspect_price`, `avg_aspect_service`, `avg_aspect_transport`, `avg_aspect_accommodation`

### Hotel Quality Features (9)
* `avg_hotel_score`, `hotel_score_std`, `high_score_ratio`, `domestic_review_ratio`, `couple_ratio`, `family_ratio`, `business_ratio`, `solo_ratio`, `hotel_vol_growth`

### Custom Features (3)
* `social_to_booking_ratio`, `sentiment_polarity_change`, `hotel_vol_std_rolling_3m`

---

## Kiến trúc Model

```
Input: (batch_size, sequence_length=3, num_features=50)
  │
  ▼
LSTM Layer (input=50, hidden=48, 2 layers, batch_first=True, dropout=0.43)
  │
  ▼
LayerNorm(48)
  │
  ▼
Temporal Attention
  │  scores = Linear(48 → 1) per timestep
  │  weights = softmax(scores)
  │  context = weighted sum of LSTM outputs
  ▼
Context Vector: (batch_size, 48)
  │
  ▼
Dense(48 → 48) → GELU → Dropout(0.43) → Dense(48 → 16) → ReLU
  │
  ▼
Dense(16 → 1)
  │
  ▼
Output: predicted log-volume (scalar)
```

---

## Chiến lược Dự báo Tịnh tiến (Recursive Forecasting)

Dự báo 12 tháng kế tiếp sử dụng mô hình tự hồi quy đệ quy:
1. Lấy chuỗi 3 tháng cuối cùng làm context (đã scaled).
2. Dự báo giá trị log-volume tháng tiếp theo.
3. Thực hiện tịnh tiến cửa sổ (shift window): bỏ tháng cũ nhất, đẩy giá trị dự báo mới vào cuối.
4. Cập nhật các biến lag động cho bước kế tiếp:
   - `month_sin/cos`: tính toán lại theo lịch.
   - `hotel_vol_lag_1`: nhận giá trị vừa dự báo (scaled).
   - `hotel_vol_lag_2/3/12`: dịch chuyển và cập nhật từ các dự báo trước đó hoặc lịch sử.
   - `hotel_vol_rolling_3m` và `hotel_vol_momentum`: tính lại động từ cửa sổ dự báo gần nhất.
   - `prev_*` và các features xã hội khác (như `avg_aspect_*`, `hotness_score`...): giữ cố định bằng giá trị tháng cuối cùng có dữ liệu thực tế (giả định tính ổn định ngắn hạn - persistence assumption).
5. Lặp lại bước 2-4 cho 12 tháng.
6. Dùng hàm **`expm1`** đưa tất cả kết quả dự báo từ log-scale về thang đo volume thực tế để lưu trữ và hiển thị.

---

## Kết quả đầu ra (Outputs)

### 1. Bảng Iceberg
* **Tên bảng**: `gold.gold.province_month_forecast_lstm_next12`
* **Cấu trúc cột chính**: `province_sk`, `province_name`, `region`, `year`, `month`, `year_month`, `horizon_month` (1-12), `predicted_hotel_volume_actual` (volume thực tế sau expm1), `predicted_growth_pct`, `forecast_date`.

### 2. Parquet trên MinIO
* **Đường dẫn**: `s3a://gold/dl_forecast/province_hotel_volume_forecast_lstm_v5/`
* Được đọc trực tiếp bởi file [app_lstm_volume.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/gradio/app_lstm_volume.py).

### 3. MLflow Tracking
* Lưu trữ loss curves, residual plots, parameters và model registry dưới tên `province_hotel_volume_forecaster_lstm_v5`.

---

## Cách chạy

### Chạy trực tiếp trên Spark Master
```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.executor.memory=2g \
    --conf spark.executor.cores=2 \
    /opt/spark/jobs/dl/train_province_lstm_v5.py
```

**Last Updated**: June 11, 2026 (v5.0)
