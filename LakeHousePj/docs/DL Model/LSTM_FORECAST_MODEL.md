# Deep Learning Model — Province Hotel Review Volume Forecasting (v3.0)
========================================================================

## Tổng quan

Model LSTM + Attention dự báo lượng đặt phòng khách sạn (thông qua proxy số lượng review `hotel_review_volume`) của các tỉnh/thành Việt Nam trong 12 tháng tiếp theo, sử dụng dữ liệu đa nguồn từ TikTok (engagement + NLP) và Booking.com (hotel reviews).

**File**: `spark/jobs/ml/train_lstm_forecast.py`

### v3.0 Fixes & Upgrades (so với v2.0/v1.0)

| # | Vấn đề trước đây | Cải tiến v3.0 |
|---|---|---|
| 1 | **Target Variable mơ hồ**: dùng `hotness_score` tự định nghĩa | Target cụ thể: **`hotel_review_volume`** (đại diện lượng đặt phòng thực) |
| 2 | **Data leakage**: current features chứa thông tin target | Khắc phục: Current metrics **LAG 1 tháng** (`prev_*`) |
| 3 | **Scaler leak**: MinMaxScaler fit trên cả test | Scaler **fit trên train only** |
| 4 | **Forecast scale sai**: lag dùng unscaled predicted | Tất cả giá trị **scale đúng** qua `_scale_single_value()` |
| 5 | **rolling_avg/lag_12 không update**: giữ giá trị cũ suốt 12 bước | **Update động** sau mỗi bước forecast đệ quy |
| 6 | **Biến động volume lớn**: volume thực tế lệch lớn giữa các tỉnh | Áp dụng **Log-normalization (`log1p`)** cho target, dự báo xong dùng **`expm1`** để trả về scale thực tế |
| 7 | **Loss function**: MSE nhạy outliers | **HuberLoss** (delta=0.5) cho robustness |
| 8 | **Chưa tích hợp Gradio**: MODELS dict cũ | **Tạo riêng dashboard mới** [app_lstm_volume.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/gradio/app_lstm_volume.py) kết nối MinIO và MLflow |

---

## Vị trí trong kiến trúc Lakehouse

```
Bronze (Raw CSV)
  → Silver (Clean Iceberg tables)
    → Gold (Dimension + Fact tables)
      → fact_province_month_dl_features (15 features + traveler ratios)
        → train_lstm_forecast.py  ← BẠN ĐANG Ở ĐÂY
          → gold.gold.province_month_forecast_lstm_next12 (output table)
          → MinIO Parquet (Gradio đọc)
          → MLflow Server Registry (model registry + artifacts)
```

---

## So sánh 3 models

| Tiêu chí | XGBoost Reduced | Random Forest | LSTM Deep Learning (v3.0) |
|---|---|---|---|
| File | `train_and_forecast.py` | `train_random_forest_model.py` | `train_lstm_forecast.py` |
| Loại | Tree-based (Boosting) | Tree-based (Bagging) | **Deep Learning (Sequence)** |
| Số features | 8 | 8 | **30** |
| Target | `hotness_score` | `hotness_score` | **`hotel_review_volume`** (log-scale) |
| Temporal features | 3 (month, sin, cos) | 3 | 2 (sin, cos only) |
| Lag features | 5 (lag_1/2/3/12, rolling) | 5 | 6 (hotel vol lags) |
| Current metrics | **Không dùng** | **Không dùng** | **engagement + NLP features** |
| Cross-source | Chỉ TikTok | Chỉ TikTok | **TikTok + Booking** |
| Input format | Tabular (1 row) | Tabular (1 row) | **Sequence (4 tháng)** |
| Output table | `forecast_reduced_next12` | `forecast_rf_next12` | `province_month_forecast_lstm_next12` |

---

## Data Sources

### 1. TikTok Data (qua `gold.gold.fact_province_month_dl_features`)
Tổng hợp từ `fact_comment_nlp_engagement` + `fact_province_content_engagement` để trích xuất các đặc trưng tương tác và cảm xúc cộng đồng trên mạng xã hội.

### 2. Booking.com Data (qua `gold.gold.fact_hotel_review_daily`)
Join với `dim_hotel`, `dim_date`, và `dim_travel_type` để tính:
- Lượng đặt phòng khách sạn theo tháng (`hotel_review_volume`).
- Tỉ lệ phân bổ các loại du khách (`couple_ratio`, `family_ratio`, `business_ratio`, `solo_ratio`).

---

## 15 Features huấn luyện

### Temporal Features (2)
* `month_sin`: sin(2π × month / 12) — mã hóa tính tuần hoàn
* `month_cos`: cos(2π × month / 12) — mã hóa tính tuần hoàn

### Lag Features (5) của Target (Volume)
* `hotel_vol_lag_1`: Volume đặt phòng tháng trước
* `hotel_vol_lag_2`: Volume đặt phòng 2 tháng trước
* `hotel_vol_lag_3`: Volume đặt phòng 3 tháng trước
* `hotel_vol_lag_12`: Volume đặt phòng cùng tháng năm trước (seasonality)
* `hotel_vol_rolling_3m`: Trung bình volume 3 tháng trước đó

### Lagged Current Features (8) — LAG 1 tháng tránh data leakage
* `prev_engagement` (TikTok): 0.4 × post_likes + 0.35 × post_saves + 0.25 × comment_likes
* `prev_sentiment` (TikTok NLP): 0.5 × positive_ratio + 0.3 × avg_sentiment + 0.2 × (1-negative_ratio)
* `prev_volume` (TikTok): 0.6 × total_posts + 0.4 × total_comments
* `prev_nlp_richness` (TikTok NLP): 0.45 × avg_words + 0.45 × unique_word_ratio + 0.10 × exclamation_ratio
* `prev_emoji_vibe` (TikTok NLP): 0.5 × emoji_count + 0.5 × emoji_sentiment
* `prev_comment_engagement` (TikTok): percent_rank(avg_comment_likes) tháng trước
* `prev_hotel_score` (Booking.com): percent_rank(avg_hotel_score) tháng trước
* `prev_hotel_volume` (Booking.com): percent_rank(hotel_review_volume) tháng trước

---

## Kiến trúc Model

```
Input: (batch_size, sequence_length=4, num_features=30)
  │
  ▼
LSTM Layer (input=30, hidden=32, 1 layer, batch_first=True)
  │
  ▼
LayerNorm(32)
  │
  ▼
Temporal Attention
  │  scores = Linear(32 → 1) per timestep
  │  weights = softmax(scores)
  │  context = weighted sum of LSTM outputs
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
Output: predicted log-volume (scalar)
```

---

## Chiến lược Dự báo Tịnh tiến (Recursive Forecasting)

Dự báo 12 tháng kế tiếp sử dụng mô hình tự hồi quy đệ quy:
1. Lấy chuỗi 4 tháng cuối cùng làm context (đã scaled).
2. Dự báo giá trị log-volume tháng tiếp theo.
3. Thực hiện tịnh tiến cửa sổ (shift window): bỏ tháng cũ nhất, đẩy giá trị dự báo mới vào cuối.
4. Cập nhật các biến lag động cho bước kế tiếp:
   - `month_sin/cos`: tính toán lại theo lịch.
   - `hotel_vol_lag_1`: nhận giá trị vừa dự báo (scaled).
   - `hotel_vol_lag_2/3`: dịch chuyển từ các lag trước đó.
   - `hotel_vol_lag_12`: truy vấn từ lịch sử thực tế của tỉnh đó (seasonality).
   - `hotel_vol_rolling_3m`: tính lại từ 3 tháng gần nhất.
   - `prev_*` features: giữ cố định bằng giá trị tháng cuối cùng (giả định tính ổn định ngắn hạn).
5. Lặp lại bước 2-4 cho 12 tháng.
6. Dùng hàm **`expm1`** đưa tất cả kết quả dự báo từ log-scale về thang đo volume thực tế để lưu trữ và hiển thị.

---

## Kết quả đầu ra (Outputs)

### 1. Bảng Iceberg
* **Tên bảng**: `gold.gold.province_month_forecast_lstm_next12`
* **Cấu trúc cột chính**: `province_sk`, `province_name`, `region`, `year`, `month`, `year_month`, `horizon_month` (1-12), `predicted_hotel_volume_actual` (volume thực tế sau expm1), `predicted_growth_pct`, `forecast_date`.

### 2. Parquet trên MinIO
* **Đường dẫn**: `s3a://gold/ml_forecast/province_hotel_volume_forecast_lstm_YYYYMMDD_HHMMSS/`
* Được đọc trực tiếp bởi file [app_lstm_volume.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/gradio/app_lstm_volume.py).

### 3. MLflow Tracking
* Lưu trữ loss curves, residual plots, parameters và model registry dưới tên `province_hotel_volume_forecaster_lstm`.

---

## Cách chạy

### Chạy trực tiếp trên Spark Master
```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.executor.memory=2g \
    --conf spark.executor.cores=2 \
    /opt/spark/jobs/ml/train_lstm_forecast.py
```

**Last Updated**: June 3, 2026 (v3.0)
