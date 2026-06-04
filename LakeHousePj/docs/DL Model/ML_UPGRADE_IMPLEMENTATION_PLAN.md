# Implementation Plan: Nâng cấp ML Pipeline — Tourism Trend Forecasting

> **Tạo ngày:** 2026-05-30  
> **Cập nhật:** 2026-06-03  
> **Trạng thái:** Đã được duyệt — sẵn sàng thực thi  
> **Workspace:** `d:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\LakeHousePj`

---

## Tổng quan

**Vấn đề hiện tại:**
- Target variable `hotness_score` là self-constructed index từ TikTok signals → circular reasoning, không defensible
- Hotel data (1.55M reviews) chưa được tận dụng đúng vai trò: chỉ dùng avg_score, bỏ qua volume và traveler_type
- `fact_province_month_dl_features` và `province_month_forecast_lstm_next12` chưa tồn tại — LSTM chưa bao giờ chạy
- `fact_hotel_review_daily` bị inflate gấp đôi (APPEND bug) → **ĐÃ FIX** (đổi sang `overwritePartitions()`)

**Mục tiêu sau khi hoàn thành:**
- Target variable rõ ràng: `hotel_review_volume` (lượt đặt phòng thực tế theo `stay_date`) = proxy đo lường du lịch thực
- Tích hợp đầy đủ hotel signals: volume growth + traveler_type distribution theo tháng
- LSTM train xong, forecast 12 tháng, so sánh được với legacy XGBoost/RF
- Câu chuyện học thuật rõ và defensible về mặt khoa học (xem chi tiết mục bên dưới)

---

## Câu chuyện học thuật (Academic Narrative)

Câu chuyện chính được chọn:

> **"Social Media as a Leading Indicator of Tourism Demand"**
>
> *TikTok viral signals (sentiment, engagement, volume) xuất hiện **trước** hành vi đặt phòng — người xem TikTok về Đà Lạt tháng 1, rồi đặt phòng tháng 3. Nếu LSTM học được độ trễ này từ lag features → có giá trị dự báo thực sự và defensible về học thuật.*

Các câu chuyện bổ sung có thể tích hợp vào báo cáo:

| # | Câu chuyện | Đóng góp chính | Dữ liệu cần |
|---|---|---|---|
| 1 | **Leading Indicator** (chính) | TikTok buzz → dự báo booking volume | Có sẵn |
| 2 | **Recovery & Seasonality** | Phân tích phục hồi sau COVID, mùa vụ theo tỉnh | Có sẵn (2019-2025) |
| 3 | **Destination Competitiveness** | Tỉnh nào đang nổi lên vs bão hòa | Có sẵn sau Phase 1 |
| 4 | **Traveler Segmentation** | Couple/family react khác nhau với social media | Có sẵn sau Phase 1 |
| 5 | **LakeHouse Architecture** | Hệ thống tích hợp dữ liệu heterogeneous | Đã có |

### Phân rã Trend vs Seasonality trong Feature Design

Mô hình cần học được **cả hai tín hiệu** mà không bị lẫn lộn — đây là điểm thiết kế quan trọng cần giải thích trong báo cáo.

**Nhóm features học Mùa vụ (Seasonality):**
- `month_sin` / `month_cos`, `is_peak_season` — định vị thời điểm trong năm
- `hotel_vol_lag_12` — giá trị cùng kỳ năm ngoái (điểm baseline mùa vụ)

**Nhóm features học Xu hướng (Trend & Momentum):**
- `hotel_vol_lag_1`, `hotel_vol_lag_3` — tín hiệu ngắn hạn gần nhất
- `hotel_vol_growth` (month-over-month %) — đo tốc độ tăng trưởng hiện tại
- `hotel_vol_rolling_3m` — xu hướng trung bình 3 tháng gần đây

Cách LSTM kết hợp hai tín hiệu:
> *Nếu tháng 7 năm ngoái Nha Trang có 100k reviews (`lag_12`) và xu hướng tăng trưởng gần đây là +15% (`vol_growth`) → LSTM dự báo tháng 7 năm nay ~115k. Model học được quy luật mùa vụ lẫn xu hướng tăng trưởng trong cùng một pass.*

> [!NOTE]
> **Grain của bảng ML features:** 1 row = 1 tỉnh × 1 tháng (~60 tỉnh × ~72 tháng = ~1,500 rows). Dữ liệu raw (800k comments, 1.55M reviews) được **aggregate trước** theo province-month — đây là thiết kế đúng cho bài toán dự báo monthly trend, không cần học từng comment riêng lẻ.

---

## Quyết định đã xác nhận

> [!IMPORTANT]
> **Target Variable:** `hotel_review_volume` (log-normalized) làm target cho LSTM.  
> `hotness_score` **không bị xóa** — giữ lại như **input feature** (social attention signal), không còn là target nữa.

> [!NOTE]
> **Normalize hotel_review_volume:** Option A — `log(volume + 1)` — đơn giản, interpretable, MinMaxScaler xử lý được.

---

## Phase 0 — Fix Bugs ✅ (Đã hoàn thành một phần)

### [DONE] Fix `fact_hotel_review_daily` APPEND bug

```python
# fact_hotel_review_job.py — Line ~200
# TRƯỚC (bug):
df.writeTo(GOLD_TABLE_FULL).using("iceberg").append()

# SAU (đã fix):
df.writeTo(GOLD_TABLE_FULL).using("iceberg").overwritePartitions()
```

### [TODO] Reset data cũ bị inflate (3.1M → 1.55M)

```bash
# Bước 1: Xóa data cũ trong MinIO
docker exec lakehouse_minio mc rm --recursive --force local/gold/lakehouse/gold.db/fact_hotel_review_daily/

# Bước 2: Drop table trong Gold catalog
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "DROP TABLE IF EXISTS gold.gold.fact_hotel_review_daily;"

# Bước 3: Re-run job với code mới
docker exec lakehouse_spark_master python3 /opt/spark/jobs/gold/fact_hotel_review/fact_hotel_review_job.py

# Verify: kỳ vọng ~1,551,297 rows
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SELECT COUNT(*) FROM gold.gold.fact_hotel_review_daily;"
```

### [TODO] Investigate `dim_comment` inflate

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SELECT COUNT(*), COUNT(DISTINCT post_url_nk, stt) FROM gold.gold.dim_comment;"
```

Nếu `COUNT(*) > COUNT(DISTINCT composite_key)` → có duplicate → xem xét fix MERGE logic.

---

## Phase 1 — Feature Engineering Update

**File:** `spark/jobs/gold/fact_dl_features/fact_dl_features_job.py`

### Thay đổi 1: Thêm traveler_type distribution vào `aggregate_hotel_reviews()`

```python
# Thêm vào aggregate_hotel_reviews():
df_travel = spark.table("gold.gold.dim_travel_type").select("travel_type_sk", "travel_type_name")

df = df_reviews.join(df_hotels, "hotel_sk").join(df_dates, ...).join(df_travel, "travel_type_sk", "left")

total_col = F.count("fact_id")

agg = df.groupBy("province_sk", "year_month").agg(
    # --- existing ---
    F.count("fact_id").alias("hotel_review_volume"),
    F.avg("review_score").alias("avg_hotel_score"),
    F.stddev("review_score").alias("hotel_score_std"),
    # ...

    # --- NEW: Traveler type distribution ---
    (F.sum(F.when(F.col("travel_type_name").contains("Cặp đôi"), 1).otherwise(0))
     / total_col).alias("couple_ratio"),
    (F.sum(F.when(F.col("travel_type_name").contains("Gia đình"), 1).otherwise(0))
     / total_col).alias("family_ratio"),
    (F.sum(F.when(F.col("travel_type_name").contains("Công tác"), 1).otherwise(0))
     / total_col).alias("business_ratio"),
    (F.sum(F.when(F.col("travel_type_name").contains("Một mình"), 1).otherwise(0))
     / total_col).alias("solo_ratio"),
)
```

### Thay đổi 2: Thêm hotel volume growth signals trong `build_combined_features()`

```python
window_hotel = Window.partitionBy("province_sk").orderBy("year_month")

df = df.withColumn("hotel_vol_lag_1",  F.lag("hotel_review_volume", 1).over(window_hotel))
df = df.withColumn("hotel_vol_lag_3",  F.lag("hotel_review_volume", 3).over(window_hotel))
df = df.withColumn("hotel_vol_lag_12", F.lag("hotel_review_volume", 12).over(window_hotel))

df = df.withColumn("hotel_vol_growth",   # month-over-month % change
    F.when(F.col("hotel_vol_lag_1") > 0,
           (F.col("hotel_review_volume") - F.col("hotel_vol_lag_1")) / F.col("hotel_vol_lag_1")
    ).otherwise(F.lit(None))
)

df = df.withColumn("hotel_vol_rolling_3m",
    F.avg("hotel_review_volume").over(
        Window.partitionBy("province_sk").orderBy("year_month").rowsBetween(-3, -1)
    )
)
```

### Thay đổi 3: Giữ `hotness_score` nhưng đổi vai trò

- Không còn là **target** của LSTM
- Trở thành **input feature** (social attention composite index)

### Thay đổi 4: Cập nhật schema bảng

```python
# Thêm vào create_output_table():
# Group 4b: Hotel Demand Dynamics (5 cột mới)
StructField("couple_ratio",          DoubleType(), True),
StructField("family_ratio",          DoubleType(), True),
StructField("business_ratio",        DoubleType(), True),
StructField("solo_ratio",            DoubleType(), True),
StructField("hotel_vol_growth",      DoubleType(), True),
StructField("hotel_vol_lag_1",       LongType(),   True),
StructField("hotel_vol_lag_3",       LongType(),   True),
StructField("hotel_vol_lag_12",      LongType(),   True),
StructField("hotel_vol_rolling_3m",  DoubleType(), True),
```

---

## Phase 2 — Model Update

**File:** `spark/jobs/ml/train_lstm_forecast.py`

### Thay đổi 1: Đổi TARGET_COL

```python
# Hiện tại:
TARGET_COL = "hotness_score"

# Thay bằng:
TARGET_COL = "hotel_review_volume"
```

### Thay đổi 2: Cập nhật danh sách features

```python
HOTEL_DEMAND_FEATURES = [
    "couple_ratio", "family_ratio", "business_ratio", "solo_ratio",
    "hotel_vol_growth", "hotel_vol_rolling_3m",
    # lag features của hotel_review_volume (thay thế hotness lags)
]

# Giữ lại hotness_score như 1 feature (social attention signal):
SOCIAL_FEATURES = [
    "hotness_score",       # social attention composite
    "avg_sentiment", "positive_ratio", "negative_ratio",
    "avg_likes_per_post", "viral_post_ratio", "engagement_score",
    # ...
]
```

### Thay đổi 3: Log Normalization

```python
import numpy as np

# Trong prepare_sequences():
df[TARGET_COL] = np.log1p(df[TARGET_COL])  # log(x + 1)

# Sau khi inverse transform để lấy kết quả thực:
# predicted_volume = np.expm1(predicted_log_volume)
```

### Thay đổi 4: Cập nhật output table schema đầy đủ

Bảng `gold.gold.province_month_forecast_lstm_next12` — mỗi row là 1 tỉnh × 1 tháng dự báo:

| Cột | Kiểu | Nguồn gốc |
|---|---|---|
| `province_sk` | Long | Copy từ dòng lịch sử cuối cùng của tỉnh |
| `province_name` | String | Copy từ `dim_province` qua lịch sử |
| `region` | String | Copy từ `dim_province` qua lịch sử |
| `year` | Integer | Tính bằng code: `last_month + horizon` |
| `month` | Integer | Tính bằng code: `last_month + horizon` |
| `year_month` | Integer | Tính bằng code: ví dụ `202607` |
| `horizon_month` | Integer | Vòng lặp `1 → 12` (số tháng trong tương lai) |
| `predicted_hotel_volume` | Double | **Mô hình LSTM tính ra** (log-scale) |
| `predicted_hotel_volume_actual` | Double | `np.expm1(predicted_hotel_volume)` — số phòng thực tế |
| `predicted_growth_pct` | Double | `(pred_actual - last_actual) / last_actual × 100` |
| `forecast_date` | String | `datetime.now()` — ngày chạy mô hình |
| `model_version` | String | Hardcode: `'lstm_v3_volume'` |

```python
# Schema đầy đủ:
schema = StructType([
    StructField("province_sk",                   LongType(),    False),
    StructField("province_name",                 StringType(),  False),
    StructField("region",                        StringType(),  False),
    StructField("year",                          IntegerType(), False),
    StructField("month",                         IntegerType(), False),
    StructField("year_month",                    IntegerType(), False),
    StructField("horizon_month",                 IntegerType(), False),
    StructField("predicted_hotel_volume",        DoubleType(),  True),  # log-scale
    StructField("predicted_hotel_volume_actual", DoubleType(),  True),  # expm1
    StructField("predicted_growth_pct",          DoubleType(),  True),  # %
    StructField("forecast_date",                 StringType(),  False),
    StructField("model_version",                 StringType(),  False),
])

# Logic sinh ra 1 dòng output (trong vòng lặp forecast):
last_actual_volume = group["hotel_review_volume"].iloc[-1]  # tháng cuối có dữ liệu thực

for horizon in range(1, 13):
    tym, ty, tm = calculate_next_month(last_ym, horizon)    # tính tháng tương lai
    pred_log    = model(last_sequence)                       # LSTM dự đoán (log)
    pred_actual = np.expm1(pred_log)                         # chuyển về số thực
    growth_pct  = (pred_actual - last_actual_volume) / last_actual_volume * 100

    results.append({
        'province_sk': province_sk,        # từ lịch sử
        'province_name': province_name,    # từ lịch sử
        'region': region,                  # từ lịch sử
        'year': ty,                        # tính ra từ last_ym + horizon
        'month': tm,                       # tính ra từ last_ym + horizon
        'year_month': tym,                 # tính ra từ last_ym + horizon
        'horizon_month': horizon,          # vòng lặp 1..12
        'predicted_hotel_volume': pred_log,
        'predicted_hotel_volume_actual': pred_actual,
        'predicted_growth_pct': growth_pct,
        'forecast_date': datetime.now().strftime('%Y-%m-%d'),
        'model_version': 'lstm_v3_volume'
    })
```

### Thay đổi 5: Evaluation — Ground Truth và cách đo hiệu suất

**Target label để đánh giá mô hình là `hotel_review_volume` thực tế trên tập Test (30% cuối).**

Dữ liệu chia theo thời gian (Time-based split, không random) để tránh data leakage:
- **Train set (70% đầu):** Khoảng 2019–2023 — mô hình học pattern từ đây
- **Test set (30% cuối):** Khoảng 2024–2025 — dùng để chấm điểm, mô hình **chưa từng thấy**

```python
# So sánh trên tập Test:
# y_true = hotel_review_volume thực tế từ Booking.com (ground truth)
# y_pred = giá trị mô hình LSTM dự đoán

def mean_absolute_percentage_error(y_true, y_pred):
    mask = y_true > 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

# Ví dụ 1 điểm data:
# y_true = 40,000  (thực tế tháng 7/2025 Nha Trang — từ Booking.com)
# y_pred = 38,000  (LSTM dự đoán)
# MAPE điểm này = |40,000 - 38,000| / 40,000 × 100 = 5%

# Report đầy đủ: RMSE, MAE, R², MAPE
```

**Ngưỡng hiệu suất chấp nhận được:**
```
R²   > 0.70   (model giải thích được >70% variance)
MAPE < 30%    (sai số tương đối trung bình <30%)
RMSE thấp hơn naive baseline (predict = giá trị tháng trước)
```

---

## Phase 3 — Run Pipeline

Thứ tự chạy bắt buộc:

```
1. [Fix]    Re-run fact_hotel_review_daily  (Phase 0 — đã fix code, cần reset data)
             → Verify COUNT(*) = ~1,551,297

2. [Create] fact_province_month_dl_features
             → docker exec lakehouse_spark_master python3 \
                  /opt/spark/jobs/gold/fact_dl_features/fact_dl_features_job.py

3. [Train]  LSTM
             → docker exec lakehouse_spark_master python3 \
                  /opt/spark/jobs/ml/train_lstm_forecast.py

4. [Verify] Kiểm tra output
             → SELECT COUNT(*), MIN(year_month), MAX(year_month)
                 FROM gold.gold.province_month_forecast_lstm_next12;
```

**Kỳ vọng metrics tốt:**
```
R²   > 0.70   (model giải thích được >70% variance)
MAPE < 30%    (sai số tương đối trung bình <30%)
RMSE thấp hơn naive baseline (predict = last month's value)
```

---

## Phase 4 — PhoBERT NLP (Optional)

> [!NOTE]
> Phase này không bắt buộc. LSTM vẫn chạy tốt với NLP v1 (underthesea). Chỉ làm nếu còn thời gian.

```
1. Chạy weak_labeling.py    → export labeled data to S3
2. Chạy train_phobert.py    → register model vào MLflow
3. Chạy inference_phobert.py → tạo fact_comment_nlp_v2
4. Re-run fact_dl_features_job.py → aspect features sẽ có giá trị thực (≠ 0)
5. Re-train LSTM với features đầy đủ hơn
```

**Lợi ích:** 6 aspect features (`avg_aspect_scenery`, `avg_aspect_food`, ...) sẽ không còn = 0 nữa.

---

## Phase 5 — So sánh & Report

### So sánh 3 models

| Model | Bảng output | Ghi chú |
|---|---|---|
| XGBoost (legacy) | `province_month_forecast_next12` | Đã có |
| Random Forest (legacy) | `province_month_forecast_rf_next12` | Đã có |
| **LSTM (mới)** | `province_month_forecast_lstm_next12` | Sẽ tạo |

### Visualizations cần có trong báo cáo

1. **Timeline plot:** Actual hotel_review_volume (2019-2025) + LSTM forecast (2025-2026) cho top 5 tỉnh
2. **Province heatmap:** Forecast tháng 7/2026 (mùa hè) per tỉnh
3. **Model comparison table:** RMSE/MAE/MAPE của LSTM vs XGBoost vs RF
4. **Feature importance:** Attention weights của LSTM (seasonal patterns)
5. **Traveler type seasonality:** couple_ratio, family_ratio theo tháng cho top tỉnh

---

## Verification Plan

### Automated Checks

```bash
# 1. Sau Phase 0: fact_hotel_review_daily fixed
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SELECT COUNT(*) FROM gold.gold.fact_hotel_review_daily;"
# Expected: ~1,551,297

# 2. Sau Phase 1: feature table exists
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SELECT COUNT(*), COUNT(DISTINCT province_sk), MIN(year_month), MAX(year_month),
             AVG(hotel_review_volume), AVG(couple_ratio), AVG(hotel_vol_growth)
      FROM gold.gold.fact_province_month_dl_features;"
# Expected: ~1500+ rows, 60+ provinces, couple_ratio NOT all 0

# 3. Sau Phase 3: forecast exists
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SELECT province_name, year_month, predicted_hotel_volume_actual, predicted_growth_pct
      FROM gold.gold.province_month_forecast_lstm_next12
      ORDER BY predicted_hotel_volume_actual DESC LIMIT 20;"
```

### Manual Verification (Sanity Check)

- **Sanity check:** Đà Lạt, Đà Nẵng, Hội An, Hạ Long phải nằm trong top 10 tỉnh về `predicted_hotel_volume`
- **Seasonal check:** Tháng 6-8 (mùa hè) phải có `predicted_hotel_volume` cao hơn tháng 1-2
- **Growth check:** `predicted_growth_pct` không có giá trị vô lý (>500% hay <-90%)

---

## Tóm tắt thay đổi theo file

| File | Trạng thái | Thay đổi chính |
|---|---|---|
| `fact_hotel_review/fact_hotel_review_job.py` | ✅ ĐÃ FIX | APPEND → OVERWRITE |
| `fact_dl_features/fact_dl_features_job.py` | TODO | Thêm traveler_type features, hotel volume growth, update schema |
| `ml/train_lstm_forecast.py` | TODO | Đổi TARGET_COL, log transform, new features, update output schema |
| Airflow DAG | TODO (nhỏ) | Thêm step fact_dl_features vào dependency chain |

**Files KHÔNG thay đổi:**
- Silver layer (tất cả) — dữ liệu nguồn giữ nguyên
- Gold dimensions (tất cả) — schema không đổi
- NLP pipeline (`nlp/`) — chạy độc lập, không bắt buộc

---

## Timeline ước tính

| Phase | Thời gian | Rủi ro |
|---|---|---|
| Phase 0: Fix bugs | ✅ Code done, cần reset data (~1-2 giờ) | Thấp |
| Phase 1: Feature engineering | 1-2 ngày | Vừa — traveler_type join có thể cần debug |
| Phase 2: Model update | 0.5-1 ngày | Thấp — thay đổi có hệ thống |
| Phase 3: Run pipeline | 2-4 giờ | Vừa — OOM nếu data lớn |
| Phase 4: PhoBERT (optional) | 1-2 ngày | Cao — train thời gian dài |
| Phase 5: Report | 2-3 ngày | Thấp |
| **Total (không PhoBERT)** | **~4-5 ngày** | |
| **Total (có PhoBERT)** | **~6-8 ngày** | |
