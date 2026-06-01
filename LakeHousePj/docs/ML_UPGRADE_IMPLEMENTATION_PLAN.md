# Implementation Plan: Nâng cấp ML Pipeline — Tourism Trend Forecasting

> **Tạo ngày:** 2026-05-30  
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
- Câu chuyện học thuật rõ: *"TikTok social signals + Booking.com demand signals → predict future hotel booking volume per province"*

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

### Thay đổi 4: Cập nhật output table schema

```python
# Output: province_month_forecast_lstm_next12
StructField("predicted_hotel_volume",        DoubleType(), True),  # log-scale
StructField("predicted_hotel_volume_actual", DoubleType(), True),  # expm1 converted
StructField("predicted_growth_pct",          DoubleType(), True),  # vs last known month
```

### Thay đổi 5: Evaluation metrics

```python
def mean_absolute_percentage_error(y_true, y_pred):
    mask = y_true > 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

# Report: RMSE, MAE, R², MAPE
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
