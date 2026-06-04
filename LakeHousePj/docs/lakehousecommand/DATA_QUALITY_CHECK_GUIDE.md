# Data Quality Check Guide — LakeHouse Du Lịch Việt Nam
> **Mục đích:** Hướng dẫn AI tự viết và chạy lệnh kiểm tra chất lượng dữ liệu trong hệ thống.
> **Cập nhật lần cuối:** 2026-05-30
> **Kết quả baseline:** Đo lần đầu 2026-05-30

---

## 1. Cấu trúc lệnh chuẩn theo Catalog

Hệ thống có **3 catalog** khác nhau — mỗi catalog cần cấu hình `--conf` riêng.

### 1.1 Catalog `lakehouse` (Silver TikTok)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SQL_QUERY_HERE"
```

**Truy cập bảng:** `lakehouse.silver.<table_name>`
**Bảng có sẵn:** `tiktok_videos`, `tiktok_post_metadata`, `tiktok_post_comments`

### 1.2 Catalog `silver` (Silver Hotel)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hive \
  --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SQL_QUERY_HERE"
```

**Truy cập bảng:** `silver.silver.<table_name>`
**Bảng có sẵn:** `hotels_list`, `hotels_detail`, `hotels_reviews`

### 1.3 Catalog `gold` (Gold Layer — từ spark-defaults.conf)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SQL_QUERY_HERE"
```

**Truy cập bảng:** `gold.gold.<table_name>`  
**Không cần thêm conf** — gold catalog đã được cấu hình sẵn trong `spark-defaults.conf` của container.

### 1.4 Truy vấn nhiều catalog cùng lúc

Khi cần JOIN hoặc so sánh dữ liệu giữa Silver + Gold:

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hive \
  --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SQL_QUERY_HERE"
```

---

## 2. Kiểm kê bảng hiện có (Gold Layer)

Lệnh xem toàn bộ bảng trong Gold:

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "SHOW TABLES IN gold.gold;"
```

**Kết quả thực tế (đo 2026-05-30):**

```
Dimensions (10):
  dim_date, dim_province, dim_destination, dim_travel_type, dim_room_type,
  dim_author, dim_country, dim_hotel, dim_post, dim_comment

Facts (3):
  fact_province_content_engagement
  fact_comment_nlp_engagement
  fact_hotel_review_daily

ML Tables (4):
  province_month_features             ← Legacy (XGBoost/RF)
  province_month_forecast_next12      ← Forecast output (legacy model)
  province_month_forecast_reduced_next12
  province_month_forecast_rf_next12

Clustering:
  hotel_clustering_features_v2
  hotel_clustering_results_v2

CHƯA TỒN TẠI (cần chạy pipeline):
  fact_province_month_dl_features     ← Cần chạy fact_dl_features_job.py
  fact_comment_nlp_v2                 ← Cần chạy inference_phobert.py
  province_month_forecast_lstm_next12 ← Cần chạy train_lstm_forecast.py
```

---

## 3. Patterns kiểm tra chất lượng dữ liệu

### Pattern A: Đếm tổng + NULL trên key columns

```sql
SELECT
  COUNT(*) as total_rows,
  COUNT(key_column) as non_null_key,      -- đếm NOT NULL
  COUNT(*) - COUNT(key_column) as null_key_count,  -- số NULL
  ROUND(100.0 * (COUNT(*) - COUNT(key_column)) / COUNT(*), 2) as null_pct
FROM catalog.schema.table_name;
```

### Pattern B: Kiểm tra duplicate trên business key

```sql
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT business_key) as unique_keys,
  COUNT(*) - COUNT(DISTINCT business_key) as duplicates,
  ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT business_key)) / COUNT(*), 2) as dup_pct
FROM catalog.schema.table_name;
```

### Pattern C: Range + Timeline check

```sql
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT year_month) as distinct_months,
  MIN(year_month) as earliest,
  MAX(year_month) as latest,
  COUNT(DISTINCT province_sk) as provinces_covered
FROM gold.gold.province_month_features;
```

### Pattern D: FK integrity check (bao nhiêu FK bị NULL)

```sql
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN fk_column IS NULL THEN 1 ELSE 0 END) as null_fk,
  ROUND(100.0 * SUM(CASE WHEN fk_column IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_fk_pct
FROM gold.gold.dim_post;
```

### Pattern E: So sánh Silver vs Gold (phát hiện inflate/shrink)

```sql
-- Đếm Silver source
SELECT 'silver_comments' as src, COUNT(*) as cnt FROM lakehouse.silver.tiktok_post_comments
UNION ALL
-- Đếm Gold dim (kỳ vọng ≈ Silver)
SELECT 'dim_comment', COUNT(*) FROM gold.gold.dim_comment;
```

### Pattern F: Phân phối giá trị (distribution check)

```sql
SELECT
  sentiment_label,
  COUNT(*) as cnt,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM gold.gold.fact_comment_nlp_engagement
GROUP BY sentiment_label
ORDER BY cnt DESC;
```

### Pattern G: Kiểm tra bảng fact có bị inflate (APPEND bug)

```sql
-- So sánh fact_hotel_review_daily vs silver.hotels_reviews
-- Nếu fact > silver → APPEND đã chạy nhiều lần
SELECT 'silver_reviews' as src, COUNT(*) FROM silver.silver.hotels_reviews
UNION ALL
SELECT 'fact_hotel_review_daily', COUNT(*) FROM gold.gold.fact_hotel_review_daily;
```

---

## 4. Bộ lệnh kiểm tra đầy đủ theo Layer

### 4.1 Kiểm tra Silver TikTok (1 lệnh, 3 bảng)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT 'tiktok_videos'        as tbl, COUNT(*) as total, COUNT(url)      as non_null_key, COUNT(posted_date)  as has_timeline, COUNT(region)    as has_partition FROM lakehouse.silver.tiktok_videos
UNION ALL
SELECT 'tiktok_post_metadata'       , COUNT(*)         , COUNT(post_url)                , COUNT(post_date)                  , COUNT(crawl_date)                  FROM lakehouse.silver.tiktok_post_metadata
UNION ALL
SELECT 'tiktok_post_comments'       , COUNT(*)         , COUNT(post_url)                , COUNT(comment_date)               , COUNT(comment)                     FROM lakehouse.silver.tiktok_post_comments;
"
```

**Kết quả baseline (2026-05-30):**

| tbl | total | non_null_key | has_timeline | has_partition |
|---|---|---|---|---|
| tiktok_videos | 8,417 | 8,417 ✅ | 7,891 ⚠️ (526 null ~6%) | 8,417 ✅ |
| tiktok_post_metadata | 6,031 | 6,031 ✅ | 5,616 ⚠️ (415 null ~7%) | 6,031 ✅ |
| tiktok_post_comments | 855,794 | 855,794 ✅ | 855,794 ✅ | 855,794 ✅ |

### 4.2 Kiểm tra Silver Hotel (1 lệnh, 3 bảng)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hive \
  --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT 'hotels_list'    as tbl, COUNT(*) as total, COUNT(hotel_url) as non_null_key, COUNT(province)      as has_province,   0                    as col4 FROM silver.silver.hotels_list
UNION ALL
SELECT 'hotels_detail'       , COUNT(*)         , COUNT(hotel_url)               , COUNT(rating_score)                      , COUNT(review_count)              FROM silver.silver.hotels_detail
UNION ALL
SELECT 'hotels_reviews'      , COUNT(*)         , COUNT(hotel_url)               , COUNT(stay_date)                         , COUNT(review_score)              FROM silver.silver.hotels_reviews;
"
```

**Kết quả baseline (2026-05-30):**

| tbl | total | non_null_key | col3 | col4 |
|---|---|---|---|---|
| hotels_list | 15,945 | 15,945 ✅ | 15,945 province ✅ | — |
| hotels_detail | 15,945 | 15,945 ✅ | 13,545 rating_score ⚠️ (~15% null) | 13,546 review_count |
| hotels_reviews | 1,551,297 | 1,551,297 ✅ | 1,551,297 stay_date ✅ | 1,551,297 review_score ✅ |

### 4.3 Kiểm tra Gold Dimensions (1 lệnh, 9 bảng)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT 'dim_province'    as tbl, COUNT(*) as total FROM gold.gold.dim_province
UNION ALL SELECT 'dim_date'        , COUNT(*) FROM gold.gold.dim_date
UNION ALL SELECT 'dim_author'      , COUNT(*) FROM gold.gold.dim_author
UNION ALL SELECT 'dim_post'        , COUNT(*) FROM gold.gold.dim_post
UNION ALL SELECT 'dim_comment'     , COUNT(*) FROM gold.gold.dim_comment
UNION ALL SELECT 'dim_hotel'       , COUNT(*) FROM gold.gold.dim_hotel
UNION ALL SELECT 'dim_country'     , COUNT(*) FROM gold.gold.dim_country
UNION ALL SELECT 'dim_room_type'   , COUNT(*) FROM gold.gold.dim_room_type
UNION ALL SELECT 'dim_travel_type' , COUNT(*) FROM gold.gold.dim_travel_type;
"
```

**Kết quả baseline (2026-05-30):**

| Bảng | Records | Ghi chú |
|---|---|---|
| dim_province | 63 | ✅ Đúng 63 tỉnh thành VN |
| dim_date | 9,497 | ✅ Range calendar đủ dài |
| dim_author | 3,814 | ✅ |
| dim_post | 6,348 | ✅ |
| dim_comment | 885,318 | ⚠️ Cao hơn Silver (855,794) — nghi duplicate |
| dim_hotel | 15,945 | ✅ Match hotels_list |
| dim_country | 194 | ✅ |
| dim_room_type | 22 | ⚠️ Thấp hơn kỳ vọng (doc nói ~50+) |
| dim_travel_type | 4 | ⚠️ Thấp hơn kỳ vọng (doc nói ~10) |

### 4.4 Kiểm tra Gold Facts (phát hiện APPEND inflation)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT 'fact_hotel_review_daily'         as tbl, COUNT(*) as total, COUNT(hotel_sk) as non_null_hotel_sk, COUNT(stay_date_sk) as non_null_date_sk FROM gold.gold.fact_hotel_review_daily
UNION ALL
SELECT 'fact_province_content_engagement'     , COUNT(*)         , COUNT(province_sk)                  , COUNT(date_sk)                        FROM gold.gold.fact_province_content_engagement
UNION ALL
SELECT 'fact_comment_nlp_engagement'          , COUNT(*)         , COUNT(comment_sk)                   , COUNT(province_sk)                    FROM gold.gold.fact_comment_nlp_engagement;
"
```

**Kết quả baseline (2026-05-30):**

| Bảng | Records | Ghi chú |
|---|---|---|
| fact_hotel_review_daily | **3,102,594** | 🔴 **BUG: Gấp đôi Silver** (1.55M) — APPEND đã chạy 2 lần |
| fact_province_content_engagement | 5,863 | ✅ |
| fact_comment_nlp_engagement | 884,572 | ✅ |

### 4.5 Kiểm tra ML Feature Tables

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT 'province_month_features' as tbl, COUNT(*) as total, COUNT(DISTINCT province_sk) as provinces, COUNT(DISTINCT year_month) as months, CAST(MIN(year_month) AS STRING) as min_ym, CAST(MAX(year_month) AS STRING) as max_ym FROM gold.gold.province_month_features
UNION ALL
SELECT 'forecast_next12'               , COUNT(*)         , COUNT(DISTINCT province_sk)              , COUNT(DISTINCT year_month)              , CAST(MIN(year_month) AS STRING)        , CAST(MAX(year_month) AS STRING)        FROM gold.gold.province_month_forecast_next12;
"
```

**Kết quả baseline (2026-05-30):**

| Bảng | Records | Provinces | Months | Range |
|---|---|---|---|---|
| province_month_features | 1,468 | 62/63 ⚠️ | 58 | 201903 → 202509 |
| province_month_forecast_next12 | 732 | 61/63 ⚠️ | 12 | 202510 → 202609 |

### 4.6 Kiểm tra Silver vs Gold (ratio check)

```bash
# Chạy với cả 2 catalog (lakehouse + gold)
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hive \
  --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT 'silver.tiktok_post_comments' as src, COUNT(*) as cnt FROM lakehouse.silver.tiktok_post_comments
UNION ALL SELECT 'gold.dim_comment'                  , COUNT(*) FROM gold.gold.dim_comment
UNION ALL SELECT 'gold.fact_comment_nlp_engagement'  , COUNT(*) FROM gold.gold.fact_comment_nlp_engagement
UNION ALL SELECT 'silver.hotels_reviews'             , COUNT(*) FROM silver.silver.hotels_reviews
UNION ALL SELECT 'gold.fact_hotel_review_daily'      , COUNT(*) FROM gold.gold.fact_hotel_review_daily;
"
```

**Kết quả kỳ vọng (tỉ lệ lành mạnh):**

| Cặp so sánh | Ratio kỳ vọng | Ghi chú |
|---|---|---|
| `dim_comment` / `tiktok_post_comments` | ≈ 1.0 (±5%) | MERGE dedup, có thể ít hơn Silver |
| `fact_hotel_review_daily` / `hotels_reviews` | = 1.0 | APPEND + anti-join, không được vượt |
| `fact_comment_nlp_engagement` / `dim_comment` | ≤ 1.0 | Filter is_active=True |

---

## 5. Kiểm tra FK Integrity

### 5.1 dim_post — FK null check

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN province_sk IS NULL THEN 1 ELSE 0 END) as null_province_sk,
  SUM(CASE WHEN author_sk IS NULL THEN 1 ELSE 0 END) as null_author_sk,
  SUM(CASE WHEN post_date_sk IS NULL THEN 1 ELSE 0 END) as null_post_date_sk,
  SUM(CASE WHEN crawl_date_sk IS NULL THEN 1 ELSE 0 END) as null_crawl_date_sk,
  ROUND(100.0 * SUM(CASE WHEN province_sk IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_province_pct
FROM gold.gold.dim_post;
"
```

### 5.2 dim_comment — FK null check

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN post_sk IS NULL THEN 1 ELSE 0 END) as null_post_sk,
  SUM(CASE WHEN comment_date_sk IS NULL THEN 1 ELSE 0 END) as null_date_sk,
  ROUND(100.0 * SUM(CASE WHEN post_sk IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_post_pct
FROM gold.gold.dim_comment;
"
```

### 5.3 fact_hotel_review_daily — FK null check

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN hotel_sk IS NULL THEN 1 ELSE 0 END) as null_hotel_sk,
  SUM(CASE WHEN stay_date_sk IS NULL THEN 1 ELSE 0 END) as null_date_sk,
  SUM(CASE WHEN country_sk IS NULL THEN 1 ELSE 0 END) as null_country_sk,
  ROUND(100.0 * SUM(CASE WHEN hotel_sk IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_hotel_pct
FROM gold.gold.fact_hotel_review_daily;
"
```

---

## 6. Kiểm tra phân phối dữ liệu (Distribution Checks)

### 6.1 NLP sentiment distribution

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT
  sentiment_label,
  COUNT(*) as cnt,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM gold.gold.fact_comment_nlp_engagement
GROUP BY sentiment_label
ORDER BY cnt DESC;
"
```

### 6.2 Province coverage trong dim_post

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT p.province_name, COUNT(dp.post_sk) as post_count
FROM gold.gold.dim_province p
LEFT JOIN gold.gold.dim_post dp ON p.province_sk = dp.province_sk
GROUP BY p.province_name
ORDER BY post_count DESC
LIMIT 20;
"
```

### 6.3 Hotel review distribution by traveler_type (Silver)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hive \
  --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT
  traveler_type,
  COUNT(*) as cnt,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct
FROM silver.silver.hotels_reviews
WHERE traveler_type IS NOT NULL
GROUP BY traveler_type
ORDER BY cnt DESC;
"
```

### 6.4 Hotel review timeline coverage (quan trọng cho LSTM)

```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hive \
  --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT
  YEAR(stay_date) as yr,
  MONTH(stay_date) as mo,
  COUNT(*) as review_count,
  COUNT(DISTINCT hotel_url) as unique_hotels
FROM silver.silver.hotels_reviews
WHERE stay_date IS NOT NULL
GROUP BY YEAR(stay_date), MONTH(stay_date)
ORDER BY yr, mo;
"
```

---

## 7. Vấn đề đã biết (Known Issues — 2026-05-30)

| # | Bảng | Vấn đề | Mức độ | Nguyên nhân |
|---|---|---|---|---|
| 🔴 1 | `fact_hotel_review_daily` | **3.1M rows** trong khi Silver chỉ **1.55M** — inflate gấp đôi | Nghiêm trọng | APPEND không dedup, DAG đã chạy 2+ lần |
| 🔴 2 | `dim_comment` | **885K rows** > Silver **855K** — có thể duplicate | Cần điều tra | MERGE incremental có thể bị lỗi logic |
| 🟡 3 | `hotels_detail` | 15% records NULL `rating_score` (~2,400 hotels) | Vừa | Dữ liệu Bronze thiếu |
| 🟡 4 | `tiktok_videos` | 6% NULL `posted_date` (~526 records) | Vừa | Trường trống trong Bronze CSV |
| 🟡 5 | `tiktok_post_metadata` | 7% NULL `post_date` (~415 records) | Vừa | Relative date parse thất bại |
| 🟡 6 | `province_month_features` | 62/63 tỉnh — 1 tỉnh thiếu data | Nhỏ | Tỉnh không có TikTok post nào |
| 🟢 7 | `fact_province_month_dl_features` | Chưa tồn tại | Info | Cần chạy `fact_dl_features_job.py` |
| 🟢 8 | `fact_comment_nlp_v2` | Chưa tồn tại | Info | Cần chạy PhoBERT `inference_phobert.py` |
| 🟢 9 | `province_month_forecast_lstm_next12` | Chưa tồn tại | Info | Cần chạy `train_lstm_forecast.py` |

---

## 8. Quick Health Check — Một lệnh duy nhất

Lệnh chạy nhanh để có cái nhìn tổng quan tất cả bảng chính:

```bash
# Silver TikTok
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT '[SILVER-TikTok] tiktok_videos'        as layer_table, COUNT(*) as rows FROM lakehouse.silver.tiktok_videos
UNION ALL SELECT '[SILVER-TikTok] tiktok_post_metadata', COUNT(*) FROM lakehouse.silver.tiktok_post_metadata
UNION ALL SELECT '[SILVER-TikTok] tiktok_post_comments', COUNT(*) FROM lakehouse.silver.tiktok_post_comments;
"

# Silver Hotel (separate command - different catalog)
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.silver.type=hive \
  --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT '[SILVER-Hotel] hotels_list'    as layer_table, COUNT(*) as rows FROM silver.silver.hotels_list
UNION ALL SELECT '[SILVER-Hotel] hotels_detail'  , COUNT(*) FROM silver.silver.hotels_detail
UNION ALL SELECT '[SILVER-Hotel] hotels_reviews' , COUNT(*) FROM silver.silver.hotels_reviews;
"

# Gold (separate command)
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
SELECT '[GOLD-Dim] dim_province'        as layer_table, COUNT(*) as rows FROM gold.gold.dim_province
UNION ALL SELECT '[GOLD-Dim] dim_post'                , COUNT(*) FROM gold.gold.dim_post
UNION ALL SELECT '[GOLD-Dim] dim_comment'             , COUNT(*) FROM gold.gold.dim_comment
UNION ALL SELECT '[GOLD-Dim] dim_hotel'               , COUNT(*) FROM gold.gold.dim_hotel
UNION ALL SELECT '[GOLD-Fact] fact_hotel_review_daily', COUNT(*) FROM gold.gold.fact_hotel_review_daily
UNION ALL SELECT '[GOLD-Fact] fact_province_content'  , COUNT(*) FROM gold.gold.fact_province_content_engagement
UNION ALL SELECT '[GOLD-Fact] fact_comment_nlp'       , COUNT(*) FROM gold.gold.fact_comment_nlp_engagement
UNION ALL SELECT '[GOLD-ML] province_month_features'  , COUNT(*) FROM gold.gold.province_month_features;
"
```

---

## 9. Baseline Numbers — Tham chiếu nhanh

| Layer | Bảng | Rows (2026-05-30) | Trạng thái |
|---|---|---|---|
| Silver-TikTok | tiktok_videos | 8,417 | ✅ |
| Silver-TikTok | tiktok_post_metadata | 6,031 | ✅ |
| Silver-TikTok | tiktok_post_comments | 855,794 | ✅ |
| Silver-Hotel | hotels_list | 15,945 | ✅ |
| Silver-Hotel | hotels_detail | 15,945 | ✅ |
| Silver-Hotel | hotels_reviews | 1,551,297 | ✅ |
| Gold-Dim | dim_province | 63 | ✅ |
| Gold-Dim | dim_date | 9,497 | ✅ |
| Gold-Dim | dim_author | 3,814 | ✅ |
| Gold-Dim | dim_post | 6,348 | ✅ |
| Gold-Dim | dim_comment | 885,318 | ⚠️ |
| Gold-Dim | dim_hotel | 15,945 | ✅ |
| Gold-Dim | dim_country | 194 | ✅ |
| Gold-Fact | fact_hotel_review_daily | 3,102,594 | 🔴 Inflate |
| Gold-Fact | fact_province_content_engagement | 5,863 | ✅ |
| Gold-Fact | fact_comment_nlp_engagement | 884,572 | ✅ |
| Gold-ML | province_month_features | 1,468 | ✅ |
| Gold-ML | province_month_forecast_next12 | 732 | ✅ |

---

*Guide này được tạo ngày 2026-05-30 từ kết quả kiểm tra data thực tế trên hệ thống.*
