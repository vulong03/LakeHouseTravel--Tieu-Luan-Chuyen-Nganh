# 📊 Apache Superset Setup & Configuration Guide

## Khởi động Superset

### Bước 1: Build và Start Superset

```bash
# Build container
docker-compose build superset

# Start Superset (cùng với các services khác)
docker-compose up -d superset

# Hoặc chỉ Superset + dependencies
docker-compose up -d postgres superset hive-metastore minio
```

### Bước 2: Truy cập Superset UI

```
http://localhost:8088
Username: admin
Password: admin
```

---

## Cấu hình Data Sources (Databases)

### Data Source 1: PostgreSQL (Metadata DB)

**Dùng để:** Truy vấn PostgreSQL metastore (dim_date table, etc.)

1. Menu → **Settings** → **Database Connections** → **+ Database**
2. Chọn **PostgreSQL** từ dropdown
3. Điền:
   - **Display Name:** `PostgreSQL Lakehouse`
   - **SQLAlchemy URI:**
     ```
     postgresql://lakehouse_user:lakehouse_pass@postgres:5432/metastore_db
     ```
   - **Test Connection** → Save

---

### Data Source 2: Hive (Spark SQL via Hive Metastore)

**Dùng để:** Truy vấn bảng Silver + Gold (Iceberg) từ Hive Metastore

1. Menu → **Settings** → **Database Connections** → **+ Database**
2. Chọn **Hive** từ dropdown
3. Điền:
   - **Display Name:** `Hive Lakehouse`
   - **Host:** `hive-metastore`
   - **Port:** `9083`
   - **Database Name:** `default` (hoặc chỉ định database sau)
   - **Test Connection** → Save

4. Hoặc dùng **SQLAlchemy URI:**
   ```
   hive://hive-metastore:9083/default
   ```

**Lưu ý:** Có thể cần cài thêm driver PySpark (đã có trong Dockerfile)

---

### Data Source 3: Spark (Thrift Server - Nếu có)

**Dùng để:** Nếu enable Spark Thrift Server trên cổng 10000

1. Chọn **Spark SQL**
2. SQLAlchemy URI:
   ```
   spark+thrift://spark-master:10000/default
   ```

---

### Data Source 4: MinIO/S3 (Apache Druid - Nếu cần)

**Dùng để:** Truy vấn Parquet/CSV từ MinIO trực tiếp (advanced)

1. Cấu hình Dremio trước (có sẵn trong hệ thống)
2. Rồi kết nối Superset → Dremio
3. Hoặc dùng Spark + S3 connector

---

## Tạo Datasets & Dashboards

### Bước 1: Tạo Dataset từ Hive

1. Menu → **+ New** → **Dataset**
2. Chọn database: **Hive Lakehouse**
3. Chọn table: `gold.gold.dim_province` (hoặc `silver.silver.tiktok_post_metadata`)
4. Click **Create Dataset**
5. Cấu hình columns, metrics, filters
6. **Save**

### Bước 2: Tạo Dashboard

1. Menu → **+ New** → **Dashboard**
2. Điền tên: "Tourism Forecast Dashboard"
3. Click **Create Dashboard**
4. Thêm charts:
   - Click **Edit Dashboard** (biểu tượng bút chì)
   - Kéo-thả components
   - Thêm **Charts** hoặc **Markdowns**

---

## Sample Charts Recommendations

### Chart 1: Province Hotness Trend (Line Chart)

```sql
SELECT 
    year_month,
    province_name,
    hotness_score,
    total_posts,
    engagement_score
FROM gold.gold.fact_province_month_dl_features
WHERE year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
ORDER BY year_month, province_name
```

**Chart Type:** Line Chart
- X-axis: `year_month`
- Y-axis: `hotness_score`, `engagement_score`
- Group by: `province_name`
- Filter: Top 10 provinces

### Chart 2: Region Distribution (Pie Chart)

```sql
SELECT 
    region,
    COUNT(DISTINCT province_sk) as province_count,
    AVG(hotness_score) as avg_hotness
FROM gold.gold.fact_province_month_dl_features
WHERE year_month = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY region
```

**Chart Type:** Pie Chart
- Values: `avg_hotness`
- Slices: `region`

### Chart 3: Post Volume vs Hotel Reviews (Scatter)

```sql
SELECT 
    province_name,
    total_posts,
    total_hotel_reviews,
    avg_hotel_score,
    total_comments
FROM gold.gold.fact_province_month_dl_features
WHERE year_month = DATE_TRUNC('month', CURRENT_DATE)
```

**Chart Type:** Scatter Plot
- X-axis: `total_posts`
- Y-axis: `total_hotel_reviews`
- Color: `avg_hotel_score`
- Size: `total_comments`

### Chart 4: Sentiment Evolution (Bar Chart)

```sql
SELECT 
    year_month,
    AVG(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive_ratio,
    AVG(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative_ratio,
    AVG(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral_ratio
FROM gold.gold.fact_comment_nlp_engagement
GROUP BY year_month
ORDER BY year_month DESC
LIMIT 12
```

**Chart Type:** 100% Stacked Bar Chart
- X-axis: `year_month`
- Values: `positive_ratio`, `negative_ratio`, `neutral_ratio`

### Chart 5: TikTok Engagement Metrics (Table)

```sql
SELECT 
    province_name,
    total_posts,
    total_comments,
    AVG(avg_likes_per_post) as avg_likes,
    AVG(engagement_score) as avg_engagement,
    ROUND(AVG(viral_post_ratio), 2) as viral_ratio
FROM gold.gold.fact_province_month_dl_features
GROUP BY province_name
ORDER BY avg_engagement DESC
LIMIT 20
```

**Chart Type:** Table
- Columns: All
- Sorting: By `avg_engagement` DESC

### Chart 6: Forecast Comparison (Line Chart - LSTM vs Actual)

```sql
SELECT 
    year_month,
    province_name,
    hotness_score,
    LEAD(hotness_score) OVER (PARTITION BY province_name ORDER BY year_month) as next_month_actual
FROM gold.gold.fact_province_month_dl_features
WHERE province_name IN ('Hà Nội', 'TP. Hồ Chí Minh', 'Đà Nẵng')
ORDER BY province_name, year_month
```

**Chart Type:** Line Chart
- X-axis: `year_month`
- Y-axis: `hotness_score`, `next_month_actual`
- Group by: `province_name`

---

## Cách Sử Dụng SQL Editor

1. Menu → **SQL Lab**
2. Chọn database: **Hive Lakehouse**
3. Viết query SQL
4. Click **Run** (Ctrl+Enter)
5. Xem kết quả
6. Click **Visualize** → Chọn chart type
7. **Save as Chart** → Chọn Dashboard

---

## Permissions & Security

### Tạo Role cho Teams

1. Menu → **Settings** → **Manage Security** → **List Roles**
2. Click **+ Role**
3. Tên: "Tourism Analysts"
4. Cấp quyền: 
   - `datasource_access` on Hive Lakehouse
   - `chart_access`, `dashboard_access`
5. Save

### Assign Users to Roles

1. Menu → **Settings** → **Manage Security** → **List Users**
2. Click user → Edit
3. Chọn roles
4. Save

---

## Troubleshooting

### Lỗi: "Cannot connect to Hive"

**Fix:**
```bash
# Check Hive Metastore is running
docker ps | grep hive-metastore

# Check port 9083
docker exec lakehouse_hive_metastore nc -zv localhost 9083

# Restart Hive
docker-compose restart hive-metastore
```

### Lỗi: "SQLAlchemy connection string invalid"

**Fix:** Thử URIs khác:
```
hive://hive-metastore:9083/silver
hive://hive-metastore:9083/gold
pyhive.sqlalchemy://hive-metastore:9083/default
```

### Lỗi: "Admin user already exists"

**Fix:** Xóa database cũ
```bash
docker-compose down superset
rm -rf superset/data/
docker-compose up -d superset
```

---

## Advanced: Custom Theme

1. Tạo file `superset/custom_config.py`:
```python
SUPERSET_UI_CUSTOM_COLORS = {
    "primary": "#1f77b4",
    "secondary": "#ff7f0e",
    "accent": "#2ca02c",
}
```

2. Cập nhật `superset_config.py`:
```python
from custom_config import SUPERSET_UI_CUSTOM_COLORS
```

---

## Performance Tips

1. **Tạo Materialized View** trong Hive cho queries phức tạp:
```sql
CREATE TABLE gold.superset_cache AS
SELECT ... FROM fact_province_month_dl_features
WHERE year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '24 months');
```

2. **Enable Caching:**
   - Menu → **Settings** → **Cache Configuration**
   - Set TTL: 3600 seconds (1 hour)

3. **Limit Data:**
   - Trong SQL Lab: `LIMIT 100000`
   - Dashboard filters: Prefilter bằng time range

---

## Tài liệu Tham Khảo

- Apache Superset Docs: https://superset.apache.org/
- Hive Metastore: https://cwiki.apache.org/confluence/display/Hive/
- Spark SQL: https://spark.apache.org/docs/latest/sql-programming-guide.html

---

**Cấu hình lần cuối:** 2026-06-03
