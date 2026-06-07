# 📊 Apache Superset - Data Visualization Dashboard

Apache Superset được tích hợp vào LakeHouse để tạo các biểu đồ và dashboard từ dữ liệu trong Hive Metastore, PostgreSQL, và các sources khác.

## 🚀 Quick Start

### 1. Build & Start

```bash
# Build Superset image
docker-compose build superset

# Start Superset (kèm dependencies)
docker-compose up -d superset

# Hoặc start toàn bộ stack
docker-compose up -d
```

### 2. Truy cập Superset UI

```
http://localhost:8088
Username: admin
Password: admin
```

### 3. Khởi tạo Database Connections (Optional)

```bash
# Tự động tạo connections đến Hive, PostgreSQL
docker exec lakehouse_superset python /app/init_connections.py
```

---

## 📁 Cấu trúc Thư mục

```
superset/
├── Dockerfile              # Container definition
├── superset_config.py      # Superset configuration
├── entrypoint.sh          # Initialization script
├── init_connections.py    # Auto-create DB connections
├── SETUP.md               # Detailed setup guide
├── .dockerignore          # Docker build excludes
└── README.md              # File này
```

---

## 🔗 Data Sources Có sẵn

| Nguồn | Loại | Mục đích |
|-------|------|---------|
| **Hive Metastore** | `lakehouse`, `silver`, `gold` catalogs | Truy vấn Iceberg tables (TikTok, Hotels) |
| **PostgreSQL** | `metastore_db` | Truy vấn dim_date, tracking tables |
| **Spark SQL** | `spark-master:10000` | Spark Thrift Server queries |
| **MinIO/S3** | S3-compatible | Parquet/CSV files (advanced) |

---

## 📊 Sample Queries & Charts

### Query 1: Province Tourism Hotness Trend

```sql
SELECT 
    year_month,
    province_name,
    region,
    hotness_score,
    total_posts,
    total_comments,
    engagement_score,
    avg_hotel_score
FROM gold.gold.fact_province_month_dl_features
WHERE year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
ORDER BY year_month DESC, hotness_score DESC
```

**Visualize as:** Line Chart (time series), Grouped by province

### Query 2: Top Provinces by Engagement

```sql
SELECT 
    province_name,
    region,
    AVG(hotness_score) as avg_hotness,
    AVG(total_posts) as avg_posts,
    AVG(engagement_score) as avg_engagement,
    AVG(avg_hotel_score) as avg_hotel_rating
FROM gold.gold.fact_province_month_dl_features
WHERE year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
GROUP BY province_name, region
ORDER BY avg_hotness DESC
LIMIT 15
```

**Visualize as:** Table, Bar Chart, or Pie Chart

### Query 3: TikTok vs Hotel Correlation

```sql
SELECT 
    province_name,
    total_posts,
    total_hotel_reviews,
    avg_likes_per_post,
    avg_hotel_score,
    comments_per_post
FROM gold.gold.fact_province_month_dl_features
WHERE year_month = DATE_TRUNC('month', CURRENT_DATE)
    AND total_posts > 0
    AND total_hotel_reviews > 0
```

**Visualize as:** Scatter Plot
- X-axis: `total_posts`
- Y-axis: `total_hotel_reviews`
- Color: `avg_hotel_score`
- Size: `comments_per_post`

### Query 4: Sentiment Analysis over Time

```sql
SELECT 
    DATE_TRUNC('month', comment_date)::DATE as month,
    sentiment_label,
    COUNT(*) as comment_count,
    AVG(CAST(sentiment_score AS FLOAT)) as avg_sentiment
FROM gold.gold.fact_comment_nlp_engagement
WHERE comment_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY month, sentiment_label
ORDER BY month, sentiment_label
```

**Visualize as:** 100% Stacked Bar Chart or Area Chart

### Query 5: Regional Distribution

```sql
SELECT 
    region,
    COUNT(DISTINCT province_sk) as num_provinces,
    AVG(hotness_score) as avg_hotness,
    SUM(total_posts) as total_posts,
    SUM(total_comments) as total_comments
FROM gold.gold.fact_province_month_dl_features
WHERE year_month = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY region
ORDER BY avg_hotness DESC
```

**Visualize as:** Pie Chart or Donut Chart

### Query 6: LSTM Forecast Comparison

```sql
SELECT 
    year_month,
    province_name,
    hotness_score as actual,
    NULL as forecast
FROM gold.gold.fact_province_month_dl_features
WHERE province_name IN ('Hà Nội', 'TP. Hồ Chí Minh', 'Đà Nẵng')
    AND year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '24 months')

UNION ALL

SELECT 
    forecast_month,
    province_name,
    NULL as actual,
    forecast_hotness
FROM gold.gold.province_month_forecast_lstm_next12
WHERE province_name IN ('Hà Nội', 'TP. Hồ Chí Minh', 'Đà Nẵng')
    
ORDER BY province_name, year_month
```

**Visualize as:** Line Chart with Dual Lines (actual vs forecast)

---

## 🎨 Dashboard Creation Steps

### Step 1: Create a New Dashboard

1. Menu → **Dashboards** → **+ Dashboard**
2. Tên: "Tourism Analytics Dashboard"
3. Click **Create**

### Step 2: Add Charts to Dashboard

1. Click **Edit Dashboard** (biểu tượng bút chì)
2. Click **+ Tab** để thêm tabs (optional)
3. Drag-drop **Chart** component
4. Chọn chart hoặc create new

### Step 3: Configure Chart

Ví dụ - Tạo "Hotness Trend" Chart:

1. SQL Lab → Write query (Query 1 ở trên)
2. Click **Run**
3. Click **Visualize**
4. Chọn **Line Chart**
5. Configure:
   - X-axis: `year_month`
   - Y-axis: `hotness_score`, `engagement_score`
   - Group by: `province_name`
6. Click **Update Chart**
7. Click **Save as Chart**
   - Tên: "Province Hotness Trend"
   - Dashboard: "Tourism Analytics Dashboard"

### Step 4: Add Filters to Dashboard

1. Dashboard Edit mode
2. Drag **Filter** component
3. Configure filter:
   - Dataset: `fact_province_month_dl_features`
   - Filter on: `year_month`, `province_name`, `region`
4. Link filters to charts

### Step 5: Share Dashboard

1. Click **Share** (top right)
2. Cấp quyền cho users/roles
3. Generate share link
4. Set refresh rate (auto-refresh interval)

---

## 🔧 Configuration & Customization

### Change Admin Password

```bash
docker exec lakehouse_superset superset fab reset-password --username admin
```

### Create New User

```bash
docker exec lakehouse_superset superset fab create-admin \
  --username newuser \
  --firstname New \
  --lastname User \
  --email user@lakehouse.local \
  --password password123
```

### Enable Row-Level Security (RLS)

1. Menu → **Settings** → **Manage Security** → **List Roles**
2. Configure row filters per role
3. Assign roles to users

### Set Refresh Interval for Dashboard

1. Dashboard view → Click settings
2. Set **Auto-refresh interval**
3. Save

---

## 🐛 Troubleshooting

### Issue: "Cannot connect to Hive Metastore"

**Diagnosis:**
```bash
# Check if Hive is running
docker ps | grep hive

# Test connection
docker exec lakehouse_superset nc -zv hive-metastore 9083

# Check Hive logs
docker logs lakehouse_hive_metastore | tail -50
```

**Fix:**
```bash
# Restart Hive
docker-compose restart hive-metastore

# Restart Superset
docker-compose restart superset
```

### Issue: "AdminError: Cannot query databases with empty URI"

**Fix:**
```bash
# Ensure database connections are created
docker exec lakehouse_superset python /app/init_connections.py

# Restart Superset
docker-compose restart superset
```

### Issue: Out of Memory

**Cause:** Superset loading too much data
**Fix:**
- Add `LIMIT 100000` to queries
- Configure `EXPORT_MAX_ROWS` in config
- Set `SUPERSET_SQLLAB_TIMEOUT` to lower value

```python
# In superset_config.py
EXPORT_MAX_ROWS = 50000
SUPERSET_SQLLAB_TIMEOUT = 300  # 5 minutes
```

### Issue: Slow Dashboard Loading

**Fix:**
1. Enable caching in queries
2. Use Materialized Views:
   ```sql
   CREATE TABLE gold.superset_cache AS
   SELECT * FROM gold.gold.fact_province_month_dl_features
   WHERE year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '24 months');
   ```
3. Set appropriate refresh interval

---

## 📚 Advanced Topics

### Custom SQL Metrics

1. Dataset → Edit Dataset
2. Click **+ Metric**
3. Define metric:
   - Name: "Engagement Rate"
   - SQL: `SUM(engagement_score) / SUM(total_posts)`
4. Save

### Alerts & Reports

1. Menu → **Reports** → **+ Alert**
2. Configure:
   - Chart to monitor
   - Condition (e.g., hotness_score > threshold)
   - Recipients (email)
3. Enable

### Export Data

1. Chart → Click **Export** menu
2. Choose format:
   - CSV
   - Excel (XLSX)
   - JSON
3. Download

### API Access

```bash
# Get CSRF token
curl -X GET http://localhost:8088/api/v1/security/csrf_token/ \
  -H "Authorization: Bearer YOUR_TOKEN"

# Query API
curl -X POST http://localhost:8088/api/v1/chart/data \
  -H "Content-Type: application/json" \
  -d '{"query_context": {...}}'
```

---

## 📖 Documentation

- **Superset Official:** https://superset.apache.org/
- **Hive Metastore:** https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration
- **Spark SQL:** https://spark.apache.org/docs/latest/sql-programming-guide.html
- **Related Docs:** See `../docs/` folder for project-specific docs

---

## 🔐 Production Checklist

- [ ] Change `SECRET_KEY` in `superset_config.py`
- [ ] Enable HTTPS (`SUPERSET_WEBSERVER_PROTOCOL = "https"`)
- [ ] Set `SESSION_COOKIE_SECURE = True`
- [ ] Configure proper database backup
- [ ] Set up authentication (LDAP/OAuth)
- [ ] Configure email for alerts
- [ ] Enable row-level security
- [ ] Test disaster recovery

---

**Created:** 2026-06-03
**Status:** Ready for Integration
**Port:** 8088
**Database Backend:** PostgreSQL (metastore_db)
