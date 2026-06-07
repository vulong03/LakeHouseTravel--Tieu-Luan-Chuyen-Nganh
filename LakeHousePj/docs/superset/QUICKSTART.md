# 🚀 Apache Superset Quick Start Guide

## 📋 What's New

Apache Superset đã được thêm vào LakeHouse stack để tạo dashboard và biểu đồ từ dữ liệu Hive, PostgreSQL, MinIO.

**Files được tạo:**
```
superset/
├── Dockerfile              # Container definition (Apache Superset base + drivers)
├── superset_config.py      # Flask/Superset configuration
├── entrypoint.sh          # Initialization script (DB migration + admin creation)
├── init.sh                # Alternative init script
├── init_connections.py    # Auto-create database connections to Hive/PostgreSQL
├── .dockerignore          # Build exclusions
├── README.md              # Comprehensive documentation
├── SETUP.md               # Detailed setup guide with examples
└── QUICKSTART.md          # This file
```

**Updated:**
```
docker-compose.yml        # Added Superset service (port 8088, depends on postgres + hive)
```

---

## ⚡ Start Superset (3 Steps)

### Step 1: Build Superset Image

```bash
cd LakeHousePj

# Build
docker-compose build superset

# Verify
docker images | grep superset
```

### Step 2: Start Superset Stack

```bash
# Start Superset + dependencies
docker-compose up -d superset

# Wait for startup (~60 seconds)
docker logs -f lakehouse_superset
```

**Expected output:**
```
...
Initializing Superset database...
Creating admin user...
✓ Superset initialization completed!
Starting Superset web server...
```

### Step 3: Access Superset UI

```
http://localhost:8088
Username: admin
Password: admin
```

---

## 🔗 Auto-Setup Database Connections

After Superset starts, create connections to Hive, PostgreSQL automatically:

```bash
docker exec lakehouse_superset python /app/init_connections.py
```

**Output:**
```
Creating PostgreSQL connection...
✓ PostgreSQL connection created
Creating Hive Metastore connection...
✓ Hive Metastore connection created
Creating Spark SQL connection...
✓ Spark SQL connection created

✓ All database connections initialized successfully!
```

---

## 📊 First Chart: 30 Seconds

### 1. Open SQL Lab

```
Menu → SQL Lab
```

### 2. Select Database

- Database: **Hive Lakehouse**

### 3. Paste Query

```sql
SELECT 
    province_name,
    year_month,
    hotness_score,
    total_posts,
    total_comments
FROM gold.gold.fact_province_month_dl_features
WHERE year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months')
LIMIT 100
```

### 4. Run Query

```
Ctrl+Enter  or  Click "Run" button
```

### 5. Visualize

```
Click "Visualize"
Choose Chart Type:
- Line Chart    (for trends)
- Bar Chart     (for comparison)
- Table         (for data)
- Pie Chart     (for distribution)
```

### 6. Save Chart

```
Click "Save as Chart"
Name: "Province Hotness Trend"
Dashboard: "Create New" → "Tourism Forecast"
```

---

## 📈 Pre-built Sample Queries

### Query 1: Top Provinces by Engagement (Last 3 Months)

```sql
SELECT 
    province_name,
    region,
    ROUND(AVG(hotness_score), 3) as avg_hotness,
    ROUND(AVG(total_posts), 1) as avg_posts,
    ROUND(AVG(engagement_score), 1) as avg_engagement,
    ROUND(AVG(avg_hotel_score), 2) as avg_hotel_rating
FROM gold.gold.fact_province_month_dl_features
WHERE year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
GROUP BY province_name, region
ORDER BY avg_hotness DESC
LIMIT 20;
```

**Best as:** Table or Bar Chart

### Query 2: Monthly Trend (Single Province)

```sql
SELECT 
    year_month,
    province_name,
    hotness_score,
    total_posts,
    total_comments,
    engagement_score,
    avg_hotel_score
FROM gold.gold.fact_province_month_dl_features
WHERE province_name = 'Hà Nội'
    AND year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '24 months')
ORDER BY year_month;
```

**Best as:** Line Chart (multi-series)

### Query 3: TikTok vs Hotel Reviews Correlation

```sql
SELECT 
    province_name,
    total_posts,
    total_hotel_reviews,
    avg_likes_per_post,
    avg_hotel_score
FROM gold.gold.fact_province_month_dl_features
WHERE year_month = DATE_TRUNC('month', CURRENT_DATE)
    AND total_posts > 0
    AND total_hotel_reviews > 0;
```

**Best as:** Scatter Chart (with color = hotel score, size = posts)

### Query 4: Regional Analysis

```sql
SELECT 
    region,
    COUNT(DISTINCT province_sk) as num_provinces,
    ROUND(AVG(hotness_score), 3) as avg_hotness,
    SUM(total_posts) as total_posts
FROM gold.gold.fact_province_month_dl_features
WHERE year_month = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY region
ORDER BY avg_hotness DESC;
```

**Best as:** Pie Chart or Donut Chart

---

## 🎨 Create Dashboard (5 Minutes)

### 1. Create New Dashboard

```
Menu → Dashboards → "+ Dashboard"
Name: "Tourism Analytics Dashboard"
Click "Create Dashboard"
```

### 2. Edit Dashboard

```
Click "Edit Dashboard" (pencil icon)
```

### 3. Add Components

#### Add Chart
```
Drag "Chart" component
Click "Select a chart"
Choose: "Province Hotness Trend" (created earlier)
```

#### Add Filter
```
Drag "Filter" component
Dataset: "fact_province_month_dl_features"
Column: "province_name"
Operator: "IS"
Link to chart
```

#### Add Text/Markdown
```
Drag "Markdown" component
Type: "## Tourism Forecast Dashboard"
```

### 4. Arrange Layout

- Drag components to arrange
- Resize by dragging corners

### 5: Save Dashboard

```
Click "Save Dashboard"
```

### 6. View Live Dashboard

```
Exit edit mode
Set auto-refresh (optional)
Share with team
```

---

## 🔍 Common Operations

### View Query Results

```
SQL Lab → Write query → Run
```

### Create Metric from Chart

```
Chart → Click "Edit Chart"
Metrics section → "+ Metric"
Name: "Engagement Rate"
Expression: "SUM(engagement_score) / SUM(total_posts)"
```

### Set Dashboard Refresh

```
Dashboard → Settings → Auto-refresh interval
Options: 15s, 30s, 1m, 5m, 10m, 30m, 1h
```

### Export Chart Data

```
Chart view → "Export" menu
Format: CSV, Excel (XLSX), JSON
```

### Share Dashboard

```
Dashboard → "Share" button
Select users/roles
Generate shareable link
Set expiration
```

---

## 🛠️ Useful Commands

### View Superset Logs

```bash
docker logs lakehouse_superset
docker logs -f lakehouse_superset  # Follow live logs
```

### SSH into Superset Container

```bash
docker exec -it lakehouse_superset bash
```

### Rebuild Superset (After config changes)

```bash
docker-compose build --no-cache superset
docker-compose up -d superset
```

### Reset Admin Password

```bash
docker exec lakehouse_superset superset fab reset-password --username admin
```

### Create New User

```bash
docker exec lakehouse_superset superset fab create-admin \
  --username analyst1 \
  --firstname Data \
  --lastname Analyst \
  --email analyst1@lakehouse.local \
  --password password123
```

### Check Database Connections

```bash
docker exec lakehouse_superset python -c \
  "from superset.models.core import Database; [print(d.database_name) for d in Database.query.all()]"
```

---

## ⚠️ Troubleshooting

### Issue: "Unable to connect to Hive Metastore"

**Symptom:** Chart fails with "Connection refused"

**Fix:**
```bash
# Check Hive is running
docker ps | grep hive

# Test port
docker exec lakehouse_superset nc -zv hive-metastore 9083

# Restart Hive
docker-compose restart hive-metastore

# Retry in Superset
```

### Issue: Slow Dashboard Loading

**Cause:** Loading too much data

**Fix:**
```sql
-- Add LIMIT
SELECT * FROM large_table LIMIT 10000

-- Or filter by date
WHERE year_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
```

### Issue: "No database configured" in SQL Lab

**Fix:**
```bash
# Create connections
docker exec lakehouse_superset python /app/init_connections.py

# Or manually in UI
Settings → Database Connections → + Database
```

### Issue: Out of Memory Error

**Cause:** Superset container memory limit

**Fix in docker-compose.yml:**
```yaml
superset:
  deploy:
    resources:
      limits:
        memory: 2G
      reservations:
        memory: 1G
```

---

## 📚 Documentation Files

| File | Purpose |
|------|---------|
| `README.md` | Comprehensive guide (features, queries, troubleshooting) |
| `SETUP.md` | Detailed setup with database connection steps |
| `QUICKSTART.md` | **You are here** - Quick reference |

---

## 🎯 Next Steps

1. ✅ Start Superset: `docker-compose up -d superset`
2. ✅ Initialize connections: `docker exec lakehouse_superset python /app/init_connections.py`
3. ✅ Create first chart (30 seconds)
4. ✅ Create dashboard (5 minutes)
5. 📖 Read `README.md` for advanced features
6. 📖 Read `SETUP.md` for detailed configuration
7. 🎨 Start building visualizations!

---

## 📞 Support

- **Superset Docs:** https://superset.apache.org/
- **Hive SQL:** https://cwiki.apache.org/confluence/display/Hive/
- **Project Docs:** See `../docs/` folder

---

**Setup Date:** 2026-06-03
**Status:** Ready to Use
**Default Port:** 8088
**Admin User:** admin / admin
