# dim_province Quick Start Guide

## 🚀 Run the Job

```powershell
cd D:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\LakeHousePj
.\scripts\run-gold-dim-province.ps1
```

Expected output:
```
✅ dim_province job completed successfully!
   Records: 63
   Execution time: 12.45s
📝 Gold job logged: gold.dim_province - 63 records
```

## 🧪 Test the Results

```powershell
.\scripts\test-gold-dim-province.ps1
```

Expected: All 10 tests pass ✅

## 📋 View Job Logs

```powershell
.\scripts\query-gold-job-logs.ps1
```

Shows:
- Recent job executions
- Success/failure status  
- Record counts
- Execution times
- Error messages (if any)

## 📊 Query the Data

### Option 1: Spark SQL Shell
```bash
docker exec -it lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=gold

spark-sql> SELECT * FROM gold.dim_province LIMIT 5;
spark-sql> SELECT COUNT(*) FROM gold.dim_province;
```

### Option 2: PySpark Interactive
```bash
docker exec -it lakehouse_spark_master /opt/spark/bin/pyspark \
  --conf spark.sql.defaultCatalog=gold
```

```python
df = spark.table("gold.dim_province")
df.show()
df.printSchema()
df.filter("is_city = true").show()
```

### Option 3: Python Script
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Query_Dim_Province") \
    .config("spark.sql.defaultCatalog", "gold") \
    .getOrCreate()

df = spark.table("gold.dim_province")
df.show()
```

## 📁 View Data in MinIO

1. Open: http://localhost:9001
2. Login: `minioadmin` / `minioadmin123`
3. Navigate: **gold** bucket → `lakehouse/gold/dim_province/`
4. Download Parquet files to inspect

## 🔍 Common Queries

### Central Municipalities
```sql
SELECT province_name, province_name_afterLaw, region
FROM gold.dim_province 
WHERE is_city = true
ORDER BY province_name;
```

Expected: 5 rows (Đà Nẵng, Hà Nội, Cần Thơ, Hải Phòng, Hồ Chí Minh)

### Provinces with Name Changes
```sql
SELECT province_name, province_name_afterLaw
FROM gold.dim_province 
WHERE province_name != province_name_afterLaw
ORDER BY province_name;
```

Expected: ~30 rows with merger mappings

### Count by Region
```sql
SELECT region, COUNT(*) as count
FROM gold.dim_province 
GROUP BY region
ORDER BY count DESC;
```

Expected: Mekong_Delta has most provinces

## 🛠️ Troubleshooting

### Table Not Found
```bash
# Check database exists
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=gold \
  -e "SHOW DATABASES;"

# Re-run job
.\scripts\run-gold-dim-province.ps1
```

### CSV Not Found
```bash
# Check file exists
docker exec lakehouse_spark_master ls -la /data/VietNam_Province/

# Copy file if missing
docker cp ./data/VietNam_Province/ lakehouse_spark_master:/data/
```

### Hive Metastore Error
```bash
# Check service is running
docker ps | grep hive-metastore

# Restart if needed
docker restart lakehouse_hive_metastore
```

## 📋 Next Steps

After successful run:

1. ✅ Use `dim_province` in fact tables (join on `province_sk`)
2. ✅ Create `dim_date` dimension
3. ✅ Build `fact_tiktok_engagement` with province FK
4. ✅ Create dashboards/reports using this dimension

## 📚 Documentation

- Full README: [README.md](README.md)
- Gold Layer Overview: [../README.md](../README.md)
- Config Details: [config.py](config.py)

