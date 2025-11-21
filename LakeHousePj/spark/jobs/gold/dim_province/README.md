# Gold Layer - Dimension Province

## Overview

Build `dim_province` dimension table from Vietnam province master data.

**Purpose**: Create a slowly changing dimension (Type 1) for provinces/cities with administrative merger mappings.

## Architecture

```
Source (CSV)
    ↓
Spark Transformation
    ↓
Gold.dim_province (Iceberg)
```

## Table Schema

| Column | Type | Description |
|--------|------|-------------|
| `province_sk` | INT | Surrogate key (auto-generated) |
| `province_name` | STRING | Current province/city name (Business Key) |
| `province_name_afterLaw` | STRING | Name after 2025 administrative merger |
| `region` | STRING | Geographic region (e.g., Red_River_Delta) |
| `is_city` | BOOLEAN | `true` for 5 central municipalities, `false` for provinces |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |
| `is_active` | BOOLEAN | Active flag (always `true` for Type 1 SCD) |

## Business Rules

### 1. Province Name After Law
Maps current province names to post-merger names based on 2025 administrative policy:
- Example: `Hà Giang` → `Tuyên Quang` (merged)
- Example: `Hà Nội` → `Hà Nội` (no change)

### 2. Central Municipalities (`is_city = true`)
- Đà Nẵng
- Hà Nội
- Cần Thơ
- Hải Phòng
- Hồ Chí Minh

### 3. Surrogate Key Generation
- Sequential integer starting from 1
- Ordered by `province_name` alphabetically

## Source Data

**File**: `/data/VietNam_Province/list_of_provinces_of_vietnam-154j.csv`

**Required Columns**:
- `Province/city`: Province or city name
- `Region`: Geographic region (Red_River_Delta, Northeast, Northwest, etc.)

**Total Records**: 63 provinces/cities

## Transformations

1. **Column Mapping**
   - `Province/city` → `province_name`
   - `Region` → `region`

2. **Business Logic**
   - Map `province_name_afterLaw` using `PROVINCE_AFTER_LAW` dictionary
   - Set `is_city` flag based on `CENTRAL_CITIES` list
   - Generate `province_sk` using `row_number()`

3. **Metadata**
   - Add `created_at` = current timestamp
   - Add `updated_at` = current timestamp
   - Add `is_active` = `true`

## How to Run

### PowerShell (Recommended)
```powershell
cd D:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\LakeHousePj

# Run the job
.\scripts\run-gold-dim-province.ps1

# View job execution logs
.\scripts\query-gold-job-logs.ps1
```

### Manual Spark Submit
```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.app.name="Gold_Dim_Province" \
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.gold.type=hive \
  --conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 \
  --conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse \
  --conf spark.sql.defaultCatalog=gold \
  --driver-memory 1g \
  --executor-memory 768m \
  --executor-cores 2 \
  --num-executors 1 \
  /opt/spark/jobs/gold/dim_province/dim_province_job.py
```

## Query Examples

### View All Provinces
```sql
SELECT * FROM gold.dim_province 
ORDER BY province_sk;
```

### Central Municipalities Only
```sql
SELECT province_sk, province_name, province_name_afterLaw, region
FROM gold.dim_province 
WHERE is_city = true
ORDER BY province_name;
```

### Provinces with Name Changes
```sql
SELECT province_sk, province_name, province_name_afterLaw, region
FROM gold.dim_province 
WHERE province_name != province_name_afterLaw
ORDER BY province_name;
```

### Count by Region
```sql
SELECT region, COUNT(*) as province_count
FROM gold.dim_province 
GROUP BY region
ORDER BY province_count DESC;
```

## Validation

After running the job, expected results:

- **Total records**: 63
- **Central municipalities**: 5
- **Provinces**: 58
- **Provinces with name changes**: ~30 (based on merger policy)

## Job Execution Logging

All job runs are logged to PostgreSQL `file_ingestion_log` table:

```sql
SELECT * FROM file_ingestion_log 
WHERE layer = 'gold' 
  AND table_name = 'gold.dim_province'
ORDER BY ingestion_timestamp DESC;
```

**Logged Information**:
- Source path
- Record count
- Execution status (success/failed)
- Execution time
- Job details (cities count, name changes, etc.)
- Error messages (if failed)

**View Logs**:
```powershell
.\scripts\query-gold-job-logs.ps1
```

## Dependencies

- Hive Metastore (for Iceberg catalog)
- MinIO (S3-compatible storage for Gold bucket)
- PostgreSQL (for Hive metadata + job logging)
- Source CSV: `/data/VietNam_Province/list_of_provinces_of_vietnam-154j.csv`

## Storage Location

- **Bucket**: `gold`
- **Path**: `s3a://gold/lakehouse/gold/dim_province/`
- **Format**: Parquet (Snappy compression)
- **Catalog**: Iceberg with Hive Metastore

## Maintenance

### Refresh Data
Run the job again - it will perform a full refresh (overwrite):
```powershell
.\scripts\run-gold-dim-province.ps1
```

### Update Merger Policy
Edit `config.py` → `PROVINCE_AFTER_LAW` dictionary, then re-run job.

### View Table Metadata
```sql
DESCRIBE EXTENDED gold.dim_province;
```

### View Iceberg History
```sql
SELECT * FROM gold.dim_province.history;
```

## Troubleshooting

### Job fails with "Database not found"
The job auto-creates `gold` database. If it fails, manually create:
```sql
CREATE DATABASE IF NOT EXISTS gold;
```

### Job fails with "File not found"
Check CSV exists:
```bash
docker exec lakehouse_spark_master ls -la /data/VietNam_Province/
```

### No data in table
Check Spark logs:
```bash
docker logs lakehouse_spark_master | tail -100
```

### MinIO connection error
Verify MinIO is running:
```bash
docker ps | grep minio
```
Check MinIO console: http://localhost:9001

## Future Enhancements

- [ ] Implement SCD Type 2 for historical tracking
- [ ] Add province code (ISO/FIPS)
- [ ] Add population/area from CSV
- [ ] Add latitude/longitude for mapping
- [ ] Add parent-child hierarchy (district → province → region)

