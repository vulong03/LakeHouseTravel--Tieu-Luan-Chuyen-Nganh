# Gold Layer - Analytics & Dimensions

## Overview

The Gold layer contains analytics-ready tables optimized for business intelligence and reporting:

- **Dimension Tables**: Master data with business keys (e.g., provinces, dates, products)
- **Fact Tables**: Aggregated metrics and events (e.g., engagement, bookings)
- **Aggregate Tables**: Pre-computed summaries for dashboards

## Architecture

```
Silver Layer (Cleaned Data)
         ↓
   Gold Layer Jobs
         ↓
Iceberg Tables (gold.*)
         ↓
    BI Tools / APIs
```

## Storage Configuration

- **Catalog**: `gold` (Iceberg + Hive Metastore)
- **Warehouse**: `s3a://gold/lakehouse`
- **Bucket**: `gold` (MinIO)
- **Format**: Parquet with Snappy compression

See `spark-defaults.conf`:
```properties
spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.gold.type=hive
spark.sql.catalog.gold.uri=thrift://hive-metastore:9083
spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse
```

## Implemented Jobs

### 1. dim_province
**Status**: ✅ Implemented  
**Path**: `gold/dim_province/`  
**Description**: Province/city dimension with administrative merger mappings  
**Run**: `.\scripts\run-gold-dim-province.ps1`

See: [dim_province/README.md](dim_province/README.md)

### 2. dim_date (Planned)
**Status**: 📋 TODO  
**Description**: Date dimension with fiscal periods, holidays, seasons

### 3. fact_tiktok_engagement (Planned)
**Status**: 📋 TODO  
**Description**: Daily aggregated TikTok metrics by province/keyword

### 4. fact_hotel_bookings (Planned)
**Status**: 📋 TODO  
**Description**: Hotel booking events with ratings and reviews

## Dimensional Model Design

### Star Schema

```
       dim_province
            |
            |
       fact_tiktok_engagement --- dim_date
            |
            |
       dim_keyword
```

### Slowly Changing Dimensions (SCD)

- **Type 1** (Overwrite): `dim_province`, `dim_date`
  - Current state only, no history
  - Use for reference data that changes rarely

- **Type 2** (Historical): Future dimensions like `dim_user`, `dim_product`
  - Maintain history with effective dates
  - Add columns: `effective_from`, `effective_to`, `is_current`

## Naming Conventions

### Tables
- **Dimensions**: `dim_<entity>` (e.g., `dim_province`, `dim_date`)
- **Facts**: `fact_<process>` (e.g., `fact_engagement`, `fact_bookings`)
- **Aggregates**: `agg_<metric>_<grain>` (e.g., `agg_engagement_daily`)

### Columns
- **Surrogate Keys**: `<entity>_sk` (e.g., `province_sk`, `date_sk`)
- **Business Keys**: `<entity>_id` or `<entity>_name` (e.g., `province_name`)
- **Foreign Keys**: `<entity>_sk` matching dimension SK
- **Measures**: descriptive names (e.g., `total_likes`, `avg_rating`)
- **Metadata**: `created_at`, `updated_at`, `is_active`

## How to Run Jobs

### PowerShell Scripts (Recommended)
```powershell
# Run dim_province
.\scripts\run-gold-dim-province.ps1

# (Future) Run all Gold jobs
.\scripts\run-gold-all.ps1
```

### Manual Spark Submit
```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.app.name="Gold_JobName" \
  --conf spark.sql.defaultCatalog=gold \
  --driver-memory 1g \
  --executor-memory 768m \
  /opt/spark/jobs/gold/<job_folder>/<job_name>.py
```

## Query Examples

### List All Gold Tables
```sql
SHOW TABLES IN gold;
```

### Describe Table Schema
```sql
DESCRIBE EXTENDED gold.dim_province;
```

### Sample Query
```sql
SELECT 
    province_name,
    province_name_afterLaw,
    region,
    is_city
FROM gold.dim_province
WHERE is_city = true
ORDER BY province_name;
```

### Join with Facts (Future)
```sql
SELECT 
    p.province_name,
    p.region,
    SUM(f.total_likes) as total_likes
FROM gold.fact_tiktok_engagement f
JOIN gold.dim_province p ON f.province_sk = p.province_sk
GROUP BY p.province_name, p.region
ORDER BY total_likes DESC;
```

## Access via MinIO Console

1. Open: http://localhost:9001
2. Login: `minioadmin` / `minioadmin123`
3. Navigate to: **gold** bucket
4. Browse: `lakehouse/gold/<table_name>/`

## Access via Spark SQL

```bash
# Enter Spark shell
docker exec -it lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=gold

# Query tables
spark-sql> SHOW DATABASES;
spark-sql> USE gold;
spark-sql> SHOW TABLES;
spark-sql> SELECT * FROM dim_province LIMIT 10;
```

## Development Guidelines

### 1. Create New Dimension Job

```bash
# Create folder
mkdir -p spark/jobs/gold/dim_<name>

# Create files
touch spark/jobs/gold/dim_<name>/__init__.py
touch spark/jobs/gold/dim_<name>/config.py
touch spark/jobs/gold/dim_<name>/dim_<name>_job.py
touch spark/jobs/gold/dim_<name>/README.md
```

### 2. Job Template Structure

```python
# config.py
SOURCE_TABLE = "silver.source_table"
GOLD_TABLE = "gold.dim_entity"
BUSINESS_KEY = ["entity_id"]

# dim_entity_job.py
def create_gold_database(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

def create_dim_table(spark):
    # Define schema
    # Create Iceberg table
    pass

def load_source_data(spark):
    # Read from Silver or CSV
    pass

def transform_to_dimension(df):
    # Apply business rules
    # Add surrogate keys
    # Add metadata
    pass

def write_to_gold(spark, df):
    # OVERWRITE or MERGE
    pass

def main():
    spark = get_spark_session(app_name="Gold_DimEntity", catalog="gold")
    create_gold_database(spark)
    create_dim_table(spark)
    source_df = load_source_data(spark)
    dim_df = transform_to_dimension(source_df)
    write_to_gold(spark, dim_df)
```

### 3. Testing

```python
# Validate row counts
assert df.count() > 0

# Validate business keys are unique
assert df.select("business_key").distinct().count() == df.count()

# Validate no nulls in required fields
assert df.filter(col("sk").isNull()).count() == 0
```

## Best Practices

1. **Idempotent Jobs**: Running multiple times produces same result
2. **Atomic Operations**: Use Iceberg ACID transactions
3. **Documentation**: Add README.md for each dimension/fact
4. **Validation**: Check data quality after load
5. **Logging**: Print statistics and sample data
6. **Error Handling**: Try-catch with meaningful messages

## Monitoring

### Job Logs
```bash
# Spark Master logs
docker logs lakehouse_spark_master

# Spark History Server
http://localhost:18080
```

### Table Statistics
```sql
-- Row count
SELECT COUNT(*) FROM gold.dim_province;

-- Table size
SELECT * FROM gold.dim_province.snapshots;
```

### MinIO Metrics
- Console: http://localhost:9001
- Check: Object count, storage size

## Troubleshooting

### Database Not Found
```sql
CREATE DATABASE IF NOT EXISTS gold;
```

### Table Not Found
Re-run the job to create table.

### Hive Metastore Connection Error
```bash
# Check Hive Metastore is running
docker ps | grep hive-metastore

# Check logs
docker logs lakehouse_hive_metastore
```

### MinIO Connection Error
```bash
# Check MinIO is running
docker ps | grep minio

# Check gold bucket exists
docker exec lakehouse_minio_init mc ls myminio/gold
```

## Future Roadmap

- [ ] Implement `dim_date` (date dimension)
- [ ] Implement `fact_tiktok_engagement` (daily metrics)
- [ ] Implement `fact_hotel_bookings` (event fact)
- [ ] Add data quality checks (Great Expectations)
- [ ] Add incremental refresh for large facts
- [ ] Add partition pruning for query optimization
- [ ] Integrate with BI tools (Tableau, Power BI)
- [ ] Add change data capture (CDC) for Type 2 SCD

