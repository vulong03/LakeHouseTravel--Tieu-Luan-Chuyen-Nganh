# Spark + Iceberg Quick Reference

## 🚀 Running Spark Jobs

### Run Bronze Ingestion
````powershell
docker exec lakehouse_spark spark-submit \
  --master local[*] \
  /opt/spark/jobs/01_ingest_bronze.py
````

### Interactive PySpark Shell
````powershell
docker exec -it lakehouse_spark pyspark
````

### Interactive Spark SQL
````powershell
docker exec -it lakehouse_spark spark-sql \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hive \
  --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083
````

---

## 📊 Iceberg Table Operations

### Create Database
````sql
CREATE DATABASE IF NOT EXISTS lakehouse.bronze LOCATION 's3a://bronze/';
CREATE DATABASE IF NOT EXISTS lakehouse.silver LOCATION 's3a://silver/';
CREATE DATABASE IF NOT EXISTS lakehouse.gold LOCATION 's3a://gold/';
````

### Create Iceberg Table
````python
df.writeTo("lakehouse.bronze.hotels") \
    .using("iceberg") \
    .createOrReplace()
````

### Read Iceberg Table
````python
df = spark.table("lakehouse.bronze.hotels")
df.show()
````

### Show Tables
````sql
SHOW DATABASES;
SHOW TABLES IN lakehouse.bronze;
````

### Describe Table
````sql
DESCRIBE EXTENDED lakehouse.bronze.hotels;
````

### Show Snapshots (Version History)
````sql
SELECT * FROM lakehouse.bronze.hotels.snapshots;
````

### Time Travel Query
````python
# By snapshot ID
df = spark.read \
    .option("snapshot-id", 123456789) \
    .table("lakehouse.bronze.hotels")

# By timestamp
df = spark.read \
    .option("as-of-timestamp", "2025-10-22 10:00:00") \
    .table("lakehouse.bronze.hotels")
````

---

## 🗄️ S3/MinIO Operations

### List Files in Bucket
````python
files = spark.sparkContext.wholeTextFiles("s3a://bronze/raw/").keys().collect()
for f in files:
    print(f)
````

### Read CSV from MinIO
````python
df = spark.read.csv("s3a://bronze/raw/hotels.csv", header=True, inferSchema=True)
````

### Write Parquet to MinIO
````python
df.write.parquet("s3a://silver/hotels/", mode="overwrite")
````

---

## 📈 Common Spark Commands

### Show Spark Configuration
````python
spark.conf.get("spark.sql.catalog.lakehouse.uri")
spark.conf.get("spark.hadoop.fs.s3a.endpoint")
````

### Check Hive Metastore Connection
````sql
SHOW DATABASES;
````

### View Spark UI
- http://localhost:4040 (when job is running)
- http://localhost:8080 (Spark Master UI)

---

## 🐛 Troubleshooting

### Check Spark Logs
````powershell
docker logs lakehouse_spark
````

### Test Hive Metastore Connection
````powershell
docker exec lakehouse_spark nc -zv hive-metastore 9083
````

### Test MinIO Connection
````powershell
docker exec lakehouse_spark curl -I http://minio:9000
````

### Access Spark Container
````powershell
docker exec -it lakehouse_spark /bin/bash
````

---

## 📝 Example Workflow

````python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Create Spark session
spark = SparkSession.builder \
    .appName("TourismETL") \
    .enableHiveSupport() \
    .getOrCreate()

# Read CSV from Bronze
df = spark.read.csv("s3a://bronze/raw/hotels.csv", header=True, inferSchema=True)

# Transform data
df_clean = df.filter(col("rating") > 0) \
    .withColumn("processed_at", current_timestamp())

# Write to Silver as Iceberg table
df_clean.writeTo("lakehouse.silver.hotels") \
    .using("iceberg") \
    .createOrReplace()

# Verify
spark.table("lakehouse.silver.hotels").show()

# Stop
spark.stop()
````

---

## 📚 Iceberg Features

| Feature | Command | Purpose |
|---------|---------|---------|
| **ACID Transactions** | Automatic | Concurrent reads/writes |
| **Schema Evolution** | `ALTER TABLE ADD COLUMN` | Add/remove columns safely |
| **Time Travel** | `.option("snapshot-id", id)` | Query historical data |
| **Partition Evolution** | `ALTER TABLE SET PARTITION` | Change partitioning without rewrite |
| **Hidden Partitioning** | `.partitionedBy(days("timestamp"))` | Automatic partition management |

---

## 🎯 Next Steps

1. Upload actual CSV data to MinIO
2. Run Bronze ingestion job
3. Create Silver transformation job
4. Create Gold aggregation job
5. Add Airflow for orchestration
