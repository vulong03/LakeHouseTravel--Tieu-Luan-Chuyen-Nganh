#!/usr/bin/env python3
"""Quick check Silver layer data"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Create Spark session
spark = SparkSession.builder \
    .appName("Quick_Check_Silver") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hive") \
    .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://silver/") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio_access_key") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio_secret_key") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .enableHiveSupport() \
    .getOrCreate()

print("="*80)
print("🔍 SILVER LAYER QUICK CHECK")
print("="*80)

tables = [
    "lakehouse.silver.tiktok_post_metadata",
    "lakehouse.silver.tiktok_post_comments",
    "lakehouse.silver.hotels_detail",
    "lakehouse.silver.hotels_list",
    "lakehouse.silver.hotels_reviews"
]

for table_name in tables:
    print(f"\n📊 {table_name}")
    print("-"*80)
    
    try:
        df = spark.table(table_name)
        total = df.count()
        cols = len(df.columns)
        
        print(f"✅ Rows: {total:,}")
        print(f"✅ Columns: {cols}")
        
        # Check NULLs in first 5 columns
        print(f"\n⚠️ NULL Check (first 5 cols):")
        for col_name in df.columns[:5]:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count/total*100) if total > 0 else 0
            print(f"   {col_name}: {null_count:,} ({null_pct:.1f}%)")
        
        # Show sample
        print(f"\n📄 Sample (2 rows):")
        df.show(2, truncate=30)
        
    except Exception as e:
        print(f"❌ Error: {e}")

spark.stop()
