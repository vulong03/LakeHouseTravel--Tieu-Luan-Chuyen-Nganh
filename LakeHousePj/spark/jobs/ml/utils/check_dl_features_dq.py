"""
Check raw Bronze CSV để tìm nguyên nhân shares bị inflate
So sánh Bronze → Silver → Gold để trace data
"""
import sys
sys.path.append('/opt/spark/jobs')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Check_Shares_Source") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gold.type", "hive") \
    .config("spark.sql.catalog.gold.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.gold.warehouse", "s3a://gold/lakehouse") \
    .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.silver.type", "hive") \
    .config("spark.sql.catalog.silver.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.silver.warehouse", "s3a://silver/lakehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("=" * 70)
print("CHECK: Silver tiktok_post_metadata - shares vs likes vs source_file")
print("=" * 70)

sv = spark.table("silver.silver.tiktok_post_metadata")

# Top 20 posts by shares - kèm source_file để trace về Bronze
print("\n[TOP 20 BY shares - kèm source_file để trace]")
sv.select(
    "post_url", "likes", "shares", "saves",
    "crawl_time", "scrape_timestamp",
    "source_file"
).orderBy(F.col("shares").desc()).show(20, truncate=False)

# Xem phân phối: có bao nhiêu post shares > 100K
print("\n[PHÂN PHỐI shares theo dải giá trị]")
sv.select(
    F.sum(F.when(F.col("shares") == 0,                  1).otherwise(0)).alias("shares_0"),
    F.sum(F.when(F.col("shares").between(1, 999),        1).otherwise(0)).alias("shares_1_999"),
    F.sum(F.when(F.col("shares").between(1000, 9999),    1).otherwise(0)).alias("shares_1K_9K"),
    F.sum(F.when(F.col("shares").between(10000, 99999),  1).otherwise(0)).alias("shares_10K_99K"),
    F.sum(F.when(F.col("shares").between(100000, 999999),1).otherwise(0)).alias("shares_100K_999K"),
    F.sum(F.when(F.col("shares") >= 1000000,             1).otherwise(0)).alias("shares_over_1M"),
    F.sum(F.col("shares").isNull().cast("int")).alias("shares_null"),
    F.count("*").alias("total"),
).show(truncate=False)

# So sánh shares vs likes cho những post có shares cực cao
print("\n[POSTS có shares > likes * 100 (tỉ lệ bất thường)]")
sv.filter(
    (F.col("shares") > F.col("likes") * 100) &
    (F.col("shares") > 100000)
).select(
    "post_url", "likes", "shares", "saves",
    "source_file", "crawl_time"
).orderBy(F.col("shares").desc()).show(20, truncate=False)

# Đọc thử 1 file Bronze có shares cao nhất
print("\n[ĐỌC THỬ RAW BRONZE: lấy source_file của top-share post]")
top_source = sv.orderBy(F.col("shares").desc()).select("source_file", "post_url", "shares").first()
if top_source:
    print(f"  Post URL: {top_source['post_url']}")
    print(f"  Shares in Silver: {top_source['shares']}")
    print(f"  Source file: {top_source['source_file']}")
    
    # Đọc bronze file này
    bronze_path = f"s3a://bronze/lakehouse/tiktok_comments/raw/{top_source['source_file']}"
    try:
        lines_df = spark.read.text(bronze_path)
        lines = [row.value for row in lines_df.collect()]
        print(f"\n  Bronze file - First 20 lines:")
        for i, line in enumerate(lines[:20]):
            print(f"    [{i+1:02d}] {line}")
    except Exception as e:
        print(f"  Cannot read bronze file: {e}")

spark.stop()
print("\nDONE")
