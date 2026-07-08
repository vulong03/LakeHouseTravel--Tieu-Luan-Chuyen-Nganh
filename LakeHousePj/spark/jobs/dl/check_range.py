from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("CheckRange") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gold.type", "hive") \
    .config("spark.sql.catalog.gold.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.gold.warehouse", "s3a://gold/lakehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df = spark.table("gold.gold.fact_province_month_dl_features")
df.select(
    F.min("year_month").alias("min_ym"),
    F.max("year_month").alias("max_ym"),
    F.count("*").alias("total_rows")
).show()

# Show min and max year_month after year >= 2023 filter
df.filter(F.col("year") >= 2023).select(
    F.min("year_month").alias("min_ym_filtered"),
    F.max("year_month").alias("max_ym_filtered"),
    F.count("*").alias("filtered_rows")
).show()

spark.stop()
