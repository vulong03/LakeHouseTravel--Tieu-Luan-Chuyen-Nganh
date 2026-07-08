from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckNLPv2") \
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

print("Checking table existence...")
try:
    tbl = "gold.gold.fact_comment_nlp_v2"
    exists = spark.catalog.tableExists(tbl)
    print(f"Table {tbl} exists: {exists}")
    if exists:
        count = spark.table(tbl).count()
        print(f"Row count: {count}")
        spark.table(tbl).select("comment_sk", "aspect_scenery", "aspect_food", "aspect_price").show(5)
except Exception as e:
    print("Error:", e)

spark.stop()
