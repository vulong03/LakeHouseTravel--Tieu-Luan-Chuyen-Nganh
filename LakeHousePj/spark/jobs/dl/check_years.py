from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("CheckYears") \
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
df.groupBy("year").count().orderBy("year").show()
spark.stop()
