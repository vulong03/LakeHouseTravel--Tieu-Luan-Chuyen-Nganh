"""
Check: Số sequences tạo ra với SEQUENCE_LENGTH = 4 vs 6
Chạy trong Spark container để biết impact của việc tăng sequence length.
"""
import sys
sys.path.append('/opt/spark/jobs')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import numpy as np

DL_FEATURES_TABLE = "gold.gold.fact_province_month_dl_features"
TARGET = "hotel_review_volume"
LAG_FEATURES = [
    "hotel_vol_lag_1", "hotel_vol_lag_2", "hotel_vol_lag_3",
    "hotel_vol_lag_12", "hotel_vol_rolling_3m", "hotel_vol_momentum",
]

spark = SparkSession.builder \
    .appName("Check_Sequences") \
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

print("\n" + "=" * 70)
print("CHECK: SEQUENCE COUNT vs SEQUENCE_LENGTH")
print("=" * 70)

# Load data
df = spark.table(DL_FEATURES_TABLE)
df = df.select("province_sk", "province_name", "year_month", TARGET, *LAG_FEATURES)
df = df.dropna(subset=LAG_FEATURES + [TARGET])
df = df.filter(F.col(TARGET) > 0)

# Count months per province
months_per_province = (
    df.groupBy("province_sk", "province_name")
      .agg(
          F.count("*").alias("num_months"),
          F.min("year_month").alias("min_ym"),
          F.max("year_month").alias("max_ym"),
      )
      .orderBy("num_months")
)

df_pd = months_per_province.toPandas()

print(f"\nTotal provinces: {len(df_pd)}")
print(f"\nDistribution of months per province:")
print(df_pd["num_months"].describe().to_string())

print("\n\nMonths distribution:")
bins = [0, 5, 7, 10, 15, 20, 25, 100]
labels = ["≤5", "6-7", "8-10", "11-15", "16-20", "21-25", ">25"]
df_pd["bucket"] = pd.cut(df_pd["num_months"], bins=bins, labels=labels)
print(df_pd["bucket"].value_counts().sort_index().to_string())

print("\n\n--- SEQUENCE COUNT SIMULATION ---")
for seq_len in [4, 5, 6, 7, 8]:
    # Province cần ít nhất (seq_len + 1) tháng để tạo ≥1 sequence
    min_months_needed = seq_len + 1
    valid_provinces = df_pd[df_pd["num_months"] >= min_months_needed]
    total_seqs = (valid_provinces["num_months"] - seq_len).sum()
    dropped_provinces = len(df_pd) - len(valid_provinces)
    print(f"  SEQUENCE_LENGTH={seq_len}: {total_seqs} sequences | "
          f"{len(valid_provinces)}/{len(df_pd)} provinces valid | "
          f"{dropped_provinces} provinces dropped")

print("\n\n--- PROVINCES WITH FEW MONTHS (risk of being dropped at seq_len=6) ---")
risky = df_pd[df_pd["num_months"] <= 8][["province_name", "num_months", "min_ym", "max_ym"]]
if len(risky) > 0:
    print(risky.to_string(index=False))
else:
    print("  Không có tỉnh nào bị ảnh hưởng (tất cả có > 8 tháng)")

spark.stop()
