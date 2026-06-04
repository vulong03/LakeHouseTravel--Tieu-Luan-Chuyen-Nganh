import sys
sys.path.append('/opt/spark/jobs')

import pandas as pd
from pyspark.sql import SparkSession
from config import SENTIMENT_LABELS

MAX_PER_CLASS = 27_000

def main():
    print("Extracting exactly 81,000 stratified samples from weak labeled parquet...")
    spark = SparkSession.builder \
        .appName("NLP_Extract_81K_Sample") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    try:
        # 1. Đọc dữ liệu từ MinIO
        df = spark.read.parquet("s3a://gold/ml_training/nlp_weak_labeled.parquet")
        pdf = df.toPandas()
        print(f"  Loaded {len(pdf):,} raw samples from MinIO")

        # 2. Thực hiện phân tầng lấy tối đa 27k mẫu cho mỗi lớp cảm xúc (Negative, Neutral, Positive)
        parts = []
        for label in SENTIMENT_LABELS:
            sub = pdf[pdf['sentiment_label'] == label]
            n = min(len(sub), MAX_PER_CLASS)
            parts.append(sub.sample(n=n, random_state=42) if len(sub) > n else sub)

        pdf_out = pd.concat(parts, ignore_index=True).sample(frac=1, random_state=42)
        print(f"  Sampled dataset size: {len(pdf_out):,}")
        print(pdf_out['sentiment_label'].value_counts())

        # 3. Xuất ra file CSV cục bộ
        output_path = "/data/GoogleColab/nlp_weak_labeled_81k.csv"
        pdf_out.to_csv(output_path, index=False)
        print(f"Success! Saved 81,000 stratified samples to {output_path}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
