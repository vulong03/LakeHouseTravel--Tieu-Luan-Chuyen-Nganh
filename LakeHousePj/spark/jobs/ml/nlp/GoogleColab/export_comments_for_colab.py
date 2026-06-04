"""
NLP Pipeline Utility: Export Comments for Google Colab Inference
================================================================
Reads comments from dim_comment (joins with dim_post and silver Tiktok comments),
and exports them as a single, compressed Parquet file to /data/comments_to_score.parquet.
This file can be easily uploaded to Google Colab for GPU-accelerated inference.
"""

import sys
sys.path.append('/opt/spark/jobs')
sys.path.append('/opt/spark/jobs/ml/nlp')

from pyspark.sql import SparkSession
from datetime import datetime
import pandas as pd
from config import DIM_COMMENT_TABLE, SILVER_COMMENTS_TABLE


def create_spark_session():
    return SparkSession.builder \
        .appName("NLP_Export_For_Colab") \
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


def main():
    print("=" * 70)
    print("Exporting Comments for Google Colab Inference")
    print("=" * 70)
    print(f"Start Time: {datetime.now()}")

    spark = create_spark_session()

    try:
        print("\n[1/2] Querying comments from dim_comment...")
        query = f"""
            SELECT
                c.comment_sk,
                c.post_sk,
                p.province_sk,
                c.comment_date_sk,
                c.comment_text,
                c.comment_level,
                sc.likes as comment_likes
            FROM {DIM_COMMENT_TABLE} c
            INNER JOIN gold.gold.dim_post p ON c.post_sk = p.post_sk
            LEFT JOIN {SILVER_COMMENTS_TABLE} sc
                ON c.post_url_nk = sc.post_url AND c.stt = sc.stt
            WHERE c.comment_text IS NOT NULL
                AND LENGTH(TRIM(c.comment_text)) > 0
                AND c.is_active = TRUE
                AND p.province_sk IS NOT NULL
        """
        
        df = spark.sql(query)
        print("  Query submitted. Fetching data and converting to Pandas...")
        pdf = df.toPandas()
        total_rows = len(pdf)
        print(f"  Successfully loaded {total_rows:,} comments.")

        # Save to single parquet file
        import os
        os.makedirs("/data/GoogleColab", exist_ok=True)
        output_path = "/data/GoogleColab/comments_to_score.parquet"
        print(f"\n[2/2] Saving to single compressed Parquet: {output_path}...")
        pdf.to_parquet(output_path, index=False, compression="snappy")
        
        print("\n🎉 EXPORT SUCCESSFUL!")
        print(f"  Output path in container: {output_path}")
        print(f"  Output path on Host:      LakeHousePj/data/GoogleColab/comments_to_score.parquet")
        print(f"  Rows exported:            {total_rows:,}")
        print("  Next Step: Upload this parquet file and your 'phobert_multi_task.pt' model to Colab.")

    except Exception as e:
        print(f"\n❌ ERROR during export: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
