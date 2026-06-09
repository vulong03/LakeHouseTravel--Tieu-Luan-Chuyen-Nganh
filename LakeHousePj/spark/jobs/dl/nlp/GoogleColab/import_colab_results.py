"""
NLP Pipeline Utility: Import Google Colab Inference Results
===========================================================
Reads the scored comments file from /data/colab_inference_results.parquet,
reconstructs the final table schema for gold.gold.fact_comment_nlp_v2,
writes it to Iceberg, and registers success in the gold job logger.
"""

import sys
sys.path.append('/opt/spark/jobs')
sys.path.append('/opt/spark/jobs/dl/nlp')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType,
    DoubleType, StringType, TimestampType,
)
from datetime import datetime
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.gold_job_logger import get_gold_logger
from config import DIM_COMMENT_TABLE

GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "fact_comment_nlp_v2"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"
INPUT_RESULTS_PATH = "/data/GoogleColab/colab_inference_results.parquet"


def create_spark_session():
    return SparkSession.builder \
        .appName("NLP_Import_Colab_Results") \
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
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1024") \
        .getOrCreate()


def create_nlp_v2_table(spark):
    schema = StructType([
        StructField("comment_sk", LongType(), False),
        StructField("post_sk", LongType(), True),
        StructField("province_sk", IntegerType(), True),
        StructField("comment_date_sk", IntegerType(), True),

        # Sentiment (continuous)
        StructField("sentiment_score", DoubleType(), True),
        StructField("sentiment_label", StringType(), True),
        StructField("sentiment_confidence", DoubleType(), True),
        StructField("sentiment_negative_prob", DoubleType(), True),
        StructField("sentiment_neutral_prob", DoubleType(), True),
        StructField("sentiment_positive_prob", DoubleType(), True),

        # Aspects (multi-label probabilities)
        StructField("aspect_scenery", DoubleType(), True),
        StructField("aspect_food", DoubleType(), True),
        StructField("aspect_price", DoubleType(), True),
        StructField("aspect_service", DoubleType(), True),
        StructField("aspect_transport", DoubleType(), True),
        StructField("aspect_accommodation", DoubleType(), True),
        StructField("aspect_labels", StringType(), True),

        # Intent
        StructField("intent_label", StringType(), True),
        StructField("intent_confidence", DoubleType(), True),

        # Basic text stats (kept from v1)
        StructField("word_count", IntegerType(), True),
        StructField("unique_word_ratio", DoubleType(), True),
        StructField("emoji_count", IntegerType(), True),
        StructField("positive_emoji_count", LongType(), True),
        StructField("negative_emoji_count", LongType(), True),
        StructField("comment_likes", LongType(), True),
        StructField("comment_level", IntegerType(), True),

        # Metadata
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ])

    create_iceberg_table_if_not_exists(
        spark=spark, database=GOLD_DATABASE, table_name=GOLD_TABLE,
        schema=schema, partition_by=["province_sk"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
        catalog=GOLD_CATALOG,
    )

    # Check if new columns exist, and if not, add them via ALTER TABLE
    try:
        if spark.catalog.tableExists(GOLD_TABLE_FULL):
            existing_cols = [c.name for c in spark.catalog.listColumns(GOLD_TABLE_FULL)]
            new_cols = []
            if "sentiment_confidence" not in existing_cols:
                new_cols.append("sentiment_confidence DOUBLE")
            if "sentiment_negative_prob" not in existing_cols:
                new_cols.append("sentiment_negative_prob DOUBLE")
            if "sentiment_neutral_prob" not in existing_cols:
                new_cols.append("sentiment_neutral_prob DOUBLE")
            if "sentiment_positive_prob" not in existing_cols:
                new_cols.append("sentiment_positive_prob DOUBLE")
            if "aspect_labels" not in existing_cols:
                new_cols.append("aspect_labels STRING")
            if "unique_word_ratio" not in existing_cols:
                new_cols.append("unique_word_ratio DOUBLE")
            if "positive_emoji_count" not in existing_cols:
                new_cols.append("positive_emoji_count BIGINT")
            if "negative_emoji_count" not in existing_cols:
                new_cols.append("negative_emoji_count BIGINT")
            
            if new_cols:
                print(f"   Adding new columns to existing Iceberg table: {new_cols}")
                for col_def in new_cols:
                    spark.sql(f"ALTER TABLE {GOLD_TABLE_FULL} ADD COLUMN {col_def}")
    except Exception as e:
        print(f"⚠️ Warning checking/altering table: {e}")


def main():
    print("=" * 70)
    print("Importing Google Colab PhoBERT Inference Results")
    print("=" * 70)
    print(f"Start Time: {datetime.now()}")

    spark = create_spark_session()
    logger = get_gold_logger(spark)

    try:
        # 1. Read Colab Results from parquet
        print(f"\n[1/3] Reading results from {INPUT_RESULTS_PATH}...")
        df_raw = spark.read.parquet(INPUT_RESULTS_PATH)
        
        # 2. Add metadata columns and cast to correct Iceberg types
        print("[2/3] Mapping columns and casting schema...")
        df_final = df_raw.select(
            F.col("comment_sk").cast("long"),
            F.col("post_sk").cast("long"),
            F.col("province_sk").cast("int"),
            F.col("comment_date_sk").cast("int"),
            F.col("sentiment_score").cast("double"),
            F.col("sentiment_label").cast("string"),
            F.col("sentiment_confidence").cast("double"),
            F.col("sentiment_negative_prob").cast("double"),
            F.col("sentiment_neutral_prob").cast("double"),
            F.col("sentiment_positive_prob").cast("double"),
            F.col("aspect_scenery").cast("double"),
            F.col("aspect_food").cast("double"),
            F.col("aspect_price").cast("double"),
            F.col("aspect_service").cast("double"),
            F.col("aspect_transport").cast("double"),
            F.col("aspect_accommodation").cast("double"),
            F.col("aspect_labels").cast("string"),
            F.col("intent_label").cast("string"),
            F.col("intent_confidence").cast("double"),
            F.col("word_count").cast("int"),
            F.col("unique_word_ratio").cast("double"),
            F.col("emoji_count").cast("int"),
            F.col("positive_emoji_count").cast("long"),
            F.col("negative_emoji_count").cast("long"),
            F.coalesce(F.col("comment_likes"), F.lit(0)).cast("long").alias("comment_likes"),
            F.col("comment_level").cast("int"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )

        # Ensure the table is created
        create_nlp_v2_table(spark)

        # Align column order with the existing Iceberg table to avoid position incompatibility
        table_cols = spark.table(GOLD_TABLE_FULL).columns
        df_final = df_final.select(*table_cols)

        # 3. Write to Iceberg
        print(f"\n[3/3] Overwriting Iceberg Table: {GOLD_TABLE_FULL}...")
        df_final.write.format("iceberg").mode("overwrite").saveAsTable(GOLD_TABLE_FULL)

        # Verify Count
        count = spark.table(GOLD_TABLE_FULL).count()
        print(f"\n✓ successfully wrote {count:,} rows to {GOLD_TABLE_FULL}")

        logger.log_job_success(
            source_path=INPUT_RESULTS_PATH,
            table_name=GOLD_TABLE_FULL,
            records_processed=count,
            job_details={
                "job_type": "nlp_inference_v2_colab",
                "method": "google_colab_gpu",
                "tasks": "sentiment+aspect+intent",
            },
        )

        print("\n=== SAMPLE DATA FROM ICEBERG ===")
        spark.table(GOLD_TABLE_FULL).select(
            "comment_sk", "sentiment_score", "sentiment_label",
            "aspect_scenery", "aspect_food", "intent_label",
        ).show(10, truncate=False)

    except Exception as e:
        logger.log_job_failure(
            source_path=INPUT_RESULTS_PATH,
            table_name=GOLD_TABLE_FULL,
            error_message=str(e),
        )
        print(f"\n❌ ERROR during import: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
