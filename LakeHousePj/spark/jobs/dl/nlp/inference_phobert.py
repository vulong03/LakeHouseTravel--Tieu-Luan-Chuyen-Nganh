"""
NLP Pipeline Step 3: Inference — Re-score all comments with fine-tuned PhoBERT
===============================================================================
Reads 465K comments from dim_comment, runs PhoBERT inference in batches,
writes results to gold.gold.fact_comment_nlp_v2

Output columns per comment:
  - sentiment_score (0.0-1.0 continuous)
  - sentiment_label (negative/neutral/positive)
  - aspect_scenery, aspect_food, aspect_price, aspect_service,
    aspect_transport, aspect_accommodation (0.0-1.0 each)
  - intent_label (recommend/complain/question/share)
  - intent_confidence (0.0-1.0)
  - word_count, emoji_count (kept from v1)
"""

import sys
import os

# Set Hugging Face cache directories to a writable location
os.environ['HF_HOME'] = '/tmp/huggingface'
os.environ['TRANSFORMERS_CACHE'] = '/tmp/huggingface'

sys.path.append('/opt/spark/jobs')


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType,
    DoubleType, StringType, TimestampType,
)
from datetime import datetime
import numpy as np
import pandas as pd
from pyspark.sql.functions import pandas_udf

import torch
import mlflow.pytorch

from config import (
    DIM_COMMENT_TABLE, SILVER_COMMENTS_TABLE,
    FINE_TUNED_MODEL_NAME,
    MLFLOW_TRACKING_URI,
    MAX_SEQ_LENGTH,
    SENTIMENT_LABELS, ASPECT_LABELS, INTENT_LABELS,
    PHOBERT_MODEL_NAME,
)

from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.gold_job_logger import get_gold_logger

GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "fact_comment_nlp_v2"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"


def create_spark_session():
    return SparkSession.builder \
        .appName("NLP_PhoBERT_Inference") \
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
        .config("spark.executor.memory", "2g") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "64") \
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

        # Aspects (multi-label probabilities)
        StructField("aspect_scenery", DoubleType(), True),
        StructField("aspect_food", DoubleType(), True),
        StructField("aspect_price", DoubleType(), True),
        StructField("aspect_service", DoubleType(), True),
        StructField("aspect_transport", DoubleType(), True),
        StructField("aspect_accommodation", DoubleType(), True),

        # Intent
        StructField("intent_label", StringType(), True),
        StructField("intent_confidence", DoubleType(), True),

        # Basic text stats (kept from v1)
        StructField("word_count", IntegerType(), True),
        StructField("emoji_count", IntegerType(), True),
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


def load_comments(spark):
    """Load comments with keys from dim_comment + dim_post."""
    print("\n[1/3] Loading comments...")

    df = spark.sql(f"""
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
    """)

    total = df.count()
    print(f"  Loaded {total:,} comments")
    return df


def create_inference_udf():
    """
    Pandas UDF that runs PhoBERT inference on batches of text.
    Loads model once per executor via closure.
    """

    output_schema = StructType([
        StructField("sentiment_score", DoubleType(), True),
        StructField("sentiment_label", StringType(), True),
        StructField("aspect_scenery", DoubleType(), True),
        StructField("aspect_food", DoubleType(), True),
        StructField("aspect_price", DoubleType(), True),
        StructField("aspect_service", DoubleType(), True),
        StructField("aspect_transport", DoubleType(), True),
        StructField("aspect_accommodation", DoubleType(), True),
        StructField("intent_label", StringType(), True),
        StructField("intent_confidence", DoubleType(), True),
        StructField("word_count", IntegerType(), True),
        StructField("emoji_count", IntegerType(), True),
    ])

    @pandas_udf(output_schema)
    def phobert_inference(texts: pd.Series) -> pd.DataFrame:
        import os as _os
        _os.environ['HF_HOME'] = '/tmp/huggingface'
        _os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
        _os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin123'
        _os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://minio:9000'

        import sys as _sys
        if '/opt/spark/jobs/dl/nlp' not in _sys.path:
            _sys.path.append('/opt/spark/jobs/dl/nlp')

        import torch as _torch
        import re as _re
        import emoji as _emoji_lib
        from transformers import AutoTokenizer

        # Load model once per executor
        if getattr(phobert_inference, '_model', None) is None:

            _mlflow_uri = MLFLOW_TRACKING_URI
            import mlflow as _mlflow
            _mlflow.set_tracking_uri(_mlflow_uri)

            try:
                model_uri = f"models:/{FINE_TUNED_MODEL_NAME}/latest"
                phobert_inference._model = _mlflow.pytorch.load_model(model_uri)
                phobert_inference._model.eval()
            except Exception as e:
                import traceback
                print(f"❌ ERROR LOADING MODEL: {e}")
                traceback.print_exc()
                phobert_inference._model = None

            phobert_inference._tokenizer = AutoTokenizer.from_pretrained(PHOBERT_MODEL_NAME)
            phobert_inference._device = _torch.device("cpu")

        model = phobert_inference._model
        tokenizer = phobert_inference._tokenizer
        device = phobert_inference._device

        # ── Pre-compute basic stats ──────────────────────────────────────────
        texts_list = texts.tolist()
        word_counts = [len(str(t).split()) if t else 0 for t in texts_list]
        emoji_counts = [sum(1 for c in str(t) if c in _emoji_lib.EMOJI_DATA) if t else 0 for t in texts_list]

        # Clean texts
        def _clean(t):
            s = str(t) if t else ""
            s = _re.sub(r'http\S+|www\S+|@\w+', '', s)
            return ' '.join(s.split()).strip()

        cleaned = [_clean(t) for t in texts_list]

        # ── Default results for empty / model-not-loaded rows ────────────────
        def _default(wc, ec):
            return {
                "sentiment_score": 0.5, "sentiment_label": "neutral",
                "aspect_scenery": 0.0, "aspect_food": 0.0,
                "aspect_price": 0.0, "aspect_service": 0.0,
                "aspect_transport": 0.0, "aspect_accommodation": 0.0,
                "intent_label": "share", "intent_confidence": 0.5,
                "word_count": wc, "emoji_count": ec,
            }

        results = [None] * len(texts_list)

        # Indices that need model inference
        valid_idx = [i for i, t in enumerate(cleaned) if model is not None and len(t) >= 3]
        skip_idx  = [i for i in range(len(texts_list)) if i not in valid_idx]

        for i in skip_idx:
            results[i] = _default(word_counts[i], emoji_counts[i])

        # ── Batch inference ──────────────────────────────────────────────────
        MINI_BATCH = 32
        for batch_start in range(0, len(valid_idx), MINI_BATCH):
            batch_ids = valid_idx[batch_start: batch_start + MINI_BATCH]
            batch_texts = [cleaned[i] for i in batch_ids]

            encoding = tokenizer(
                batch_texts,
                max_length=MAX_SEQ_LENGTH,
                padding='max_length',
                truncation=True,
                return_tensors='pt',
            )
            input_ids      = encoding['input_ids'].to(device)
            attention_mask = encoding['attention_mask'].to(device)

            with _torch.no_grad():
                sent_logits, aspect_logits, intent_logits = model(input_ids, attention_mask)

            sent_probs   = _torch.softmax(sent_logits, dim=1).cpu().numpy()
            aspect_probs = _torch.sigmoid(aspect_logits).cpu().numpy()
            intent_probs = _torch.softmax(intent_logits, dim=1).cpu().numpy()

            for j, i in enumerate(batch_ids):
                sp = sent_probs[j]
                ap = aspect_probs[j]
                ip = intent_probs[j]
                intent_idx = int(ip.argmax())
                results[i] = {
                    "sentiment_score": round(float(sp[0]*0.0 + sp[1]*0.5 + sp[2]*1.0), 4),
                    "sentiment_label": SENTIMENT_LABELS[int(sp.argmax())],
                    "aspect_scenery":       round(float(ap[0]), 4),
                    "aspect_food":          round(float(ap[1]), 4),
                    "aspect_price":         round(float(ap[2]), 4),
                    "aspect_service":       round(float(ap[3]), 4),
                    "aspect_transport":     round(float(ap[4]), 4),
                    "aspect_accommodation": round(float(ap[5]), 4),
                    "intent_label":      INTENT_LABELS[intent_idx],
                    "intent_confidence": round(float(ip[intent_idx]), 4),
                    "word_count":  word_counts[i],
                    "emoji_count": emoji_counts[i],
                }

            # Free memory after each mini-batch
            del input_ids, attention_mask, sent_logits, aspect_logits, intent_logits

        return pd.DataFrame(results)

    return phobert_inference


def run_inference(spark, df):
    """Apply PhoBERT inference UDF to all comments."""
    print("\n[2/3] Running PhoBERT inference...")

    udf = create_inference_udf()

    df_result = df.withColumn("nlp", udf(F.col("comment_text")))

    df_final = df_result.select(
        "comment_sk", "post_sk", "province_sk", "comment_date_sk",
        F.col("nlp.sentiment_score"),
        F.col("nlp.sentiment_label"),
        F.col("nlp.aspect_scenery"),
        F.col("nlp.aspect_food"),
        F.col("nlp.aspect_price"),
        F.col("nlp.aspect_service"),
        F.col("nlp.aspect_transport"),
        F.col("nlp.aspect_accommodation"),
        F.col("nlp.intent_label"),
        F.col("nlp.intent_confidence"),
        F.col("nlp.word_count"),
        F.col("nlp.emoji_count"),
        F.coalesce(F.col("comment_likes"), F.lit(0)).cast("long").alias("comment_likes"),
        F.col("comment_level"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )

    return df_final


def write_results(spark, df):
    """Write NLP v2 results to Iceberg table."""
    print("\n[3/3] Writing results...")

    create_nlp_v2_table(spark)

    df.write.format("iceberg").mode("overwrite").saveAsTable(GOLD_TABLE_FULL)

    count = spark.table(GOLD_TABLE_FULL).count()
    print(f"  Wrote {count:,} rows to {GOLD_TABLE_FULL}")
    return count


def main():
    print("=" * 70)
    print("NLP Pipeline Step 3: PhoBERT Inference")
    print("=" * 70)
    print(f"Start: {datetime.now()}")

    spark = create_spark_session()
    logger = get_gold_logger(spark)

    try:
        df = load_comments(spark)
        df_scored = run_inference(spark, df)
        count = write_results(spark, df_scored)

        logger.log_job_success(
            source_path=DIM_COMMENT_TABLE,
            table_name=GOLD_TABLE_FULL,
            records_processed=count,
            job_details={
                "job_type": "nlp_inference_v2",
                "model": FINE_TUNED_MODEL_NAME,
                "tasks": "sentiment+aspect+intent",
            },
        )

        print(f"\nCompleted: {count:,} comments scored")
        print(f"  Table: {GOLD_TABLE_FULL}")

        # Sample
        spark.table(GOLD_TABLE_FULL).select(
            "comment_sk", "sentiment_score", "sentiment_label",
            "aspect_scenery", "aspect_food", "intent_label",
        ).show(10, truncate=False)

    except Exception as e:
        logger.log_job_failure(
            source_path=DIM_COMMENT_TABLE,
            table_name=GOLD_TABLE_FULL,
            error_message=str(e),
        )
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
