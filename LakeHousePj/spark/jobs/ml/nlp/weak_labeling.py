"""
NLP Pipeline Step 1: Weak Labeling
====================================
Auto-label 465K TikTok comments using rule-based signals:
- underthesea sentiment (existing)
- Emoji polarity (existing)
- Keyword matching (new)
- Confidence scoring (new)

Output: Parquet with columns:
  comment_text, sentiment_label, sentiment_confidence,
  aspects (list), intent_label, intent_confidence

Only high-confidence samples (>=0.6) are kept for PhoBERT training.
"""

import sys
import re
sys.path.append('/opt/spark/jobs')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    ArrayType, IntegerType,
)
from datetime import datetime

from config import (
    DIM_COMMENT_TABLE, SILVER_COMMENTS_TABLE,
    LABELED_PARQUET_PATH,
    SENTIMENT_LABELS, SENTIMENT_TO_ID,
    ASPECT_LABELS, ASPECT_KEYWORD_MAP,
    INTENT_LABELS, INTENT_TO_ID,
    POSITIVE_KEYWORDS, NEGATIVE_KEYWORDS,
    QUESTION_KEYWORDS, RECOMMEND_KEYWORDS, COMPLAIN_KEYWORDS,
    POSITIVE_EMOJIS, NEGATIVE_EMOJIS,
)

import pandas as pd
from pyspark.sql.functions import pandas_udf


def create_spark_session():
    return SparkSession.builder \
        .appName("NLP_Weak_Labeling") \
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
        .getOrCreate()


def load_comments(spark):
    """Load comment text from dim_comment (has comment_text, cleaned)."""
    print("\n[1/4] Loading comments...")

    df = spark.sql(f"""
        SELECT
            c.comment_sk,
            c.comment_text,
            c.comment_level,
            c.post_url_nk,
            sc.likes as comment_likes
        FROM {DIM_COMMENT_TABLE} c
        LEFT JOIN {SILVER_COMMENTS_TABLE} sc
            ON c.post_url_nk = sc.post_url AND c.stt = sc.stt
        WHERE c.comment_text IS NOT NULL
            AND LENGTH(TRIM(c.comment_text)) > 3
            AND c.is_active = TRUE
    """)

    total = df.count()
    print(f"  Loaded {total:,} comments with text")
    return df


def create_weak_labeling_udf():
    """
    Pandas UDF that assigns sentiment, aspects, and intent labels
    using keyword + emoji rules. Returns confidence scores.
    """

    output_schema = StructType([
        StructField("sentiment_label", StringType(), False),
        StructField("sentiment_confidence", FloatType(), False),
        StructField("aspects", StringType(), False),
        StructField("intent_label", StringType(), False),
        StructField("intent_confidence", FloatType(), False),
    ])

    @pandas_udf(output_schema)
    def weak_label_batch(texts: pd.Series) -> pd.DataFrame:
        import re as _re
        import emoji as _emoji_lib

        results = []

        for text in texts:
            if not text or len(str(text).strip()) < 3:
                results.append({
                    "sentiment_label": "neutral",
                    "sentiment_confidence": 0.3,
                    "aspects": "",
                    "intent_label": "share",
                    "intent_confidence": 0.3,
                })
                continue

            text_str = str(text)
            text_lower = text_str.lower()
            text_clean = _re.sub(r'http\S+|www\S+|@\w+', '', text_lower)
            text_clean = ' '.join(text_clean.split())

            # ========== SENTIMENT ==========
            pos_signals = 0
            neg_signals = 0
            total_signals = 0

            # Signal 1: Keywords
            pos_kw_count = sum(1 for kw in POSITIVE_KEYWORDS if kw in text_clean)
            neg_kw_count = sum(1 for kw in NEGATIVE_KEYWORDS if kw in text_clean)
            if pos_kw_count > 0:
                pos_signals += min(pos_kw_count, 3)
                total_signals += min(pos_kw_count, 3)
            if neg_kw_count > 0:
                neg_signals += min(neg_kw_count, 3)
                total_signals += min(neg_kw_count, 3)

            # Signal 2: Emojis
            emojis_in_text = [c for c in text_str if c in _emoji_lib.EMOJI_DATA]
            pos_emoji = sum(1 for e in emojis_in_text if e in POSITIVE_EMOJIS)
            neg_emoji = sum(1 for e in emojis_in_text if e in NEGATIVE_EMOJIS)
            if pos_emoji > 0:
                pos_signals += min(pos_emoji, 3)
                total_signals += min(pos_emoji, 3)
            if neg_emoji > 0:
                neg_signals += min(neg_emoji, 3)
                total_signals += min(neg_emoji, 3)

            # Signal 3: Exclamation (weak positive signal)
            excl_count = text_str.count('!')
            if excl_count >= 2:
                pos_signals += 1
                total_signals += 1

            # Signal 4: underthesea (if available)
            try:
                from underthesea import sentiment as _sentiment
                import os, tempfile
                if 'HOME' not in os.environ or not os.access(os.environ.get('HOME', ''), os.W_OK):
                    os.environ['HOME'] = tempfile.mkdtemp()
                uts_result = _sentiment(text_clean)
                if uts_result == 'positive':
                    pos_signals += 2
                    total_signals += 2
                elif uts_result == 'negative':
                    neg_signals += 2
                    total_signals += 2
                else:
                    total_signals += 1
            except Exception:
                total_signals += 1

            # Determine sentiment
            if total_signals == 0:
                sent_label = "neutral"
                sent_conf = 0.4
            elif pos_signals > neg_signals:
                sent_label = "positive"
                sent_conf = min(0.95, 0.4 + (pos_signals / max(total_signals, 1)) * 0.55)
            elif neg_signals > pos_signals:
                sent_label = "negative"
                sent_conf = min(0.95, 0.4 + (neg_signals / max(total_signals, 1)) * 0.55)
            else:
                sent_label = "neutral"
                sent_conf = 0.5

            # ========== ASPECTS (multi-label) ==========
            detected_aspects = []
            for aspect, keywords in ASPECT_KEYWORD_MAP.items():
                matches = sum(1 for kw in keywords if kw in text_clean)
                if matches >= 2:
                    detected_aspects.append(aspect)
                elif matches == 1 and len(text_clean.split()) <= 15:
                    detected_aspects.append(aspect)

            aspects_str = ",".join(detected_aspects) if detected_aspects else ""

            # ========== INTENT ==========
            question_count = sum(1 for kw in QUESTION_KEYWORDS if kw in text_clean)
            recommend_count = sum(1 for kw in RECOMMEND_KEYWORDS if kw in text_clean)
            complain_count = sum(1 for kw in COMPLAIN_KEYWORDS if kw in text_clean)

            intent_scores = {
                "question": question_count * 2,
                "recommend": recommend_count * 2,
                "complain": complain_count * 2,
                "share": 1,
            }

            if '?' in text_str:
                intent_scores["question"] += 3

            best_intent = max(intent_scores, key=intent_scores.get)
            best_score = intent_scores[best_intent]
            total_intent = sum(intent_scores.values())
            intent_conf = min(0.95, best_score / max(total_intent, 1))

            if best_score <= 1:
                best_intent = "share"
                intent_conf = 0.5

            results.append({
                "sentiment_label": sent_label,
                "sentiment_confidence": float(sent_conf),
                "aspects": aspects_str,
                "intent_label": best_intent,
                "intent_confidence": float(intent_conf),
            })

        return pd.DataFrame(results)

    return weak_label_batch


def apply_weak_labels(spark, df):
    """Apply weak labeling UDF and filter by confidence."""
    print("\n[2/4] Applying weak labeling rules...")

    label_udf = create_weak_labeling_udf()

    df_labeled = df.withColumn(
        "labels", label_udf(F.col("comment_text"))
    )

    df_labeled = df_labeled.select(
        "comment_sk",
        "comment_text",
        "comment_level",
        "comment_likes",
        F.col("labels.sentiment_label").alias("sentiment_label"),
        F.col("labels.sentiment_confidence").alias("sentiment_confidence"),
        F.col("labels.aspects").alias("aspects"),
        F.col("labels.intent_label").alias("intent_label"),
        F.col("labels.intent_confidence").alias("intent_confidence"),
    )

    total = df_labeled.count()
    print(f"  Labeled {total:,} comments")

    # Distribution
    print("\n  Sentiment distribution:")
    df_labeled.groupBy("sentiment_label").agg(
        F.count("*").alias("count"),
        F.round(F.avg("sentiment_confidence"), 3).alias("avg_confidence"),
    ).orderBy("sentiment_label").show()

    print("  Intent distribution:")
    df_labeled.groupBy("intent_label").agg(
        F.count("*").alias("count"),
        F.round(F.avg("intent_confidence"), 3).alias("avg_confidence"),
    ).orderBy("intent_label").show()

    return df_labeled


def filter_confident_samples(df):
    """Keep only samples with high enough confidence for training."""
    print("\n[3/4] Filtering confident samples...")

    min_sentiment_conf = 0.6
    min_intent_conf = 0.5

    df_filtered = df.filter(
        (F.col("sentiment_confidence") >= min_sentiment_conf) |
        (F.col("intent_confidence") >= min_intent_conf)
    )

    kept = df_filtered.count()
    total = df.count()
    print(f"  Kept {kept:,} / {total:,} ({kept/total*100:.1f}%) confident samples")

    return df_filtered


def export_labeled_data(df):
    """Export to Parquet for PhoBERT fine-tuning."""
    print("\n[4/4] Exporting labeled data...")

    df.coalesce(4).write.mode("overwrite").parquet(LABELED_PARQUET_PATH)
    print(f"  Exported to {LABELED_PARQUET_PATH}")

    return df.count()


def main():
    print("=" * 70)
    print("NLP Pipeline Step 1: Weak Labeling")
    print("=" * 70)
    print(f"Start: {datetime.now()}")

    spark = create_spark_session()

    try:
        df = load_comments(spark)
        df_labeled = apply_weak_labels(spark, df)
        df_filtered = filter_confident_samples(df_labeled)
        count = export_labeled_data(df_filtered)

        print(f"\nCompleted: {count:,} labeled samples exported")
        print(f"  Path: {LABELED_PARQUET_PATH}")
        print(f"  Next: Run train_phobert.py to fine-tune PhoBERT")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
