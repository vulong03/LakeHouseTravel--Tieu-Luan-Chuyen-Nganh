"""
NLP Pipeline Step 1: Weak Labeling
====================================
Auto-label > 800K TikTok comments using rule-based signals:
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
    POSITIVE_KEYWORDS, NEGATIVE_KEYWORDS,
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
    using clause-level keyword + emoji rules. Returns confidence scores.
    """

    output_schema = StructType([
        StructField("sentiment_label", StringType(), True),
        StructField("sentiment_confidence", FloatType(), True),
        StructField("aspects", StringType(), True),
        
        # New aspect-sentiment pairs
        StructField("aspect_scenery_pos", FloatType(), True),
        StructField("aspect_scenery_neg", FloatType(), True),
        StructField("aspect_food_pos", FloatType(), True),
        StructField("aspect_food_neg", FloatType(), True),
        StructField("aspect_price_pos", FloatType(), True),
        StructField("aspect_price_neg", FloatType(), True),
        StructField("aspect_service_pos", FloatType(), True),
        StructField("aspect_service_neg", FloatType(), True),
        StructField("aspect_transport_pos", FloatType(), True),
        StructField("aspect_transport_neg", FloatType(), True),
        StructField("aspect_accommodation_pos", FloatType(), True),
        StructField("aspect_accommodation_neg", FloatType(), True),
    ])

    @pandas_udf(output_schema)
    def weak_label_batch(texts: pd.Series) -> pd.DataFrame:
        import re as _re
        import emoji as _emoji_lib
        
        # Move underthesea import outside loop for performance
        try:
            from underthesea import sentiment as _uts_sentiment
            HAS_UTS = True
        except Exception:
            HAS_UTS = False

        def match_keyword(kw, text_str):
            if len(kw) <= 2:
                VIETNAMESE_LETTERS = 'a-zA-ZđĐêÊôÔâÂăĂưƯơƠáàảãạấầẩẫậắằẳẵặéèẻẽẹếềểễệíìỉĩịóòỏõọốồổỗộớờởỡợúùủũụứừửữựýỳỷỹỵ'
                pattern = r'(?<![' + VIETNAMESE_LETTERS + r'])' + _re.escape(kw) + r'(?![' + VIETNAMESE_LETTERS + r'])'
                return bool(_re.search(pattern, text_str))
            else:
                return kw in text_str

        results = []

        for text in texts:
            # Default empty output dictionary
            out = {
                "sentiment_label": "neutral",
                "sentiment_confidence": 0.3,
                "aspects": "",
                "aspect_scenery_pos": 0.0,
                "aspect_scenery_neg": 0.0,
                "aspect_food_pos": 0.0,
                "aspect_food_neg": 0.0,
                "aspect_price_pos": 0.0,
                "aspect_price_neg": 0.0,
                "aspect_service_pos": 0.0,
                "aspect_service_neg": 0.0,
                "aspect_transport_pos": 0.0,
                "aspect_transport_neg": 0.0,
                "aspect_accommodation_pos": 0.0,
                "aspect_accommodation_neg": 0.0,
            }

            if not text or len(str(text).strip()) < 3:
                results.append(out)
                continue

            text_str = str(text)
            text_lower = text_str.lower()
            text_clean = _re.sub(r'http\S+|www\S+|@\w+', '', text_lower)
            text_clean = ' '.join(text_clean.split())

            # ========== GLOBAL SENTIMENT ==========
            pos_signals = 0
            neg_signals = 0
            total_signals = 0

            # Signal 1: Keywords
            pos_kw_count = sum(1 for kw in POSITIVE_KEYWORDS if match_keyword(kw, text_clean))
            neg_kw_count = sum(1 for kw in NEGATIVE_KEYWORDS if match_keyword(kw, text_clean))
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
            if HAS_UTS:
                try:
                    uts_result = _uts_sentiment(text_clean)
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
            else:
                total_signals += 1

            # Determine global sentiment
            if total_signals == 0:
                global_sent = "neutral"
                sent_conf = 0.4
            elif pos_signals > neg_signals:
                global_sent = "positive"
                signal_strength = min(total_signals / 5.0, 1.0)
                ratio = pos_signals / max(total_signals, 1)
                sent_conf = min(0.95, 0.4 + ratio * 0.55 * signal_strength)
            elif neg_signals > pos_signals:
                global_sent = "negative"
                signal_strength = min(total_signals / 5.0, 1.0)
                ratio = neg_signals / max(total_signals, 1)
                sent_conf = min(0.95, 0.4 + ratio * 0.55 * signal_strength)
            else:
                global_sent = "neutral"
                sent_conf = 0.5

            out["sentiment_label"] = global_sent
            out["sentiment_confidence"] = float(sent_conf)

            # ========== CLAUSE-LEVEL ASPECT & SENTIMENT ==========
            # Split by punctuation and contrastive conjunctions
            clauses = _re.split(r'[.,!?;\n]|\b(?:nhưng|tuy nhiên|bù lại|nhưng mà|song|trong khi)\b', text_lower)
            
            detected_aspects = set()
            aspect_pos_signals = {a: 0 for a in ASPECT_LABELS}
            aspect_neg_signals = {a: 0 for a in ASPECT_LABELS}

            for clause in clauses:
                clause = clause.strip()
                if len(clause) < 3:
                    continue
                
                # Detect aspects in clause
                clause_aspects = []
                for aspect, keywords in ASPECT_KEYWORD_MAP.items():
                    matches = sum(1 for kw in keywords if match_keyword(kw, clause))
                    if matches >= 2:
                        clause_aspects.append(aspect)
                    elif matches == 1 and len(clause.split()) <= 15:
                        clause_aspects.append(aspect)
                
                if not clause_aspects:
                    continue
                
                for a in clause_aspects:
                    detected_aspects.add(a)

                # Local sentiment signals in clause
                c_pos = sum(1 for kw in POSITIVE_KEYWORDS if match_keyword(kw, clause))
                c_neg = sum(1 for kw in NEGATIVE_KEYWORDS if match_keyword(kw, clause))
                c_emojis = [c for c in clause if c in _emoji_lib.EMOJI_DATA]
                c_pos += sum(1 for e in c_emojis if e in POSITIVE_EMOJIS)
                c_neg += sum(1 for e in c_emojis if e in NEGATIVE_EMOJIS)
                
                if clause.count('!') >= 2:
                    c_pos += 1

                for a in clause_aspects:
                    if c_pos > c_neg:
                        aspect_pos_signals[a] += 1
                    elif c_neg > c_pos:
                        aspect_neg_signals[a] += 1

            # Populate ABSA columns based on clause results and global fallback
            for a in detected_aspects:
                pos_sig = aspect_pos_signals[a]
                neg_sig = aspect_neg_signals[a]

                if pos_sig > neg_sig:
                    out[f"aspect_{a}_pos"] = 1.0
                elif neg_sig > pos_sig:
                    out[f"aspect_{a}_neg"] = 1.0
                else:
                    # Fallback to global sentiment if clause sentiment is tied/neutral
                    if global_sent == "positive":
                        out[f"aspect_{a}_pos"] = 1.0
                    elif global_sent == "negative":
                        out[f"aspect_{a}_neg"] = 1.0

            out["aspects"] = ",".join(detected_aspects) if detected_aspects else ""

            results.append(out)

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
        F.col("labels.aspect_scenery_pos").alias("aspect_scenery_pos"),
        F.col("labels.aspect_scenery_neg").alias("aspect_scenery_neg"),
        F.col("labels.aspect_food_pos").alias("aspect_food_pos"),
        F.col("labels.aspect_food_neg").alias("aspect_food_neg"),
        F.col("labels.aspect_price_pos").alias("aspect_price_pos"),
        F.col("labels.aspect_price_neg").alias("aspect_price_neg"),
        F.col("labels.aspect_service_pos").alias("aspect_service_pos"),
        F.col("labels.aspect_service_neg").alias("aspect_service_neg"),
        F.col("labels.aspect_transport_pos").alias("aspect_transport_pos"),
        F.col("labels.aspect_transport_neg").alias("aspect_transport_neg"),
        F.col("labels.aspect_accommodation_pos").alias("aspect_accommodation_pos"),
        F.col("labels.aspect_accommodation_neg").alias("aspect_accommodation_neg"),
    )

    total = df_labeled.count()
    print(f"  Labeled {total:,} comments")

    # Distribution
    print("\n  Sentiment distribution:")
    df_labeled.groupBy("sentiment_label").agg(
        F.count("*").alias("count"),
        F.round(F.avg("sentiment_confidence"), 3).alias("avg_confidence"),
    ).orderBy("sentiment_label").show()

    return df_labeled


def filter_confident_samples(df):
    """Keep only samples with high enough confidence for training."""
    print("\n[3/4] Filtering confident samples...")

    min_sentiment_conf = 0.6

    # Filter per-task independently using flags (no intent task)
    # Lower negative threshold to 0.5 due to class scarcity
    df_annotated = df.withColumn(
        "use_for_sentiment",
        F.when(F.col("sentiment_label") == "negative", F.col("sentiment_confidence") >= 0.5)
        .otherwise(F.col("sentiment_confidence") >= min_sentiment_conf)
    )

    df_high_conf = df_annotated.filter(
        F.col("use_for_sentiment")
    ).withColumn("is_weak_label", F.lit(False))

    # FIX ISSUE-04: Preserve neutral samples with weak labels
    df_neutral_weak = df_annotated.filter(
        (F.col("sentiment_label") == "neutral") &
        (F.col("sentiment_confidence") >= 0.35) &
        (F.col("sentiment_confidence") < min_sentiment_conf)
    ).withColumn("use_for_sentiment", F.lit(True)).withColumn("is_weak_label", F.lit(True)).limit(30_000)

    df_filtered = df_high_conf.unionByName(df_neutral_weak)

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
