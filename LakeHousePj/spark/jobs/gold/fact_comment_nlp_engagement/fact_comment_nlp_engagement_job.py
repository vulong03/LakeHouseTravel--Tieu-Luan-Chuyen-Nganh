"""
Gold Layer - Fact: Comment NLP Engagement
==========================================
ML Feature Engineering job that extracts NLP features from TikTok comments
and combines them with comment/post engagement metrics.

Key responsibilities:
  1. Read from gold.dim_comment (comment text + metadata)
  2. Join with silver tables to get engagement metrics (likes, saves, shares)
  3. Extract 8 NLP features using Pandas UDF:
     - word_count, unique_word_ratio, exclamation_count
     - sentiment_score, sentiment_label (underthesea)
     - emoji_count, positive_emoji_count, negative_emoji_count
  4. Denormalize post metrics (post_likes, post_comments_count, etc.)
  5. Calculate row_checksum for incremental load
  6. MERGE into Iceberg table (skip unchanged, update changed, insert new)
  7. Log execution to PostgreSQL via GoldJobLogger

Output: Comment-level fact table with NLP + engagement features
Grain: 1 row = 1 comment
Usage: Aggregate to province-month for ML training
"""

import sys
import re
from datetime import datetime
from typing import Tuple, Dict, Any

sys.path.append("/opt/spark/jobs")

from config import (  # type: ignore
    DIM_COMMENT_TABLE,
    DIM_POST_TABLE,
    SILVER_COMMENTS_TABLE,
    SILVER_METADATA_TABLE,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    BUSINESS_KEY,
    BUSINESS_COLUMNS,
    SOURCE_DESCRIPTION,
    MAX_TEXT_LENGTH,
    DEFAULT_NLP_VALUES,
    POSITIVE_EMOJIS,
    NEGATIVE_EMOJIS,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, col, current_timestamp
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    TimestampType,
)

from utils.spark_session import get_spark_session  # type: ignore
from utils.iceberg_utils import create_iceberg_table_if_not_exists  # type: ignore
from utils.gold_job_logger import get_gold_logger  # type: ignore
from utils.merge_utils import calculate_row_checksum, merge_into_gold_dim, print_merge_stats  # type: ignore

import pandas as pd


def create_gold_database(spark: SparkSession) -> None:
    """Create Gold database if not exists"""
    print("\n[*] Ensuring Gold database exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
    print(f"[OK] Database {GOLD_CATALOG}.{GOLD_DATABASE} ready")


def create_fact_table(spark: SparkSession) -> None:
    """Create fact_comment_nlp_engagement Iceberg table if not exists"""
    print("\n[*] Ensuring fact_comment_nlp_engagement Iceberg table exists...")
    
    schema = StructType([
        # Keys
        StructField("comment_sk", LongType(), False),
        StructField("post_sk", LongType(), False),
        StructField("province_sk", IntegerType(), False),
        StructField("comment_date_sk", IntegerType(), False),
        
        # NLP Features (8 columns)
        StructField("word_count", LongType(), True),
        StructField("unique_word_ratio", DoubleType(), True),
        StructField("exclamation_count", LongType(), True),
        StructField("sentiment_score", DoubleType(), True),
        StructField("sentiment_label", StringType(), True),
        StructField("emoji_count", LongType(), True),
        StructField("positive_emoji_count", LongType(), True),
        StructField("negative_emoji_count", LongType(), True),
        
        # Comment Metrics (2 columns)
        StructField("comment_likes", LongType(), True),
        StructField("comment_level", LongType(), True),
        
        # Post Metrics - Denormalized (4 columns)
        StructField("post_likes", LongType(), True),
        StructField("post_comments_count", LongType(), True),
        StructField("post_saves", LongType(), True),
        StructField("post_shares", LongType(), True),
        
        # Checksum & Metadata
        StructField("row_checksum", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database=GOLD_DATABASE,
        table_name=GOLD_TABLE,
        schema=schema,
        partition_by=["province_sk"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
        catalog=GOLD_CATALOG,
    )
    print(f"[OK] Table {GOLD_TABLE_FULL} ready")


def load_source_data(spark: SparkSession) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Load comment data from Gold dim_comment and join with Silver tables
    to get engagement metrics.
    """
    print(f"\n📥 Loading source data...")
    print(f"   - {DIM_COMMENT_TABLE}")
    print(f"   - {DIM_POST_TABLE}")
    print(f"   - {SILVER_COMMENTS_TABLE}")
    print(f"   - {SILVER_METADATA_TABLE}")
    
    # Load and join all required data
    df = spark.sql(f"""
        SELECT 
            c.comment_sk,
            c.post_sk,
            p.province_sk,
            c.comment_date_sk,
            c.comment_text,
            c.comment_level,
            c.post_url_nk,
            c.stt,
            
            -- Comment metrics (from Silver)
            sc.likes as comment_likes,
            
            -- Post metrics (from Silver metadata)
            sm.likes as post_likes,
            sm.comments_count as post_comments_count,
            sm.saves as post_saves,
            sm.shares as post_shares
            
        FROM {DIM_COMMENT_TABLE} c
        INNER JOIN {DIM_POST_TABLE} p 
            ON c.post_sk = p.post_sk
        LEFT JOIN {SILVER_COMMENTS_TABLE} sc 
            ON c.post_url_nk = sc.post_url AND c.stt = sc.stt
        LEFT JOIN {SILVER_METADATA_TABLE} sm
            ON p.post_url = sm.post_url
        WHERE c.comment_text IS NOT NULL 
            AND LENGTH(TRIM(c.comment_text)) > 0
            AND c.is_active = TRUE
            AND p.province_sk IS NOT NULL
            AND c.comment_date_sk IS NOT NULL
    """)
    
    total_count = df.count()
    provinces = df.select("province_sk").distinct().count()
    posts = df.select("post_sk").distinct().count()
    
    print(f"   ✅ Loaded {total_count:,} comments")
    print(f"      - Distinct provinces: {provinces}")
    print(f"      - Distinct posts: {posts}")
    
    stats = {
        "total_comments": total_count,
        "distinct_provinces": provinces,
        "distinct_posts": posts,
    }
    
    return df, stats


def preprocess_text(text: str) -> str:
    """
    Full text cleaning pipeline (Option B)
    - Remove URLs
    - Remove mentions
    - Normalize whitespace
    - Lowercase
    """
    if not text:
        return ""
    
    # Truncate if too long
    if len(text) > MAX_TEXT_LENGTH:
        text = text[:MAX_TEXT_LENGTH]
    
    # Remove URLs
    text = re.sub(r'http\S+|www\S+', '', text)
    
    # Remove mentions (@username)
    text = re.sub(r'@\w+', '', text)
    
    # Normalize whitespace
    text = ' '.join(text.split())
    
    # Lowercase
    text = text.lower()
    
    return text.strip()


def create_nlp_extraction_udf():
    """
    Create Pandas UDF for NLP feature extraction.
    
    This function extracts 8 NLP features from comment text:
    1. word_count - number of tokens
    2. unique_word_ratio - unique tokens / total tokens
    3. exclamation_count - number of '!' characters
    4. sentiment_score - -1.0 (negative) to 1.0 (positive)
    5. sentiment_label - 'positive', 'neutral', 'negative'
    6. emoji_count - total emoji count
    7. positive_emoji_count - count of positive emojis
    8. negative_emoji_count - count of negative emojis
    
    Error handling: If any error occurs, returns default neutral values.
    """
    
    @pandas_udf(StructType([
        StructField("word_count", LongType(), True),
        StructField("unique_word_ratio", DoubleType(), True),
        StructField("exclamation_count", LongType(), True),
        StructField("sentiment_score", DoubleType(), True),
        StructField("sentiment_label", StringType(), True),
        StructField("emoji_count", LongType(), True),
        StructField("positive_emoji_count", LongType(), True),
        StructField("negative_emoji_count", LongType(), True),
    ]))
    def extract_nlp_features(texts: pd.Series) -> pd.DataFrame:
        """
        Extract NLP features from a batch of texts using Pandas UDF.
        Imports must be inside UDF to ensure availability on all workers.
        """
        # Import inside UDF for worker nodes
        from underthesea import sentiment as _sentiment_func
        from underthesea import word_tokenize as _word_tokenize
        import emoji as _emoji_lib
        import re as _re
        import os
        import tempfile
        
        # FIX: Set HOME to writable temp directory for underthesea model download
        if 'HOME' not in os.environ or not os.access(os.environ.get('HOME', ''), os.W_OK):
            temp_home = tempfile.mkdtemp()
            os.environ['HOME'] = temp_home
        
        results = []
        
        for text in texts:
            try:
                # Validate input
                if not text or len(str(text).strip()) == 0:
                    results.append(DEFAULT_NLP_VALUES.copy())
                    continue
                
                text = str(text)
                
                # Truncate if too long
                if len(text) > MAX_TEXT_LENGTH:
                    text = text[:MAX_TEXT_LENGTH]
                
                # Preprocess
                text = _re.sub(r'http\S+|www\S+', '', text)
                text = _re.sub(r'@\w+', '', text)
                text = ' '.join(text.split())
                text_lower = text.lower()
                
                # === Word Statistics (with tokenization) ===
                try:
                    tokens = _word_tokenize(text_lower)
                    word_count = len(tokens)
                    unique_words = len(set(tokens))
                    unique_word_ratio = unique_words / word_count if word_count > 0 else 0.0
                except Exception:
                    # Fallback to simple split if tokenization fails
                    words = text_lower.split()
                    word_count = len(words)
                    unique_words = len(set(words))
                    unique_word_ratio = unique_words / word_count if word_count > 0 else 0.0
                
                exclamation_count = text.count('!')
                
                # === Sentiment Analysis ===
                try:
                    # Check if text is only emojis
                    text_without_spaces = text.replace(' ', '')
                    if all(c in _emoji_lib.EMOJI_DATA for c in text_without_spaces):
                        # Only emojis → neutral sentiment (6c - Option 1)
                        sentiment_label = 'neutral'
                        sentiment_score = 0.0
                    else:
                        # Call underthesea sentiment()
                        # Returns: 'positive', 'negative', or None
                        sentiment_result = _sentiment_func(text_lower)
                        
                        # Handle sentiment result (can be None)
                        if sentiment_result == 'positive':
                            sentiment_label = 'positive'
                            sentiment_score = 1.0
                        elif sentiment_result == 'negative':
                            sentiment_label = 'negative'
                            sentiment_score = -1.0
                        else:
                            # None or any other value → neutral
                            sentiment_label = 'neutral'
                            sentiment_score = 0.0
                except Exception as e:
                    # Error in sentiment analysis → neutral (6a - Option 1)
                    sentiment_label = 'neutral'
                    sentiment_score = 0.0
                
                # === Emoji Analysis ===
                emojis = [c for c in text if c in _emoji_lib.EMOJI_DATA]
                emoji_count = len(emojis)
                positive_emoji_count = sum(1 for e in emojis if e in POSITIVE_EMOJIS)
                negative_emoji_count = sum(1 for e in emojis if e in NEGATIVE_EMOJIS)
                
                results.append({
                    'word_count': word_count,
                    'unique_word_ratio': round(unique_word_ratio, 4),
                    'exclamation_count': exclamation_count,
                    'sentiment_score': sentiment_score,
                    'sentiment_label': sentiment_label,
                    'emoji_count': emoji_count,
                    'positive_emoji_count': positive_emoji_count,
                    'negative_emoji_count': negative_emoji_count,
                })
                
            except Exception as e:
                # Any unexpected error → return default values (6a - Option 1)
                results.append(DEFAULT_NLP_VALUES.copy())
        
        return pd.DataFrame(results)
    
    return extract_nlp_features


def transform_to_fact(df: DataFrame) -> Tuple[DataFrame, int]:
    """
    Apply NLP extraction UDF and prepare final fact table structure.
    """
    print("\n🔄 Transforming data to fact table format...")
    print("   - Applying NLP extraction UDF (this may take a while)...")
    
    # Create and apply NLP UDF
    extract_nlp_udf = create_nlp_extraction_udf()
    
    df_with_nlp = df.withColumn(
        "nlp_features",
        extract_nlp_udf(col("comment_text"))
    )
    
    # Expand NLP features struct
    fact_df = df_with_nlp.select(
        col("comment_sk"),
        col("post_sk"),
        col("province_sk"),
        col("comment_date_sk"),
        
        # NLP features
        col("nlp_features.word_count"),
        col("nlp_features.unique_word_ratio"),
        col("nlp_features.exclamation_count"),
        col("nlp_features.sentiment_score"),
        col("nlp_features.sentiment_label"),
        col("nlp_features.emoji_count"),
        col("nlp_features.positive_emoji_count"),
        col("nlp_features.negative_emoji_count"),
        
        # Comment metrics
        col("comment_likes"),
        col("comment_level"),
        
        # Post metrics
        col("post_likes"),
        col("post_comments_count"),
        col("post_saves"),
        col("post_shares"),
    )
    
    # Add metadata
    print("   - Adding metadata columns...")
    fact_df = fact_df \
        .withColumn("created_at", current_timestamp()) \
        .withColumn("updated_at", current_timestamp())
    
    # Calculate row_checksum for MERGE
    print("   - Calculating row_checksum...")
    fact_df = calculate_row_checksum(fact_df, BUSINESS_COLUMNS)
    
    # Reorder columns to match schema
    fact_df = fact_df.select(
        "comment_sk",
        "post_sk",
        "province_sk",
        "comment_date_sk",
        "word_count",
        "unique_word_ratio",
        "exclamation_count",
        "sentiment_score",
        "sentiment_label",
        "emoji_count",
        "positive_emoji_count",
        "negative_emoji_count",
        "comment_likes",
        "comment_level",
        "post_likes",
        "post_comments_count",
        "post_saves",
        "post_shares",
        "row_checksum",
        "created_at",
        "updated_at",
    )
    
    record_count = fact_df.count()
    print(f"   ✅ Transformation complete: {record_count:,} records")
    
    return fact_df, record_count


def merge_to_gold_table(spark: SparkSession, fact_df: DataFrame, record_count: int) -> Dict[str, int]:
    """
    MERGE fact data into Gold Iceberg table (incremental load).
    Strategy: INSERT new, UPDATE changed (by row_checksum), SKIP unchanged.
    """
    print(f"\n💾 MERGE into Gold table: {GOLD_TABLE_FULL}")
    print(f"   📊 Records to merge: {record_count:,}")
    print(f"   🔑 Business Key: {', '.join(BUSINESS_KEY)}")
    print(f"   Strategy: INSERT new, UPDATE changed, SKIP unchanged")
    
    # Get all columns for merge (exclude surrogate key comment_sk if auto-generated)
    all_columns = [col for col in fact_df.columns if col != 'comment_sk']
    
    # Use existing merge utility
    stats = merge_into_gold_dim(
        spark=spark,
        new_data_df=fact_df,
        target_table=GOLD_TABLE_FULL,
        business_keys=BUSINESS_KEY,
        all_columns=all_columns,
    )
    
    print_merge_stats(stats)
    
    return stats


def validate_results(spark: SparkSession) -> None:
    """Display sample results for validation"""
    print("\n✅ Validation - Sample records from fact table:")
    
    sample_df = spark.sql(f"""
        SELECT 
            comment_sk,
            province_sk,
            word_count,
            sentiment_label,
            sentiment_score,
            emoji_count,
            comment_likes,
            post_likes
        FROM {GOLD_TABLE_FULL}
        ORDER BY comment_sk DESC
        LIMIT 10
    """)
    
    sample_df.show(10, truncate=False)
    
    # Show sentiment distribution
    print("\n📊 Sentiment Distribution:")
    spark.sql(f"""
        SELECT 
            sentiment_label,
            COUNT(*) as count,
            ROUND(AVG(sentiment_score), 3) as avg_score,
            ROUND(AVG(word_count), 1) as avg_word_count,
            ROUND(AVG(emoji_count), 1) as avg_emoji_count
        FROM {GOLD_TABLE_FULL}
        GROUP BY sentiment_label
        ORDER BY count DESC
    """).show()


def main():
    """Main execution function"""
    spark = get_spark_session("Gold_Fact_Comment_NLP_Engagement")
    logger = get_gold_logger(spark)
    job_start_time = datetime.now()
    
    try:
        print("\n" + "=" * 80)
        print("🚀 Starting fact_comment_nlp_engagement job")
        print("=" * 80)
        
        # Step 1: Create database and table
        create_gold_database(spark)
        create_fact_table(spark)
        
        # Step 2: Load source data
        source_df, source_stats = load_source_data(spark)
        
        # Step 3: Transform and extract NLP features
        fact_df, record_count = transform_to_fact(source_df)
        
        # Step 4: MERGE to Gold table
        merge_stats = merge_to_gold_table(spark, fact_df, record_count)
        
        # Step 5: Validate results
        validate_results(spark)
        
        # Log success
        job_end_time = datetime.now()
        execution_time = (job_end_time - job_start_time).total_seconds()
        
        logger.log_job_success(
            source_path=SOURCE_DESCRIPTION,
            table_name=GOLD_TABLE_FULL,
            records_processed=merge_stats["total_processed"],
            job_details={
                "job_type": "fact_ml",
                "execution_time_seconds": execution_time,
                "source_type": "gold_dim + silver_tables",
                "merge_strategy": "incremental",
                "records_inserted": merge_stats["inserted"],
                "records_updated": merge_stats["updated"],
                "records_skipped": merge_stats["skipped"],
                "distinct_provinces": source_stats["distinct_provinces"],
                "distinct_posts": source_stats["distinct_posts"],
                "nlp_features_extracted": 8,
            }
        )
        
        print("\n" + "=" * 80)
        print("✅ fact_comment_nlp_engagement job completed successfully!")
        print(f"   Records processed: {merge_stats['total_processed']:,}")
        print(f"   - Inserted: {merge_stats['inserted']:,}")
        print(f"   - Updated: {merge_stats['updated']:,}")
        print(f"   - Skipped: {merge_stats['skipped']:,}")
        print(f"   Execution time: {execution_time:.2f}s ({execution_time/60:.1f} minutes)")
        print("=" * 80)
        
    except Exception as exc:
        logger.log_job_failure(
            source_path=SOURCE_DESCRIPTION,
            table_name=GOLD_TABLE_FULL,
            error_message=str(exc)
        )
        
        print("\n" + "=" * 80)
        print(f"❌ fact_comment_nlp_engagement job failed: {exc}")
        print("=" * 80)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
