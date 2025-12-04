"""
Gold Layer - Dimension Comment Job
===================================
Builds `gold.dim_comment` from Silver layer tables.

Key responsibilities:
  1. Read from silver.silver.tiktok_post_comments (main source)
  2. Join with dim_post (by post_url)
  3. Join with dim_date (by comment_date)
  4. Transform and calculate row_checksum
  5. MERGE into Iceberg table (incremental load)
  6. Log execution to PostgreSQL via GoldJobLogger
"""

import sys
from datetime import datetime

sys.path.append("/opt/spark/jobs")

from config import (  # type: ignore
    SOURCE_COMMENTS_TABLE,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    BUSINESS_KEY,
    BUSINESS_COLUMNS,
    DIM_POST_TABLE,
    DIM_DATE_TABLE,
)
from utils.spark_session import get_spark_session  # type: ignore
from utils.iceberg_utils import create_iceberg_table_if_not_exists  # type: ignore
from utils.gold_job_logger import get_gold_logger  # type: ignore
from utils.merge_utils import calculate_row_checksum, merge_into_gold_dim, print_merge_stats  # type: ignore

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    LongType,
    StringType,
    BooleanType,
    TimestampType,
)
from pyspark.sql.window import Window


def create_gold_database(spark):
    """Create Gold database if not exists"""
    print("\n[*] Ensuring Gold database exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
    print(f"[OK] Database {GOLD_CATALOG}.{GOLD_DATABASE} ready")


def create_dim_comment_table(spark):
    """Create dim_comment Iceberg table if not exists"""
    print("\n[*] Ensuring dim_comment Iceberg table exists...")
    schema = StructType(
        [
            StructField("comment_sk", LongType(), False),  # Surrogate key
            StructField("post_sk", LongType(), True),  # FK to dim_post
            StructField("comment_date_sk", IntegerType(), True),  # FK to dim_date
            StructField("post_url_nk", StringType(), False),  # Natural key
            StructField("stt", IntegerType(), True),  # Sequence number
            StructField("commenter_tag", StringType(), True),
            StructField("commenter_name", StringType(), True),
            StructField("commenter_url", StringType(), True),
            StructField("comment_text", StringType(), False),
            StructField("comment_level", IntegerType(), True),  # 1 or 2
            StructField("replied_to_tag_name", StringType(), True),
            StructField("row_checksum", StringType(), False),  # For MERGE
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("is_active", BooleanType(), False),
        ]
    )

    create_iceberg_table_if_not_exists(
        spark=spark,
        database=GOLD_DATABASE,
        table_name=GOLD_TABLE,
        schema=schema,
        partition_by=[],  # Dimension table, not partitioned
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
        catalog=GOLD_CATALOG,
    )
    print(f"[OK] Table {GOLD_TABLE_FULL} ready")


def load_source_data(spark) -> DataFrame:
    """Load comment data from Silver layer"""
    print(f"\nLoading source data from: {SOURCE_COMMENTS_TABLE}")
    df_comments = spark.table(SOURCE_COMMENTS_TABLE)

    comments_count = df_comments.count()
    print(f"Loaded {comments_count:,} records from {SOURCE_COMMENTS_TABLE}")

    return df_comments


def join_with_dim_post(spark, df: DataFrame) -> DataFrame:
    """Join with dim_post to get post_sk"""
    print("\nJoining with dim_post...")
    
    # Load dim_post
    df_post = spark.table(DIM_POST_TABLE).select(
        "post_sk",
        "post_url"
    )
    
    # Left join with dim_post
    df_joined = df.join(
        df_post,
        df["post_url"] == df_post["post_url"],
        how="left"
    ).drop(df_post["post_url"])  # Keep only comments.post_url
    
    missing_count = df_joined.filter(F.col("post_sk").isNull()).count()
    if missing_count > 0:
        print(f"Warning: {missing_count} comments have no matching post in dim_post")
    
    return df_joined


def join_with_dim_date(spark, df: DataFrame) -> DataFrame:
    """Join with dim_date to get comment_date_sk"""
    print("\nJoining with dim_date...")
    
    # Load dim_date
    df_date = spark.table(DIM_DATE_TABLE).select(
        "date_sk",
        "full_date"
    )
    
    # Left join with dim_date
    df_joined = df.join(
        df_date,
        df["comment_date"] == df_date["full_date"],
        how="left"
    ).withColumnRenamed("date_sk", "comment_date_sk") \
     .drop("full_date")
    
    missing_count = df_joined.filter(
        F.col("comment_date").isNotNull() & F.col("comment_date_sk").isNull()
    ).count()
    if missing_count > 0:
        print(f"Warning: {missing_count} comments have comment_date but no matching date in dim_date")
    
    return df_joined


def transform_level_comment(df: DataFrame) -> DataFrame:
    """Convert level_comment from string 'Yes'/'No' to integer 1/2"""
    print("\nConverting level_comment to integer...")
    
    df = df.withColumn(
        "comment_level",
        F.when(
            F.lower(F.trim(F.col("level_comment"))) == "yes",
            2
        ).when(
            F.lower(F.trim(F.col("level_comment"))) == "no",
            1
        ).otherwise(F.lit(None).cast(IntegerType()))
    )
    
    return df

def deduplicate_by_composite_key(df: DataFrame) -> DataFrame:
    """
    Deduplicate by (post_url, stt), keeping the latest record based on ingestion_timestamp
    
    This ensures each comment appears only once in the dimension table.
    """
    print("\nDeduplicating by (post_url, stt) composite key...")
    
    original_count = df.count()
    print(f"Original records: {original_count:,}")
    
    # Window function to rank by (post_url, stt), ordered by latest ingestion_timestamp
    window_spec = Window.partitionBy("post_url", "stt").orderBy(
        F.col("ingestion_timestamp").desc()
    )
    
    df = df.withColumn("_rank", F.row_number().over(window_spec)) \
           .filter(F.col("_rank") == 1) \
           .drop("_rank")
    
    final_count = df.count()
    duplicates_removed = original_count - final_count
    print(f"Removed duplicates: {duplicates_removed:,} records")
    print(f"Final unique comments: {final_count:,}")
    
    return df


def transform_to_dimension(df: DataFrame) -> DataFrame:
    """
    Transform to dimension table format
    
    Transformations:
    1. Rename columns to dim naming convention
    2. Add metadata columns (created_at, updated_at, is_active)
    3. Calculate row_checksum for MERGE
    4. Generate surrogate key (comment_sk)
    """
    print("\nTransforming to dimension format...")
    
    current_timestamp = F.current_timestamp()
    
    # Step 1: Select and rename columns
    df = df.select(
        "post_sk",
        "comment_date_sk",
        F.col("post_url").alias("post_url_nk"),  # Natural key
        "stt",
        F.col("tag_ten").alias("commenter_tag"),
        F.col("ten").alias("commenter_name"),
        F.col("url").alias("commenter_url"),
        F.col("comment").alias("comment_text"),
        "comment_level",
        "replied_to_tag_name"
    )
    
    # Step 2: Add metadata columns
    print("Adding SCD metadata...")
    df = df.withColumn("created_at", current_timestamp)
    df = df.withColumn("updated_at", current_timestamp)
    df = df.withColumn("is_active", F.lit(True))
    
    # Step 3: Calculate row_checksum for MERGE
    print("Calculating row_checksum...")
    df = calculate_row_checksum(df, BUSINESS_COLUMNS)
    
    # Step 4: Generate surrogate key (comment_sk)
    print("Generating surrogate keys...")
    window_spec = Window.orderBy("post_url_nk", "stt")
    df = df.withColumn("comment_sk", F.row_number().over(window_spec))
    
    # Reorder columns to match schema
    df = df.select(
        "comment_sk",
        "post_sk",
        "comment_date_sk",
        "post_url_nk",
        "stt",
        "commenter_tag",
        "commenter_name",
        "commenter_url",
        "comment_text",
        "comment_level",
        "replied_to_tag_name",
        "row_checksum",
        "created_at",
        "updated_at",
        "is_active"
    )
    
    print("Transformation complete")
    df.printSchema()
    df.show(10, truncate=False)
    
    return df


def merge_to_gold_table(spark, df: DataFrame, record_count: int):
    """MERGE dimension data into Gold Iceberg table (incremental load)"""
    print(f"\nMERGE into Gold table: {GOLD_TABLE_FULL}")
    print(f"Records to merge: {record_count:,}")
    print(f"Business Keys: {', '.join(BUSINESS_KEY)}")
    print(f"Strategy: INSERT new, UPDATE changed, SKIP unchanged")
    
    # All columns (exclude comment_sk for INSERT - will be auto-generated)
    all_columns = [
        "post_sk",
        "comment_date_sk",
        "post_url_nk",
        "stt",
        "commenter_tag",
        "commenter_name",
        "commenter_url",
        "comment_text",
        "comment_level",
        "replied_to_tag_name",
        "row_checksum",
        "created_at",
        "updated_at",
        "is_active"
    ]
    
    stats = merge_into_gold_dim(
        spark=spark,
        new_data_df=df,
        target_table=GOLD_TABLE_FULL,
        business_keys=BUSINESS_KEY,
        all_columns=all_columns
    )
    
    print_merge_stats(stats)
    
    return stats


def validate_results(spark):
    """Validate the created dimension table"""
    print("\nValidating results...")
    
    df = spark.table(GOLD_TABLE_FULL)
    
    total_count = df.count()
    with_post_sk = df.filter(F.col("post_sk").isNotNull()).count()
    with_date_sk = df.filter(F.col("comment_date_sk").isNotNull()).count()
    level_1 = df.filter(F.col("comment_level") == 1).count()
    level_2 = df.filter(F.col("comment_level") == 2).count()
    
    print("\nValidation Summary:")
    print(f"Total comments: {total_count:,}")
    print(f"Comments with post_sk: {with_post_sk:,} ({with_post_sk/total_count*100:.2f}%)")
    print(f"Comments with comment_date_sk: {with_date_sk:,} ({with_date_sk/total_count*100:.2f}%)")
    print(f"Level 1 comments (No): {level_1:,} ({level_1/total_count*100:.2f}%)")
    print(f"Level 2 comments (Yes): {level_2:,} ({level_2/total_count*100:.2f}%)")
    
    print("\nSample comments:")
    df.select("comment_sk", "post_url_nk", "stt", "commenter_name", "comment_text", "comment_level") \
        .orderBy("comment_sk") \
        .show(20, truncate=True)


def main():
    """Main execution flow for dim_comment job"""
    print("=" * 80)
    print("Gold Layer - Dimension Comment Job")
    print("=" * 80)
    
    spark = get_spark_session(app_name="Gold_Dim_Comment")
    logger = get_gold_logger(spark)
    
    job_start_time = datetime.now()
    record_count = 0
    
    try:
        # Step 1: Create Gold database
        create_gold_database(spark)
        
        # Step 2: Create dimension table
        create_dim_comment_table(spark)
        
        # Step 3: Load source data
        df_comments = load_source_data(spark)

        # Step 4: Join with dim_post
        df_joined = join_with_dim_post(spark, df_comments)
        
        # Step 5: Join with dim_date
        df_joined = join_with_dim_date(spark, df_joined)
        
        # Step 6: Transform level_comment
        df_joined = transform_level_comment(df_joined)
        
        # Step 7: Deduplicate by composite key (post_url, stt)
        df_joined = deduplicate_by_composite_key(df_joined)
        
        # Step 8: Transform to dimension format
        dim_df = transform_to_dimension(df_joined)
        record_count = dim_df.count()
        
        # Step 9: MERGE to Gold table (incremental load)
        stats = merge_to_gold_table(spark, dim_df, record_count)
        
        # Step 10: Validate results
        validate_results(spark)
        
        # Log success
        job_end_time = datetime.now()
        execution_time = (job_end_time - job_start_time).total_seconds()
        
        logger.log_job_success(
            source_path=SOURCE_COMMENTS_TABLE,
            table_name=GOLD_TABLE_FULL,
            records_processed=stats["total_processed"],
            job_details={
                "job_type": "dimension",
                "execution_time_seconds": execution_time,
                "source_type": "silver_table",
                "merge_strategy": "incremental",
                "records_inserted": stats["inserted"],
                "records_updated": stats["updated"],
                "records_skipped": stats["skipped"],
                "comments_with_post_sk": dim_df.filter(F.col("post_sk").isNotNull()).count(),
                "comments_with_date_sk": dim_df.filter(F.col("comment_date_sk").isNotNull()).count(),
            }
        )
        
        print("\n" + "=" * 80)
        print("dim_comment job completed successfully!")
        print(f"Records processed: {stats['total_processed']:,}")
        print(f"Inserted: {stats['inserted']:,}")
        print(f"Updated: {stats['updated']:,}")
        print(f"Skipped: {stats['skipped']:,}")
        print(f"Execution time: {execution_time:.2f}s")
        print("=" * 80)
        
    except Exception as e:
        logger.log_job_failure(
            source_path=SOURCE_COMMENTS_TABLE,
            table_name=GOLD_TABLE_FULL,
            error_message=str(e)
        )
        
        print("\n" + "=" * 80)
        print(f"dim_comment job failed: {e}")
        print("=" * 80)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

