"""
Gold Layer - Dimension Author Job
==================================
Builds `gold.dim_author` from Silver layer `tiktok_post_metadata`.

Key responsibilities:
  1. Read from silver.silver.tiktok_post_metadata
  2. Validate and normalize author_tag (NOT NULL, trim, remove @ prefix)
  3. Normalize author_name and author_url (trim only)
  4. Deduplicate by author_tag (take latest based on crawl_time)
  5. Generate surrogate key + metadata + write to Iceberg table
  6. Log execution to PostgreSQL via GoldJobLogger
"""

import sys
from datetime import datetime

sys.path.append("/opt/spark/jobs")

from config import (  # type: ignore
    SOURCE_SILVER_TABLE,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    BUSINESS_KEY,
)
from utils.spark_session import get_spark_session  # type: ignore
from utils.iceberg_utils import create_iceberg_table_if_not_exists  # type: ignore
from utils.gold_job_logger import get_gold_logger  # type: ignore

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
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


def create_dim_author_table(spark):
    """Create dim_author Iceberg table if not exists"""
    print("\n[*] Ensuring dim_author Iceberg table exists...")
    schema = StructType(
        [
            StructField("author_sk", IntegerType(), False),
            StructField("author_tag", StringType(), False),  # Business key (NOT NULL)
            StructField("author_name", StringType(), True),  # Nullable
            StructField("author_url", StringType(), True),   # Nullable
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
        partition_by=[],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy",
        },
        catalog=GOLD_CATALOG,
    )
    print(f"[OK] Table {GOLD_TABLE_FULL} ready")


def load_source_data(spark) -> DataFrame:
    """Load author data from Silver layer"""
    print(f"\nLoading source data from: {SOURCE_SILVER_TABLE}")
    df = spark.table(SOURCE_SILVER_TABLE)
    
    original_count = df.count()
    print(f"Loaded {original_count:,} records from Silver")
    
    return df


def validate_and_normalize_author_data(df: DataFrame) -> DataFrame:
    """
    Validate and normalize author data with simplified rules:
    
    Validation Rules:
    1. author_tag: NOT NULL, trim, remove @ prefix
    2. author_name: trim only (nullable OK)
    3. author_url: trim only (nullable OK)
    4. Deduplicate by author_tag (take latest based on crawl_time)
    """
    print("\nValidating and normalizing author data...")
    
    original_count = df.count()
    print(f"Original records: {original_count:,}")
    
    # Step 1: Filter NULL/empty author_tag
    print("Step 1: Filtering NULL/empty author_tag...")
    df = df.filter(
        F.col("author_tag").isNotNull() & 
        (F.trim(F.col("author_tag")) != "")
    )
    after_filter_count = df.count()
    filtered_out = original_count - after_filter_count
    print(f"Filtered out: {filtered_out:,} records ({filtered_out/original_count*100:.2f}%)")
    
    # Step 2: Normalize author_tag (trim + remove @ prefix)
    print("Step 2: Normalizing author_tag (trim + remove @ prefix)...")
    df = df.withColumn("author_tag", 
        F.regexp_replace(F.trim(F.col("author_tag")), "^@", "")
    )
    
    # Step 3: Normalize author_name (trim only)
    print("Step 3: Normalizing author_name (trim only)...")
    df = df.withColumn("author_name",
        F.when(
            F.col("author").isNotNull(),
            F.trim(F.col("author"))
        ).otherwise(F.lit(None))
    )
    
    # Step 4: Normalize author_url (trim only)
    print("Step 4: Normalizing author_url (trim only)...")
    df = df.withColumn("author_url",
        F.when(
            F.col("author_url").isNotNull(),
            F.trim(F.col("author_url"))
        ).otherwise(F.lit(None))
    )
    
    # Step 5: Handle duplicates (take latest based on crawl_time or ingestion_timestamp)
    print("Step 5: Deduplicating by author_tag (take latest)...")
    window_spec = Window.partitionBy("author_tag").orderBy(
        F.coalesce(F.col("crawl_time"), F.col("ingestion_timestamp")).desc()
    )
    df = df.withColumn("_rank", F.row_number().over(window_spec)) \
           .filter(F.col("_rank") == 1) \
           .drop("_rank")
    
    final_count = df.count()
    duplicates_removed = after_filter_count - final_count
    print(f"Removed duplicates: {duplicates_removed:,} records")
    print(f"Final unique authors: {final_count:,}")
    
    return df


def transform_to_dimension(df: DataFrame) -> DataFrame:
    """
    Transform validated author data to dimension table format
    
    Transformations:
    1. Select only required columns (author_tag, author_name, author_url)
    2. Add metadata columns (created_at, updated_at, is_active)
    3. Generate surrogate key 'author_sk' using row_number
    """
    print("\nTransforming to dimension format...")
    
    current_timestamp = F.current_timestamp()
    
    # Step 1: Select required columns
    df = df.select(
        "author_tag",
        "author_name",
        "author_url"
    )
    
    # Step 2: Add metadata columns
    print("Adding SCD metadata...")
    df = df.withColumn("created_at", current_timestamp)
    df = df.withColumn("updated_at", current_timestamp)
    df = df.withColumn("is_active", F.lit(True))
    
    # Step 3: Generate surrogate key
    print("Generating surrogate keys...")
    window_spec = Window.orderBy("author_tag")
    df = df.withColumn("author_sk", F.row_number().over(window_spec))
    
    # Reorder columns to match schema
    df = df.select(
        "author_sk",
        "author_tag",
        "author_name",
        "author_url",
        "created_at",
        "updated_at",
        "is_active"
    )
    
    print("Transformation complete")
    df.printSchema()
    df.show(10, truncate=False)
    
    return df


def write_to_gold_table(spark, df: DataFrame, record_count: int):
    """
    Write dimension data to Gold Iceberg table
    
    Mode: OVERWRITE (full refresh for Type 1 SCD)
    """
    print(f"\nWriting to Gold table: {GOLD_TABLE_FULL}")
    print(f"Records to write: {record_count:,}")
    
    # Write to Iceberg table
    df.writeTo(GOLD_TABLE_FULL) \
        .using("iceberg") \
        .overwritePartitions()  # Full refresh
    
    print(f"Successfully wrote {record_count:,} records to {GOLD_TABLE_FULL}")


def validate_results(spark):
    """
    Validate the created dimension table
    """
    print("\nValidating results...")
    
    df = spark.table(GOLD_TABLE_FULL)
    
    total_count = df.count()
    authors_with_name = df.filter(F.col("author_name").isNotNull()).count()
    authors_with_url = df.filter(F.col("author_url").isNotNull()).count()
    
    print("\nValidation Summary:")
    print(f"Total authors: {total_count:,}")
    print(f"Authors with name: {authors_with_name:,} ({authors_with_name/total_count*100:.2f}%)")
    print(f"Authors with URL: {authors_with_url:,} ({authors_with_url/total_count*100:.2f}%)")
    
    print("\nSample authors:")
    df.select("author_sk", "author_tag", "author_name", "author_url") \
        .orderBy("author_sk") \
        .show(20, truncate=False)
    
    print("\nTop 20 authors by tag (alphabetical):")
    df.select("author_sk", "author_tag", "author_name") \
        .orderBy("author_tag") \
        .show(20, truncate=False)


def main():
    """
    Main execution flow for dim_author job
    """
    print("=" * 80)
    print("Gold Layer - Dimension Author Job")
    print("=" * 80)
    
    # Initialize Spark session
    spark = get_spark_session(app_name="Gold_Dim_Author")
    
    # Initialize logger
    logger = get_gold_logger(spark)
    
    job_start_time = datetime.now()
    record_count = 0
    
    try:
        # Step 1: Create Gold database
        create_gold_database(spark)
        
        # Step 2: Create dimension table
        create_dim_author_table(spark)
        
        # Step 3: Load source data
        source_df = load_source_data(spark)
        
        # Step 4: Validate and normalize
        validated_df = validate_and_normalize_author_data(source_df)
        
        # Step 5: Transform to dimension format
        dim_df = transform_to_dimension(validated_df)
        record_count = dim_df.count()
        
        # Step 6: Write to Gold table
        write_to_gold_table(spark, dim_df, record_count)
        
        # Step 7: Validate results
        validate_results(spark)
        
        # Log success
        job_end_time = datetime.now()
        execution_time = (job_end_time - job_start_time).total_seconds()
        
        logger.log_job_success(
            source_path=SOURCE_SILVER_TABLE,
            table_name=GOLD_TABLE_FULL,
            records_processed=record_count,
            job_details={
                "job_type": "dimension",
                "execution_time_seconds": execution_time,
                "source_type": "silver_table",
                "authors_with_name": dim_df.filter(F.col("author_name").isNotNull()).count(),
                "authors_with_url": dim_df.filter(F.col("author_url").isNotNull()).count(),
            }
        )
        
        print("\n" + "=" * 80)
        print("dim_author job completed successfully!")
        print(f"Records: {record_count:,}")
        print(f"Execution time: {execution_time:.2f}s")
        print("=" * 80)
        
    except Exception as e:
        # Log failure
        logger.log_job_failure(
            source_path=SOURCE_SILVER_TABLE,
            table_name=GOLD_TABLE_FULL,
            error_message=str(e)
        )
        
        print("\n" + "=" * 80)
        print(f"dim_author job failed: {e}")
        print("=" * 80)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

