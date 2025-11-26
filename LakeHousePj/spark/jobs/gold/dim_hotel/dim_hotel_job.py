"""
Gold Layer - Dimension Hotel Job
================================
Builds `gold.dim_hotel` from Silver layer table `hotels_detail`.

Key responsibilities:
  1. Read from `silver.silver.hotels_detail` (main source)
  2. Join with `dim_province` (by province name)
  3. Deduplicate by `hotel_url` keeping latest
  4. Transform and write to Iceberg table
  5. Log execution to PostgreSQL via GoldJobLogger
"""

import sys
from datetime import datetime

sys.path.append("/opt/spark/jobs")

from config import (  # type: ignore
    SOURCE_HOTELS_TABLE,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    BUSINESS_KEY,
    DIM_PROVINCE_TABLE,
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
    DoubleType,
)
from pyspark.sql.window import Window


def create_gold_database(spark):
    """Create Gold database if not exists"""
    print("\n[*] Ensuring Gold database exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
    print(f"[OK] Database {GOLD_CATALOG}.{GOLD_DATABASE} ready")


def create_dim_hotel_table(spark):
    """Create dim_hotel Iceberg table if not exists"""
    print("\n[*] Ensuring dim_hotel Iceberg table exists...")
    schema = StructType(
        [
            StructField("hotel_sk", IntegerType(), False),
            StructField("province_sk", IntegerType(), True),
            StructField("hotel_url", StringType(), False),  # Business key
            StructField("hotel_name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("top_amenities", StringType(), True),
            StructField("rating_score", DoubleType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("rating_breakdown", StringType(), True),
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
    """Load hotel data from Silver layer"""
    print(f"\n📥 Loading source data from: {SOURCE_HOTELS_TABLE}")
    df = spark.table(SOURCE_HOTELS_TABLE)

    cnt = df.count()
    print(f"✅ Loaded {cnt:,} records from {SOURCE_HOTELS_TABLE}")
    return df


def join_with_dim_province(spark, df: DataFrame) -> DataFrame:
    """Join with dim_province to get province_sk"""
    print("\n🔗 Joining with dim_province...")

    df_province = spark.table(DIM_PROVINCE_TABLE).select(
        "province_sk",
        "province_name"
    )

    # Trim both sides before join
    df = df.withColumn("_province_trimmed", F.trim(F.col("province")))
    df_province = df_province.withColumn("_province_name_trimmed", F.trim(F.col("province_name")))

    df_joined = df.join(
        df_province,
        df["_province_trimmed"] == df_province["_province_name_trimmed"],
        how="left"
    ).drop("province_name", "_province_name_trimmed", "_province_trimmed")

    missing_count = df_joined.filter(F.col("province").isNotNull() & F.col("province_sk").isNull()).count()
    if missing_count > 0:
        print(f"⚠️  Warning: {missing_count} hotels have province but no matching province_sk")

    return df_joined


def deduplicate_by_hotel_url(df: DataFrame) -> DataFrame:
    """Deduplicate by hotel_url keeping the latest record"""
    print("\n🔧 Deduplicating by hotel_url (keep latest)...")

    original_count = df.count()
    print(f"   📊 Original records: {original_count:,}")

    # Order by latest `ingestion_timestamp`. `crawl_time` is intentionally
    # not used/processed for this job, so we rely on `ingestion_timestamp` only.
    window_spec = Window.partitionBy("hotel_url").orderBy(
        F.col("ingestion_timestamp").desc()
    )

    df = df.withColumn("_rank", F.row_number().over(window_spec)) \
           .filter(F.col("_rank") == 1) \
           .drop("_rank")

    final_count = df.count()
    removed = original_count - final_count
    print(f"   ✅ Removed duplicates: {removed:,} records")
    print(f"   ✅ Final unique hotels: {final_count:,}")

    return df


def transform_to_dimension(df: DataFrame) -> DataFrame:
    """Transform to dimension table format"""
    print("\n🔄 Transforming to dimension format...")

    current_timestamp = F.current_timestamp()

    df = df.select(
        "hotel_url",
        F.trim(F.col("hotel_name")).alias("hotel_name"),
        F.when(F.col("description").isNotNull(), F.trim(F.col("description"))).otherwise(F.lit(None)).alias("description"),
        F.when(F.col("top_amenities").isNotNull(), F.trim(F.col("top_amenities"))).otherwise(F.lit(None)).alias("top_amenities"),
        "rating_score",
        "review_count",
        "rating_breakdown",
        "province_sk"
    )

    # Add metadata
    print("   ⏰ Adding SCD metadata...")
    df = df.withColumn("created_at", current_timestamp)
    df = df.withColumn("updated_at", current_timestamp)
    df = df.withColumn("is_active", F.lit(True))

    # Generate surrogate key
    print("   🔑 Generating surrogate keys...")
    window_spec = Window.orderBy("hotel_url")
    df = df.withColumn("hotel_sk", F.row_number().over(window_spec))

    # Reorder columns to match schema
    df = df.select(
        "hotel_sk",
        "province_sk",
        "hotel_url",
        "hotel_name",
        "description",
        "top_amenities",
        "rating_score",
        "review_count",
        "rating_breakdown",
        "created_at",
        "updated_at",
        "is_active",
    )

    print("✅ Transformation complete")
    df.printSchema()
    df.show(10, truncate=False)

    return df


def write_to_gold_table(spark, df: DataFrame, record_count: int):
    """Write dimension data to Gold Iceberg table"""
    print(f"\n💾 Writing to Gold table: {GOLD_TABLE_FULL}")
    print(f"   📊 Records to write: {record_count:,}")

    df.writeTo(GOLD_TABLE_FULL) \
      .using("iceberg") \
      .overwritePartitions()

    print(f"✅ Successfully wrote {record_count:,} records to {GOLD_TABLE_FULL}")


def validate_results(spark):
    """Validate the created dimension table"""
    print("\n✅ Validating results...")

    df = spark.table(GOLD_TABLE_FULL)

    total_count = df.count()
    with_province = df.filter(F.col("province_sk").isNotNull()).count()
    with_name = df.filter(F.col("hotel_name").isNotNull()).count()

    print("\n📊 Validation Summary:")
    print(f"   Total hotels: {total_count:,}")
    print(f"   Hotels with province_sk: {with_province:,} ({with_province/total_count*100:.2f}%)")
    print(f"   Hotels with name: {with_name:,} ({with_name/total_count*100:.2f}%)")

    print("\n📝 Sample hotels:")
    df.select("hotel_sk", "hotel_url", "hotel_name", "province_sk") \
      .orderBy("hotel_sk") \
      .show(20, truncate=False)


def main():
    """Main execution flow for dim_hotel job"""
    print("=" * 80)
    print("Gold Layer - Dimension Hotel Job")
    print("=" * 80)

    spark = get_spark_session(app_name="Gold_Dim_Hotel")
    logger = get_gold_logger(spark)

    job_start_time = datetime.now()
    record_count = 0

    try:
        # Step 1: Create Gold database
        create_gold_database(spark)

        # Step 2: Create dimension table
        create_dim_hotel_table(spark)

        # Step 3: Load source data
        df = load_source_data(spark)

        # Step 4: Join with dim_province
        df = join_with_dim_province(spark, df)

        # Step 5: Deduplicate by hotel_url
        df = deduplicate_by_hotel_url(df)

        # Step 6: Transform to dimension format
        dim_df = transform_to_dimension(df)
        record_count = dim_df.count()

        # Step 7: Write to Gold table
        write_to_gold_table(spark, dim_df, record_count)

        # Step 8: Validate results
        validate_results(spark)

        # Log success
        job_end_time = datetime.now()
        execution_time = (job_end_time - job_start_time).total_seconds()

        logger.log_job_success(
            source_path=f"{SOURCE_HOTELS_TABLE}",
            table_name=GOLD_TABLE_FULL,
            records_processed=record_count,
            job_details={
                "job_type": "dimension",
                "execution_time_seconds": execution_time,
                "source_type": "silver_table",
                "hotels_with_province": dim_df.filter(F.col("province_sk").isNotNull()).count(),
            }
        )

        print("\n" + "=" * 80)
        print("✅ dim_hotel job completed successfully!")
        print(f"   Records: {record_count:,}")
        print(f"   Execution time: {execution_time:.2f}s")
        print("=" * 80)

    except Exception as e:
        logger.log_job_failure(
            source_path=f"{SOURCE_HOTELS_TABLE}",
            table_name=GOLD_TABLE_FULL,
            error_message=str(e)
        )

        print("\n" + "=" * 80)
        print(f"❌ dim_hotel job failed: {e}")
        print("=" * 80)
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
