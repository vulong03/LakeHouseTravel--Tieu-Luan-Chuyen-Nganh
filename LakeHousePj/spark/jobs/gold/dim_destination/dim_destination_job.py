"""
Gold Layer - Dimension Destination Job
======================================
Builds `gold.dim_destination` from curated tourism destination master data.

Key responsibilities:
  1. Read static CSV at /data/VietNam_Province/List_Destination.csv
  2. Deduplicate using (destination_name, province_name)
  3. Enrich with `province_sk` from dim_province (FK)
  4. Add surrogate key + metadata + write to Iceberg table
  5. Log execution to PostgreSQL via GoldJobLogger
"""

import sys
from datetime import datetime

sys.path.append("/opt/spark/jobs")

from config import (  # type: ignore
    SOURCE_CSV_PATH,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    BUSINESS_KEY,
    DESTINATION_TYPES,
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
    DoubleType,
    TimestampType,
)
from pyspark.sql.window import Window


def create_gold_database(spark):
    print("\n[*] Ensuring Gold database exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
    print(f"[OK] Database {GOLD_CATALOG}.{GOLD_DATABASE} ready")


def create_dim_destination_table(spark):
    print("\n[*] Ensuring dim_destination Iceberg table exists...")
    schema = StructType(
        [
            StructField("destination_sk", IntegerType(), False),
            StructField("destination_name", StringType(), False),
            StructField("province_name", StringType(), False),
            StructField("province_sk", IntegerType(), False),
            StructField("destination_type", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
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
    print(f"\n📥 Loading source data from: {SOURCE_CSV_PATH}")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(SOURCE_CSV_PATH)
    )
    print(f"✅ Loaded {df.count()} raw rows")
    df.printSchema()
    df.show(5, truncate=False)
    return df


def preprocess_source(df: DataFrame) -> DataFrame:
    print("\n🔧 Cleaning and normalizing source data...")
    df = df.select(
        F.trim(F.col("destination_name")).alias("destination_name"),
        F.trim(F.col("province_name")).alias("province_name"),
        F.trim(F.col("destination_type")).alias("destination_type"),
        F.trim(F.col("latitude")).alias("latitude_raw"),
        F.trim(F.col("longitude")).alias("longitude_raw"),
    )

    df = df.filter(
        F.col("destination_name").isNotNull() & F.col("province_name").isNotNull()
    )

    df = df.withColumn(
        "latitude",
        F.when(F.col("latitude_raw") == "", None)
        .otherwise(F.col("latitude_raw"))
        .cast(DoubleType()),
    ).withColumn(
        "longitude",
        F.when(F.col("longitude_raw") == "", None)
        .otherwise(F.col("longitude_raw"))
        .cast(DoubleType()),
    )

    df = df.drop("latitude_raw", "longitude_raw")

    before_dedup = df.count()
    df = df.dropDuplicates(BUSINESS_KEY)
    after_dedup = df.count()
    print(f"   • Deduplicated {before_dedup - after_dedup} duplicate rows")
    return df


def join_with_dim_province(spark, df: DataFrame) -> DataFrame:
    print("\n🔗 Joining with dim_province to fetch province_sk...")
    province_df = spark.table(DIM_PROVINCE_TABLE).select(
        "province_name", "province_sk"
    )

    joined_df = df.join(province_df, on="province_name", how="left")
    missing_df = joined_df.filter(F.col("province_sk").isNull())

    missing_count = missing_df.count()
    if missing_count > 0:
        print("❌ Found destinations with unknown province:")
        missing_df.select("destination_name", "province_name").show(
            missing_count, truncate=False
        )
        raise ValueError(
            f"{missing_count} destinations have province_name not present in dim_province"
        )

    return joined_df


def transform_to_dimension(df: DataFrame) -> DataFrame:
    print("\n🔄 Transforming dataset into dimension schema...")
    current_ts = F.current_timestamp()

    window_spec = Window.orderBy("province_sk", "destination_name")

    df = (
        df.withColumn("destination_sk", F.row_number().over(window_spec))
        .withColumn("created_at", current_ts)
        .withColumn("updated_at", current_ts)
        .withColumn("is_active", F.lit(True))
        .select(
            "destination_sk",
            "destination_name",
            "province_name",
            "province_sk",
            "destination_type",
            "latitude",
            "longitude",
            "created_at",
            "updated_at",
            "is_active",
        )
    )

    df.printSchema()
    df.show(10, truncate=False)
    return df


def write_to_gold_table(df: DataFrame):
    record_count = df.count()
    print(f"\n💾 Writing {record_count} records to {GOLD_TABLE_FULL} ...")
    (
        df.writeTo(GOLD_TABLE_FULL)
        .using("iceberg")
        .overwritePartitions()
    )
    print(f"✅ Successfully wrote {record_count} rows")
    return record_count


def validate_results(spark):
    print("\n✅ Validating dim_destination...")
    df = spark.table(GOLD_TABLE_FULL)
    total = df.count()
    distinct_provinces = df.select("province_name").distinct().count()

    print(f"   • Total destinations: {total}")
    print(f"   • Provinces covered: {distinct_provinces}")

    print("\n🏷️  Top destination types:")
    (
        df.groupBy("destination_type")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
        .show(10, truncate=False)
    )

    print("\n📍 Sample destinations:")
    (
        df.orderBy("province_name", "destination_name")
        .select(
            "destination_sk",
            "destination_name",
            "province_name",
            "province_sk",
            "destination_type",
            "latitude",
            "longitude",
        )
        .show(10, truncate=False)
    )


def collect_type_stats(df: DataFrame):
    type_counts = (
        df.groupBy("destination_type")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
        .collect()
    )
    return [
        {"destination_type": row["destination_type"], "count": row["count"]}
        for row in type_counts
    ]


def main():
    print("=" * 80)
    print("Gold Layer - Dimension Destination Job")
    print("=" * 80)

    spark = get_spark_session(app_name="Gold_Dim_Destination")
    logger = get_gold_logger(spark)

    job_start = datetime.now()
    record_count = 0

    try:
        create_gold_database(spark)
        create_dim_destination_table(spark)

        source_df = load_source_data(spark)
        cleaned_df = preprocess_source(source_df)
        enriched_df = join_with_dim_province(spark, cleaned_df)
        dim_df = transform_to_dimension(enriched_df)

        record_count = write_to_gold_table(dim_df)
        validate_results(spark)

        execution_time = (datetime.now() - job_start).total_seconds()
        type_stats = collect_type_stats(dim_df)

        logger.log_job_success(
            source_path=SOURCE_CSV_PATH,
            table_name=GOLD_TABLE_FULL,
            records_processed=record_count,
            job_details={
                "job_type": "dimension",
                "execution_time_seconds": execution_time,
                "source_type": "csv",
                "distinct_provinces": enriched_df.select("province_name")
                .distinct()
                .count(),
                "destination_types": type_stats,
                "schema": {
                    "business_key": BUSINESS_KEY,
                    "foreign_key": "province_sk",
                },
            },
        )

        print("\n" * 2)
        print("=" * 80)
        print("✅ dim_destination job completed successfully!")
        print(f"   Records: {record_count}")
        print(f"   Execution time: {execution_time:.2f}s")
        print("=" * 80)

    except Exception as exc:
        logger.log_job_failure(
            source_path=SOURCE_CSV_PATH,
            table_name=GOLD_TABLE_FULL,
            error_message=str(exc),
        )
        print(f"\n❌ Job failed: {exc}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()


