"""
Gold Layer - Dimension Room Type Job
=====================================
Builds `gold.dim_room_type` from the `room_type` values in `silver.hotels_reviews`.

Schema:
 - room_type_sk: surrogate key (int)
 - room_type_name: business key (string)
 - created_at, updated_at: timestamps
 - is_active: boolean

Writes Iceberg table: `gold.gold.dim_room_type`
"""

import sys
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from config import (
    SOURCE_TABLE, SOURCE_COLUMN, SOURCE_CATALOG, SOURCE_DATABASE,
    GOLD_CATALOG, GOLD_DATABASE, GOLD_TABLE, GOLD_TABLE_FULL, BUSINESS_KEY
)
from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.gold_job_logger import get_gold_logger

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
)
from pyspark.sql.window import Window


def create_gold_database(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")


def create_dim_room_type_table(spark):
    schema = StructType([
        StructField("room_type_sk", IntegerType(), False),
        StructField("room_type_name", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("is_active", BooleanType(), False),
    ])

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


def load_source(spark):
    source_full = f"{SOURCE_CATALOG}.{SOURCE_DATABASE}.{SOURCE_TABLE}"
    print(f"\n📥 Loading room_type values from: {source_full}.{SOURCE_COLUMN}")
    df = spark.table(source_full).select(F.col(SOURCE_COLUMN).alias("room_type_name"))
    df = df.filter(F.col("room_type_name").isNotNull())
    print(f"✅ Found {df.count()} raw rows (including duplicates)")
    return df


def transform(df):
    print("\n🔄 Transforming room types to dimension format...")
    df = df.select(F.trim(F.col("room_type_name")).alias("room_type_name"))
    df = df.filter(F.col("room_type_name") != "")
    before = df.count()
    df = df.dropDuplicates(BUSINESS_KEY)
    after = df.count()
    print(f"   • Deduplicated {before - after} rows; unique room types: {after}")

    current_ts = F.current_timestamp()
    window_spec = Window.orderBy("room_type_name")

    df = (
        df.withColumn("room_type_sk", F.row_number().over(window_spec))
        .withColumn("created_at", current_ts)
        .withColumn("updated_at", current_ts)
        .withColumn("is_active", F.lit(True))
        .select("room_type_sk", "room_type_name", "created_at", "updated_at", "is_active")
    )

    df.printSchema()
    df.show(20, truncate=False)
    return df


def write_to_gold(df):
    count = df.count()
    print(f"\n💾 Writing {count} records to {GOLD_TABLE_FULL}")
    df.writeTo(GOLD_TABLE_FULL).using("iceberg").overwritePartitions()
    print(f"✅ Wrote {count} records to {GOLD_TABLE_FULL}")


def validate(spark):
    print("\n✅ Validating dim_room_type...")
    df = spark.table(GOLD_TABLE_FULL)
    total = df.count()
    print(f"   • Total room types: {total}")
    df.show(20, truncate=False)


def main():
    print("=" * 80)
    print("Gold Layer - Dimension Room Type Job")
    print("=" * 80)

    spark = get_spark_session(app_name="Gold_Dim_Room_Type")
    logger = get_gold_logger(spark)

    start = datetime.now()
    records = 0

    try:
        create_gold_database(spark)
        create_dim_room_type_table(spark)

        src = load_source(spark)
        dim_df = transform(src)
        records = dim_df.count()
        write_to_gold(dim_df)
        validate(spark)

        execution_time = (datetime.now() - start).total_seconds()
        logger.log_job_success(
            source_path=f"silver.{SOURCE_TABLE}.{SOURCE_COLUMN}",
            table_name=GOLD_TABLE_FULL,
            records_processed=records,
            job_details={
                "job_type": "dimension",
                "execution_time_seconds": execution_time,
            },
        )

        print("\n✅ dim_room_type completed successfully")

    except Exception as e:
        logger.log_job_failure(
            source_path=f"silver.{SOURCE_TABLE}.{SOURCE_COLUMN}",
            table_name=GOLD_TABLE_FULL,
            error_message=str(e),
        )
        print(f"\n❌ Job failed: {e}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
