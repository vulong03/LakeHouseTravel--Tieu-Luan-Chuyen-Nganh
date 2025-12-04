"""
Gold Layer - Dimension Country Job
=====================================
Builds `gold.dim_country` from the `reviewer_country` values in `silver.hotels_reviews`.

Schema:
 - country_sk: surrogate key (int)
 - country_name: business key (string)
 - region: string (nullable)
 - created_at, updated_at: timestamps
 - is_active: boolean

Writes Iceberg table: `gold.gold.dim_country`
"""

import sys
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from config import (
    SOURCE_TABLE, SOURCE_COLUMN, SOURCE_CATALOG, SOURCE_DATABASE,
    GOLD_CATALOG, GOLD_DATABASE, GOLD_TABLE, GOLD_TABLE_FULL, BUSINESS_KEY,
    COUNTRY_TO_REGION
)
from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.gold_job_logger import get_gold_logger

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
)
from pyspark.sql.window import Window
import unicodedata
import re


def create_gold_database(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")


def create_dim_country_table(spark):
    schema = StructType([
        StructField("country_sk", IntegerType(), False),
        StructField("country_name", StringType(), False),
        StructField("region", StringType(), True),
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
    print(f"\nLoading country values from: {source_full}.{SOURCE_COLUMN}")
    df = spark.table(source_full).select(F.col(SOURCE_COLUMN).alias("country_name"))
    df = df.filter(F.col("country_name").isNotNull())
    print(f"Found {df.count()} raw rows (including duplicates)")
    return df


def map_region_py(name: str) -> str:
    if name is None:
        return None
    # Normalize input: lowercase, strip accents, collapse spaces
    n = name.strip().lower()
    n = unicodedata.normalize('NFKD', n)
    n = ''.join(c for c in n if not unicodedata.combining(c))
    n = re.sub(r"\s+", " ", n)

    # Build normalized mapping once (cache on module)
    if not hasattr(map_region_py, "_norm_map"):
        norm_map = {}
        for k, v in COUNTRY_TO_REGION.items():
            if k is None:
                continue
            kk = k.strip().lower()
            kk = unicodedata.normalize('NFKD', kk)
            kk = ''.join(c for c in kk if not unicodedata.combining(c))
            kk = re.sub(r"\s+", " ", kk)
            norm_map[kk] = v
        map_region_py._norm_map = norm_map

    return map_region_py._norm_map.get(n)


def transform(df):
    print("\nTransforming countries to dimension format...")
    df = df.select(F.trim(F.col("country_name")).alias("country_name"))
    df = df.filter(F.col("country_name") != "")
    before = df.count()
    df = df.dropDuplicates(BUSINESS_KEY)
    after = df.count()
    print(f"Deduplicated {before - after} rows; unique countries: {after}")

    # map region using small python dict via UDF
    map_region_udf = F.udf(map_region_py, StringType())
    df = df.withColumn("region", map_region_udf(F.col("country_name")))

    current_ts = F.current_timestamp()
    window_spec = Window.orderBy("country_name")

    df = (
        df.withColumn("country_sk", F.row_number().over(window_spec))
        .withColumn("created_at", current_ts)
        .withColumn("updated_at", current_ts)
        .withColumn("is_active", F.lit(True))
        .select("country_sk", "country_name", "region", "created_at", "updated_at", "is_active")
    )

    df.printSchema()
    df.show(50, truncate=False)
    return df


def write_to_gold(df):
    count = df.count()
    print(f"\nWriting {count} records to {GOLD_TABLE_FULL}")
    df.writeTo(GOLD_TABLE_FULL).using("iceberg").overwritePartitions()
    print(f"Wrote {count} records to {GOLD_TABLE_FULL}")


def validate(spark):
    print("\nValidating dim_country...")
    df = spark.table(GOLD_TABLE_FULL)
    total = df.count()
    print(f"Total countries: {total}")
    df.show(50, truncate=False)


def main():
    print("=" * 80)
    print("Gold Layer - Dimension Country Job")
    print("=" * 80)

    spark = get_spark_session(app_name="Gold_Dim_Country")
    logger = get_gold_logger(spark)

    start = datetime.now()
    records = 0

    try:
        create_gold_database(spark)
        create_dim_country_table(spark)

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

        print("\ndim_country completed successfully")

    except Exception as e:
        logger.log_job_failure(
            source_path=f"silver.{SOURCE_TABLE}.{SOURCE_COLUMN}",
            table_name=GOLD_TABLE_FULL,
            error_message=str(e),
        )
        print(f"\nJob failed: {e}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
