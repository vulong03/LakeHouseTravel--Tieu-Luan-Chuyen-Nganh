"""
Gold Layer - Fact: fact_hotel_review_daily Job
===============================================
Builds `gold.fact_hotel_review_daily` by joining `silver.hotels_reviews` with Gold dimensions:
  - dim_hotel, dim_travel_type, dim_room_type, dim_country, dim_date

Produces columns described in the ER diagram and maps text columns from silver (review_score, reviewer_name, review_title,
review_positive, review_negative). Also computes `is_low_score`, `is_high_score`, `review_score_bucket`.

Notes / assumptions:
 - LOW_SCORE_THRESHOLD = 2.0, HIGH_SCORE_THRESHOLD = 4.0 (configurable in config.py)
 - Joins try `hotel_url` first, then fall back to `hotel_name` when hotel match is missing.
"""

import sys
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from config import (
    SOURCE_TABLE,
    SOURCE_CATALOG,
    SOURCE_DATABASE,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    DIM_HOTEL_TABLE,
    DIM_TRAVEL_TYPE_TABLE,
    DIM_ROOM_TYPE_TABLE,
    DIM_COUNTRY_TABLE,
    DIM_DATE_TABLE,
    LOW_SCORE_THRESHOLD,
    HIGH_SCORE_THRESHOLD,
)
from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.gold_job_logger import get_gold_logger

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    DoubleType,
    StringType,
    BooleanType,
    TimestampType,
)
from pyspark.sql.window import Window


def create_gold_database(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")


def create_fact_table(spark):
    schema = StructType([
        StructField("fact_id", LongType(), False),
        StructField("hotel_sk", IntegerType(), True),
        StructField("traveler_type_sk", IntegerType(), True),
        StructField("room_type_sk", IntegerType(), True),
        StructField("country_sk", IntegerType(), True),
        StructField("stay_date_sk", IntegerType(), True),
        StructField("review_date_sk", IntegerType(), True),
        StructField("review_score", DoubleType(), True),
        StructField("reviewer_name", StringType(), True),
        StructField("review_title", StringType(), True),
        StructField("review_positive", StringType(), True),
        StructField("review_negative", StringType(), True),
        StructField("is_low_score", BooleanType(), False),
        StructField("is_high_score", BooleanType(), False),
        StructField("review_score_bucket", StringType(), True),
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


def load_sources(spark) -> DataFrame:
    source_full = f"{SOURCE_CATALOG}.{SOURCE_DATABASE}.{SOURCE_TABLE}"
    print(f"Loading silver source: {source_full}")
    df = spark.table(source_full)
    print(f"  → Loaded {df.count():,} rows from silver")
    return df


def transform(spark, df: DataFrame) -> DataFrame:
    print("Transforming and joining with dimensions...")

    # normalize keys for joining
    df = df.withColumn("_hotel_url_norm", F.lower(F.coalesce(F.col("hotel_url"), F.lit("")))) \
           .withColumn("_hotel_name_norm", F.lower(F.coalesce(F.col("hotel_name"), F.lit("")))) \
           .withColumn("_traveler_type_norm", F.lower(F.coalesce(F.col("traveler_type"), F.lit("")))) \
           .withColumn("_room_type_norm", F.lower(F.coalesce(F.col("room_type"), F.lit("")))) \
           .withColumn("_reviewer_country_norm", F.trim(F.col("reviewer_country")))

    # load dims
    df_hotel = spark.table(DIM_HOTEL_TABLE).select("hotel_sk", "hotel_name", "hotel_url") \
        .withColumn("_dhotel_url", F.lower(F.coalesce(F.col("hotel_url"), F.lit("")))) \
        .withColumn("_dhotel_name", F.lower(F.coalesce(F.col("hotel_name"), F.lit(""))))

    df_trav = spark.table(DIM_TRAVEL_TYPE_TABLE).select("traveler_type_sk", "traveler_type_name") \
        .withColumn("_dttrav", F.lower(F.coalesce(F.col("traveler_type_name"), F.lit(""))))

    df_room = spark.table(DIM_ROOM_TYPE_TABLE).select("room_type_sk", "room_type_name") \
        .withColumn("_droom", F.lower(F.coalesce(F.col("room_type_name"), F.lit(""))))

    df_country = spark.table(DIM_COUNTRY_TABLE).select("country_sk", "country_name")

    df_date = spark.table(DIM_DATE_TABLE).select("date_sk", "full_date")

    # Join: hotel by url first (use aliases to avoid ambiguous self-join columns)
    df_h1 = df_hotel.select("hotel_sk", "_dhotel_url", "_dhotel_name").alias("h1")
    joined = df.join(
        df_h1,
        df["_hotel_url_norm"] == F.col("h1._dhotel_url"),
        how="left",
    )

    # For rows without hotel_sk, try join by hotel_name (separate alias)
    df_h2 = df_hotel.select("hotel_sk", "_dhotel_name").withColumnRenamed("hotel_sk", "hotel_sk_name").alias("h2")
    joined = joined.join(
        df_h2,
        (F.col("hotel_sk").isNull()) & (F.col("_hotel_name_norm") == F.col("h2._dhotel_name")),
        how="left",
    )

    # coalesce hotel_sk
    joined = joined.withColumn(
        "hotel_sk_final",
        F.coalesce(F.col("hotel_sk"), F.col("hotel_sk_name")).cast(IntegerType()),
    )

    # traveler_type join
    joined = joined.join(
        df_trav.select("traveler_type_sk", "_dttrav"),
        joined["_traveler_type_norm"] == df_trav["_dttrav"],
        how="left",
    )

    # room_type join
    joined = joined.join(
        df_room.select("room_type_sk", "_droom"),
        joined["_room_type_norm"] == df_room["_droom"],
        how="left",
    )

    # country join (simple trim match)
    joined = joined.join(
        df_country.select("country_sk", "country_name"),
        F.trim(joined["reviewer_country"]) == df_country["country_name"],
        how="left",
    )

    # join with dim_date for stay_date and review_date
    joined = joined.withColumn("_stay_date", F.to_date(F.col("stay_date"))) \
                   .withColumn("_review_date", F.to_date(F.col("review_date")))

    joined = joined.join(
        df_date.withColumnRenamed("date_sk", "stay_date_sk"),
        joined["_stay_date"] == df_date["full_date"],
        how="left",
    ).withColumnRenamed("date_sk", "_tmp_date_sk").drop("_tmp_date_sk")

    # Because we used df_date earlier, join again properly (left join aliasing)
    df_date_alias = df_date.withColumnRenamed("date_sk", "date_sk_review").withColumnRenamed("full_date", "full_date_review")
    joined = joined.join(
        df_date_alias,
        joined["_review_date"] == df_date_alias["full_date_review"],
        how="left",
    )

    # Compute flags and buckets
    joined = joined.withColumn(
        "is_low_score",
        F.when(F.col("review_score").isNotNull() & (F.col("review_score") <= F.lit(LOW_SCORE_THRESHOLD)), True).otherwise(False),
    ).withColumn(
        "is_high_score",
        F.when(F.col("review_score").isNotNull() & (F.col("review_score") >= F.lit(HIGH_SCORE_THRESHOLD)), True).otherwise(False),
    )

    # bucket by nearest integer
    joined = joined.withColumn("review_score_bucket", F.when(F.col("review_score").isNotNull(), F.round(F.col("review_score")).cast(StringType())).otherwise(F.lit(None)))

    # Generate surrogate fact_id
    window = Window.orderBy("_review_date", "hotel_name")
    final = joined.withColumn("fact_id", F.row_number().over(window).cast(LongType()))

    # Build final select with chosen column names
    now = F.current_timestamp()
    result = final.select(
        F.col("fact_id"),
        F.col("hotel_sk_final").alias("hotel_sk"),
        F.col("traveler_type_sk"),
        F.col("room_type_sk").alias("room_type_sk"),
        F.col("country_sk"),
        F.col("stay_date_sk"),
        F.col("date_sk_review").alias("review_date_sk"),
        F.col("review_score"),
        F.col("reviewer_name"),
        F.col("review_title"),
        F.col("review_positive"),
        F.col("review_negative"),
        F.col("is_low_score"),
        F.col("is_high_score"),
        F.col("review_score_bucket"),
        now.alias("created_at"),
        now.alias("updated_at"),
        F.lit(True).alias("is_active"),
    )

    print("Transformation complete. Columns:")
    result.printSchema()
    return result


def write_to_gold(df: DataFrame):
    print(f"Writing {df.count():,} rows to {GOLD_TABLE_FULL} (append)")
    df.writeTo(GOLD_TABLE_FULL).using("iceberg").append()
    print("Write complete")


def main():
    print("=" * 80)
    print("Gold Layer - fact_hotel_review_daily Job")
    print("=" * 80)

    spark = get_spark_session(app_name="Gold_Fact_Hotel_Review_Daily")
    logger = get_gold_logger(spark)

    start = datetime.now()
    records = 0

    try:
        create_gold_database(spark)
        create_fact_table(spark)

        src = load_sources(spark)
        df_fact = transform(spark, src)
        records = df_fact.count()

        write_to_gold(df_fact)

        execution_time = (datetime.now() - start).total_seconds()
        logger.log_job_success(
            source_path=f"{SOURCE_CATALOG}.{SOURCE_DATABASE}.{SOURCE_TABLE}",
            table_name=GOLD_TABLE_FULL,
            records_processed=records,
            job_details={
                "job_type": "fact",
                "execution_time_seconds": execution_time,
            },
        )

        print("\nfact_hotel_review_daily completed successfully")

    except Exception as e:
        logger.log_job_failure(
            source_path=f"{SOURCE_CATALOG}.{SOURCE_DATABASE}.{SOURCE_TABLE}",
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
