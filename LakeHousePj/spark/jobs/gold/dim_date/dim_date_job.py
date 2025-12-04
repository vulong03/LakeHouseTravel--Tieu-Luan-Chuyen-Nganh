"""
Gold Layer - Dimension Date Job
================================
Purpose:
  - Read dim_date from PostgreSQL Date_DB database
  - Write to Gold Iceberg table gold.dim_date
  - Full refresh (overwrite) mode

Source: PostgreSQL Date_DB.dim_date
Target: gold.dim_date (Iceberg table)
"""

import sys
from datetime import datetime

sys.path.append("/opt/spark/jobs")

from config import (
    SOURCE_JDBC_URL,
    SOURCE_TABLE,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DRIVER,
    GOLD_CATALOG,
    GOLD_DATABASE,
    GOLD_TABLE,
    GOLD_TABLE_FULL,
    BUSINESS_KEY,
)
from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.gold_job_logger import get_gold_logger

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DateType,
    StringType,
    BooleanType,
)


def create_gold_database(spark):
    """Create Gold database if not exists"""
    print("\n[*] Ensuring Gold database exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
    print(f"[OK] Database {GOLD_CATALOG}.{GOLD_DATABASE} ready")


def create_dim_date_table(spark):
    """
    Create dim_date Iceberg table if not exists
    
    Schema matches PostgreSQL dim_date table
    """
    schema = StructType([
        StructField("date_sk", IntegerType(), False),  # Primary key
        StructField("full_date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("quarter_name", StringType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), False),
        StructField("year_month", IntegerType(), False),
        StructField("week_of_year", IntegerType(), False),
        StructField("year_week", IntegerType(), False),
        StructField("day_of_month", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("is_weekend", BooleanType(), False),
        StructField("is_month_start", BooleanType(), False),
        StructField("is_month_end", BooleanType(), False),
        StructField("is_quarter_start", BooleanType(), False),
        StructField("is_quarter_end", BooleanType(), False),
        StructField("is_year_start", BooleanType(), False),
        StructField("is_year_end", BooleanType(), False),
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database=GOLD_DATABASE,
        table_name=GOLD_TABLE,
        schema=schema,
        partition_by=[],  # Dimension table, not partitioned
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        },
        catalog=GOLD_CATALOG
    )
    print(f"[OK] Table {GOLD_TABLE_FULL} ready")


def load_from_postgres(spark):
    """
    Load dim_date data from PostgreSQL using JDBC
    """
    print(f"\nLoading data from PostgreSQL: {SOURCE_JDBC_URL}")
    print(f"Table: {SOURCE_TABLE}")
    
    jdbc_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": POSTGRES_DRIVER
    }
    
    df = spark.read.jdbc(
        url=SOURCE_JDBC_URL,
        table=SOURCE_TABLE,
        properties=jdbc_properties
    )
    
    record_count = df.count()
    print(f"Loaded {record_count:,} records from PostgreSQL")
    df.printSchema()
    df.show(10, truncate=False)
    
    return df


def transform_to_dimension(df):
    """
    Transform PostgreSQL data to dimension format
    - Ensure correct column order
    - Explicitly cast boolean columns to BooleanType (safe even if already boolean)
    """
    print("\nTransforming to dimension format...")
    
    # Ensure columns are in correct order and explicitly cast boolean columns
    df = df.select(
        "date_sk",
        "full_date",
        "year",
        "quarter",
        "quarter_name",
        "month",
        "month_name",
        "year_month",
        "week_of_year",
        "year_week",
        "day_of_month",
        "day_of_week",
        "day_name",
        F.col("is_weekend").cast(BooleanType()).alias("is_weekend"),
        F.col("is_month_start").cast(BooleanType()).alias("is_month_start"),
        F.col("is_month_end").cast(BooleanType()).alias("is_month_end"),
        F.col("is_quarter_start").cast(BooleanType()).alias("is_quarter_start"),
        F.col("is_quarter_end").cast(BooleanType()).alias("is_quarter_end"),
        F.col("is_year_start").cast(BooleanType()).alias("is_year_start"),
        F.col("is_year_end").cast(BooleanType()).alias("is_year_end")
    )
    
    print("Transformation complete")
    df.printSchema()
    df.show(10, truncate=False)
    
    return df


def write_to_gold_table(df, record_count=None):
    """
    Write dimension data to Gold Iceberg table
    Mode: OVERWRITE (full refresh for Type 1 SCD)
    
    Args:
        df: DataFrame to write
        record_count: Optional pre-computed count (to avoid counting twice)
    """
    print(f"\nWriting to Gold table: {GOLD_TABLE_FULL}")
    
    if record_count is None:
        record_count = df.count()
    
    print(f"   Records to write: {record_count:,}")
    
    # Write to Iceberg table
    df.writeTo(GOLD_TABLE_FULL) \
        .using("iceberg") \
        .overwritePartitions()  # Full refresh
    
    print(f"Successfully wrote {record_count:,} records to {GOLD_TABLE_FULL}")
    
    return record_count


def validate_results(spark):
    """
    Validate the created dimension table
    """
    print("\nValidating results...")
    
    df = spark.table(GOLD_TABLE_FULL)
    
    total_count = df.count()
    print(f"\nValidation Summary:")
    print(f"   Total dates: {total_count:,}")
    
    # Check date range
    min_date = df.agg(F.min("full_date").alias("min_date")).collect()[0]["min_date"]
    max_date = df.agg(F.max("full_date").alias("max_date")).collect()[0]["max_date"]
    print(f"Date range: {min_date} to {max_date}")
    
    # Check year distribution
    print("\nYear distribution:")
    df.groupBy("year") \
        .agg(F.count("*").alias("count")) \
        .orderBy("year") \
        .show(truncate=False)
    
    # Check weekend count
    weekend_count = df.filter(F.col("is_weekend") == True).count()
    print(f"\nWeekend days: {weekend_count:,}")
    
    # Sample data
    print("\nSample dates:")
    df.orderBy("full_date") \
        .select("date_sk", "full_date", "year", "quarter_name", "month_name", "day_name", "is_weekend") \
        .show(10, truncate=False)


def main():
    """
    Main execution flow for dim_date job
    """
    print("=" * 80)
    print("Gold Layer - Dimension Date Job")
    print("=" * 80)
    
    # Initialize Spark session
    spark = get_spark_session(app_name="Gold_Dim_Date")
    
    # Initialize logger
    logger = get_gold_logger(spark)
    
    job_start_time = datetime.now()
    record_count = 0
    
    try:
        # Step 1: Create Gold database
        create_gold_database(spark)
        
        # Step 2: Create dimension table
        create_dim_date_table(spark)
        
        # Step 3: Load from PostgreSQL
        source_df = load_from_postgres(spark)
        
        # Step 4: Transform to dimension format
        dim_df = transform_to_dimension(source_df)
        record_count = dim_df.count()
        
        # Step 5: Write to Gold table (pass record_count to avoid counting twice)
        write_to_gold_table(dim_df, record_count=record_count)
        
        # Step 6: Validate results
        validate_results(spark)
        
        # Log success
        job_end_time = datetime.now()
        execution_time = (job_end_time - job_start_time).total_seconds()
        
        logger.log_job_success(
            source_path=f"{SOURCE_JDBC_URL}/{SOURCE_TABLE}",
            table_name=GOLD_TABLE_FULL,
            records_processed=record_count,
            job_details={
                "job_type": "dimension",
                "execution_time_seconds": execution_time,
                "source_type": "postgresql",
                "date_range": {
                    "min_date": str(dim_df.agg(F.min("full_date")).collect()[0][0]),
                    "max_date": str(dim_df.agg(F.max("full_date")).collect()[0][0])
                },
                "schema": {
                    "business_key": BUSINESS_KEY,
                },
            }
        )
        
        print("\n" + "=" * 80)
        print("dim_date job completed successfully!")
        print(f"Records: {record_count:,}")
        print(f"Execution time: {execution_time:.2f}s")
        print("=" * 80)
        
    except Exception as e:
        # Log failure
        logger.log_job_failure(
            source_path=f"{SOURCE_JDBC_URL}/{SOURCE_TABLE}",
            table_name=GOLD_TABLE_FULL,
            error_message=str(e)
        )
        
        print(f"\nJob failed with error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

