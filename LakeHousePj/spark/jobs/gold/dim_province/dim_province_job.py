"""
Gold Layer - Dimension Province Job
====================================
Purpose:
  - Build dim_province dimension table from list_of_provinces_of_vietnam CSV
  - Apply business transformations:
    1. Map province_name_afterLaw using administrative merger policy
    2. Set is_city flag for 5 central municipalities
    3. Generate surrogate key (province_sk)
    4. Add SCD Type 1 metadata (created_at, updated_at, is_active)

Source: /data/VietNam_Province/list_of_provinces_of_vietnam-154j.csv
Target: gold.dim_province (Iceberg table)
"""

import sys
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from config import (
    SOURCE_CSV_PATH, GOLD_CATALOG, GOLD_DATABASE, GOLD_TABLE, GOLD_TABLE_FULL,
    BUSINESS_KEY, PROVINCE_AFTER_LAW, CENTRAL_CITIES
)
from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.gold_job_logger import get_gold_logger

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, 
    IntegerType, TimestampType
)
from pyspark.sql.window import Window


def create_gold_database(spark):
    """Create Gold database if not exists"""
    print(f"\n[*] Creating Gold database if not exists...")
    # Full path: catalog.database
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
    print(f"[OK] Database {GOLD_CATALOG}.{GOLD_DATABASE} ready")


def create_dim_province_table(spark):
    """
    Create dim_province Iceberg table if not exists
    
    Schema:
    - province_sk: Surrogate key (auto-generated)
    - province_name: Business key (current province name)
    - province_name_afterLaw: Name after administrative merger
    - region: Geographic region (enum from CSV)
    - is_city: Boolean (1 = central municipality, 0 = province)
    - created_at: Timestamp when record created
    - updated_at: Timestamp when record last updated
    - is_active: Boolean (SCD Type 1 flag)
    """
    schema = StructType([
        StructField("province_sk", IntegerType(), False),  # Surrogate key
        StructField("province_name", StringType(), False),  # Business key
        StructField("province_name_afterLaw", StringType(), True),
        StructField("region", StringType(), True),
        StructField("is_city", BooleanType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("is_active", BooleanType(), False),
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database=GOLD_DATABASE,
        table_name=GOLD_TABLE,
        schema=schema,
        partition_by=[],  # Dimension tables typically not partitioned
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        },
        catalog=GOLD_CATALOG
    )


def load_source_data(spark):
    """
    Load source CSV from /data/VietNam_Province/
    
    Expected columns:
    - Province/city
    - Region
    """
    print(f"\nLoading source data from: {SOURCE_CSV_PATH}")
    
    df = spark.read.csv(
        SOURCE_CSV_PATH,
        header=True,
        inferSchema=True
    )
    
    print(f"Loaded {df.count()} provinces")
    df.printSchema()
    df.show(5, truncate=False)
    
    return df


def transform_to_dim_province(df):
    """
    Transform source CSV to dimension table format
    
    Transformations:
    1. Rename 'Province/city' → 'province_name'
    2. Map 'province_name_afterLaw' using PROVINCE_AFTER_LAW dict
    3. Set 'is_city' flag (1 for central municipalities, 0 for provinces)
    4. Add metadata columns (created_at, updated_at, is_active)
    5. Generate surrogate key 'province_sk' using row_number
    """
    print(f"\nTransforming to dimension format...")
    
    current_timestamp = F.current_timestamp()
    
    # Step 1: Rename and select columns
    df = df.select(
        F.col("Province/city").alias("province_name"),
        F.col("Region").alias("region")
    )
    
    # Step 2: Map province_name_afterLaw
    print(f"Mapping province_name_afterLaw...")
    
    # Create mapping expression using CASE WHEN
    mapping_expr = F.create_map(
        *[F.lit(x) for pair in PROVINCE_AFTER_LAW.items() for x in pair]
    )
    
    df = df.withColumn(
        "province_name_afterLaw",
        F.coalesce(
            mapping_expr[F.col("province_name")],
            F.col("province_name")  # Default: keep original if not in mapping
        )
    )
    
    # Step 3: Set is_city flag
    print(f"Setting is_city flag for central municipalities...")
    df = df.withColumn(
        "is_city",
        F.when(F.col("province_name").isin(CENTRAL_CITIES), True).otherwise(False)
    )
    
    # Step 4: Add metadata columns
    print(f"Adding SCD metadata...")
    df = df.withColumn("created_at", current_timestamp)
    df = df.withColumn("updated_at", current_timestamp)
    df = df.withColumn("is_active", F.lit(True))
    
    # Step 5: Generate surrogate key
    print(f"Generating surrogate keys...")
    window_spec = Window.orderBy("province_name")
    df = df.withColumn("province_sk", F.row_number().over(window_spec))
    
    # Reorder columns to match schema
    df = df.select(
        "province_sk",
        "province_name",
        "province_name_afterLaw",
        "region",
        "is_city",
        "created_at",
        "updated_at",
        "is_active"
    )
    
    print(f"Transformation complete")
    df.printSchema()
    df.show(10, truncate=False)
    
    return df


def write_to_gold_table(spark, df):
    """
    Write dimension data to Gold Iceberg table
    
    Mode: OVERWRITE (full refresh for Type 1 SCD)
    """
    print(f"\nWriting to Gold table: {GOLD_TABLE_FULL}")
    
    record_count = df.count()
    print(f"Records to write: {record_count}")
    
    # Write to Iceberg table
    df.writeTo(GOLD_TABLE_FULL) \
        .using("iceberg") \
        .overwritePartitions()  # Full refresh
    
    print(f"Successfully wrote {record_count} records to {GOLD_TABLE_FULL}")


def validate_results(spark):
    """
    Validate the created dimension table
    """
    print(f"\nValidating results...")
    
    df = spark.table(GOLD_TABLE_FULL)
    
    total_count = df.count()
    city_count = df.filter(F.col("is_city") == True).count()
    province_count = df.filter(F.col("is_city") == False).count()
    
    print(f"\nValidation Summary:")
    print(f"Total provinces/cities: {total_count}")
    print(f"Central municipalities: {city_count}")
    print(f"Provinces: {province_count}")
    
    print(f"\nCentral municipalities:")
    df.filter(F.col("is_city") == True) \
        .select("province_sk", "province_name", "province_name_afterLaw", "region") \
        .orderBy("province_name") \
        .show(truncate=False)
    
    print(f"\nProvinces with name changes after law:")
    df.filter(F.col("province_name") != F.col("province_name_afterLaw")) \
        .select("province_sk", "province_name", "province_name_afterLaw", "region") \
        .orderBy("province_name") \
        .show(20, truncate=False)
    
    print(f"\nSample by region:")
    df.groupBy("region") \
        .agg(F.count("*").alias("province_count")) \
        .orderBy("region") \
        .show(truncate=False)


def main():
    """
    Main execution flow for dim_province job
    """
    print("=" * 80)
    print("Gold Layer - Dimension Province Job")
    print("=" * 80)
    
    # Initialize Spark session
    spark = get_spark_session(app_name="Gold_Dim_Province")
    
    # Initialize logger
    logger = get_gold_logger(spark)
    
    job_start_time = datetime.now()
    record_count = 0
    
    try:
        # Step 1: Create Gold database
        create_gold_database(spark)
        
        # Step 2: Create dimension table
        create_dim_province_table(spark)
        
        # Step 3: Load source data
        source_df = load_source_data(spark)
        
        # Step 4: Transform to dimension format
        dim_df = transform_to_dim_province(source_df)
        record_count = dim_df.count()
        
        # Step 5: Write to Gold table
        write_to_gold_table(spark, dim_df)
        
        # Step 6: Validate results
        validate_results(spark)
        
        # Log success
        job_end_time = datetime.now()
        execution_time = (job_end_time - job_start_time).total_seconds()
        
        logger.log_job_success(
            source_path=SOURCE_CSV_PATH,
            table_name=GOLD_TABLE_FULL,
            records_processed=record_count,
            job_details={
                "job_type": "dimension",
                "execution_time_seconds": execution_time,
                "source_type": "csv",
                "central_cities_count": 5,
                "provinces_with_name_change": record_count - dim_df.filter("province_name = province_name_afterLaw").count()
            }
        )
        
        print("\n" + "=" * 80)
        print("dim_province job completed successfully!")
        print(f"Records: {record_count}")
        print(f"Execution time: {execution_time:.2f}s")
        print("=" * 80)
        
    except Exception as e:
        # Log failure
        logger.log_job_failure(
            source_path=SOURCE_CSV_PATH,
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

