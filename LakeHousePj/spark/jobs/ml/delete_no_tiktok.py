import sys
import argparse
from minio import Minio
from minio.error import S3Error
from pyspark.sql import SparkSession

MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin123"
BUCKET_NAME = "gold"

TABLE_NAME = "gold.gold.province_month_forecast_lstm_no_tiktok_next12"
MINIO_PREFIX = "lakehouse/gold.db/province_month_forecast_lstm_no_tiktok_next12/"

def create_spark_session():
    return SparkSession.builder \
        .appName("Clean_No_Tiktok_Tables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.gold", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.gold.type", "hive") \
        .config("spark.sql.catalog.gold.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.gold.warehouse", "s3a://gold/lakehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def clean(execute=False):
    print("=" * 80)
    print("CLEAN UP 'NO_TIKTOK' BASES")
    print("=" * 80)
    print(f"Action Mode: {'EXECUTE (DELETING)' if execute else 'DRY RUN (PREVIEW)'}")
    
    # 1. Check and drop Spark Iceberg table
    print("\n--- 1. SPARK HIVE METASTORE ICEBERG TABLE ---")
    spark = create_spark_session()
    
    table_exists = False
    try:
        tables_df = spark.sql("SHOW TABLES IN gold.gold")
        tables = [row.tableName for row in tables_df.collect()]
        print(f"Active tables in gold.gold: {tables}")
        target_short_name = TABLE_NAME.split(".")[-1]
        if target_short_name in tables:
            table_exists = True
            print(f"Found table: {TABLE_NAME}")
        else:
            print(f"Table {TABLE_NAME} not found in Spark catalog.")
    except Exception as e:
        print(f"Error checking Spark tables: {e}")
        
    if table_exists:
        if execute:
            try:
                print(f"Executing: DROP TABLE IF EXISTS {TABLE_NAME}")
                spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
                print(f"Successfully dropped table {TABLE_NAME}")
            except Exception as e:
                print(f"Failed to drop table {TABLE_NAME}: {e}")
        else:
            print(f"Would execute: DROP TABLE IF EXISTS {TABLE_NAME}")
            
    spark.stop()
    
    # 2. Check and delete MinIO files
    print("\n--- 2. MINIO STORAGE FILES ---")
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )
    
    try:
        if not client.bucket_exists(BUCKET_NAME):
            print(f"Bucket '{BUCKET_NAME}' does not exist.")
            return
            
        objects = list(client.list_objects(BUCKET_NAME, prefix=MINIO_PREFIX, recursive=True))
        if not objects:
            print(f"No files found under prefix '{MINIO_PREFIX}' in bucket '{BUCKET_NAME}'.")
        else:
            print(f"Found {len(objects)} files under prefix '{MINIO_PREFIX}':")
            for obj in objects:
                print(f"  - {obj.object_name}")
                
            if execute:
                print(f"\nDeleting {len(objects)} objects...")
                for obj in objects:
                    try:
                        client.remove_object(BUCKET_NAME, obj.object_name)
                        print(f"  Deleted: {obj.object_name}")
                    except Exception as e:
                        print(f"  Failed to delete {obj.object_name}: {e}")
            else:
                print(f"\nWould delete {len(objects)} files from MinIO.")
                
    except S3Error as e:
        print(f"MinIO error: {e}")
    except Exception as e:
        print(f"Error checking MinIO: {e}")
        
    print("=" * 80)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Execute actual deletion")
    args = parser.parse_args()
    
    clean(execute=args.execute)
