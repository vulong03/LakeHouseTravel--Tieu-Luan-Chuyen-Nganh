"""
Script to check Silver layer data
Shows tables, record counts, and sample data
"""
from pyspark.sql import SparkSession
import sys

def main():
    print("=" * 60)
    print("   CHECKING SILVER LAYER DATA")
    print("=" * 60)
    print()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Check Silver Data") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://silver/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Check databases
        print("📦 DATABASES:")
        print("-" * 60)
        databases = spark.sql("SHOW DATABASES").collect()
        for db in databases:
            print(f"  • {db.namespace}")
        print()
        
        # Use silver database
        spark.sql("USE silver")
        
        # Show all tables in silver
        print("📋 TABLES IN SILVER DATABASE:")
        print("-" * 60)
        tables = spark.sql("SHOW TABLES").collect()
        
        if not tables:
            print("  ⚠️  No tables found in Silver layer!")
            return
        
        for table in tables:
            table_name = table.tableName
            print(f"\n  📊 Table: {table_name}")
            print(f"     " + "-" * 50)
            
            # Get row count
            try:
                count_df = spark.sql(f"SELECT COUNT(*) as count FROM silver.{table_name}")
                row_count = count_df.collect()[0]['count']
                print(f"     Total records: {row_count:,}")
                
                if row_count > 0:
                    # Show schema
                    print(f"\n     Schema:")
                    df = spark.table(f"silver.{table_name}")
                    for field in df.schema.fields:
                        print(f"       - {field.name}: {field.dataType.simpleString()}")
                    
                    # Show sample data (first 5 rows)
                    print(f"\n     Sample data (first 5 rows):")
                    sample_df = df.limit(5)
                    sample_df.show(truncate=False)
                    
                    # Show date range if there's a timestamp column
                    timestamp_cols = [f.name for f in df.schema.fields if 'timestamp' in f.name.lower() or 'date' in f.name.lower()]
                    if timestamp_cols:
                        for ts_col in timestamp_cols:
                            try:
                                date_range = spark.sql(f"""
                                    SELECT 
                                        MIN({ts_col}) as min_date,
                                        MAX({ts_col}) as max_date
                                    FROM silver.{table_name}
                                """)
                                date_info = date_range.collect()[0]
                                print(f"     Date range ({ts_col}): {date_info.min_date} to {date_info.max_date}")
                            except:
                                pass
                else:
                    print(f"     ⚠️  Table is empty!")
                    
            except Exception as e:
                print(f"     ❌ Error querying table: {str(e)}")
        
        print("\n" + "=" * 60)
        print("✅ Silver layer check completed!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
