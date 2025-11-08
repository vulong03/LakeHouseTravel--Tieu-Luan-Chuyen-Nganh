"""
Script to delete all data from Silver layer tables
"""
from pyspark.sql import SparkSession
import sys

def main():
    print("=" * 60)
    print("   DELETING ALL DATA FROM SILVER LAYER")
    print("=" * 60)
    print()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Delete Silver Data") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://silver/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        spark.sql("USE silver")
        
        # Get list of tables
        tables = spark.sql("SHOW TABLES").collect()
        
        if not tables:
            print("⚠️  No tables found in Silver database")
            return
        
        print(f"📋 Found {len(tables)} tables in Silver database:")
        for table in tables:
            print(f"  - {table.tableName}")
        
        print()
        print("🗑️  Starting deletion process...")
        print()
        
        for table in tables:
            table_name = table.tableName
            
            try:
                # Count before deletion
                count_before = spark.sql(f"SELECT COUNT(*) as count FROM silver.{table_name}").collect()[0]['count']
                print(f"📊 Table: {table_name}")
                print(f"   Records before: {count_before:,}")
                
                # Delete all records using Iceberg DELETE
                print(f"   ⏳ Deleting all records...")
                spark.sql(f"DELETE FROM silver.{table_name} WHERE 1=1")
                
                # Count after deletion
                count_after = spark.sql(f"SELECT COUNT(*) as count FROM silver.{table_name}").collect()[0]['count']
                print(f"   Records after: {count_after:,}")
                print(f"   ✅ Deleted: {count_before:,} records")
                print()
                
            except Exception as e:
                print(f"   ❌ Error deleting from {table_name}: {str(e)}")
                print()
        
        print("=" * 60)
        print("✅ Silver layer deletion completed!")
        print("=" * 60)
        print()
        print("📝 Next steps:")
        print("   1. Run Bronze ingestion: .\\scripts\\run-bronze-ingestion.ps1")
        print("   2. Check data: .\\scripts\\check-silver-data.ps1")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
