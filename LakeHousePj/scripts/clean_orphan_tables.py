#!/usr/bin/env python3
"""
Clean orphaned tables from Hive Metastore
Tables that have metadata in Hive but no actual data files
"""

from pyspark.sql import SparkSession

def main():
    print("🧹 Cleaning orphaned tables from Silver database...")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("CleanOrphanTables") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://silver/warehouse") \
        .getOrCreate()
    
    # List of tables to clean
    orphan_tables = ["hotels_list", "tiktok_videos"]
    
    for table in orphan_tables:
        try:
            print(f"\n🗑️  Dropping table: {table}")
            # Use Hive catalog directly to drop without checking file existence
            spark.sql(f"DROP TABLE IF EXISTS silver.{table} PURGE")
            print(f"   ✅ Dropped: {table}")
        except Exception as e:
            print(f"   ⚠️  Error dropping {table}: {str(e)}")
            # Try alternative approach - direct metastore manipulation
            try:
                print(f"   🔄 Attempting alternative cleanup for {table}...")
                spark.sql(f"DROP TABLE silver.{table}")
                print(f"   ✅ Cleaned: {table}")
            except Exception as e2:
                print(f"   ❌ Failed: {str(e2)}")
    
    # Verify cleanup
    print("\n📋 Remaining tables in Silver:")
    remaining = spark.sql("SHOW TABLES IN silver").collect()
    if remaining:
        for row in remaining:
            print(f"   - {row.tableName}")
    else:
        print("   (No tables - Clean!)")
    
    spark.stop()
    print("\n✅ Cleanup completed!")

if __name__ == "__main__":
    main()
