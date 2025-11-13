"""
Test script to verify Spark catalogs after config change
"""
from pyspark.sql import SparkSession

def main():
    print("\n" + "="*60)
    print("TESTING SPARK CATALOGS CONFIGURATION")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("TestCatalogs") \
        .getOrCreate()
    
    try:
        print("\n📋 === AVAILABLE CATALOGS ===")
        catalogs_df = spark.sql("SHOW CATALOGS")
        catalogs_df.show(truncate=False)
        
        catalogs = [row.catalog for row in catalogs_df.collect()]
        print(f"\nFound {len(catalogs)} catalogs: {catalogs}")
        
        # Test each catalog
        for catalog_name in ['bronze', 'silver', 'gold', 'lakehouse']:
            print(f"\n🔍 === TESTING CATALOG: {catalog_name} ===")
            try:
                spark.sql(f"SHOW DATABASES IN {catalog_name}").show()
                print(f"✅ Catalog '{catalog_name}' is accessible")
            except Exception as e:
                print(f"❌ Catalog '{catalog_name}' error: {str(e)[:100]}")
        
        # Test warehouse locations
        print("\n📦 === WAREHOUSE LOCATIONS ===")
        for catalog_name in ['bronze', 'silver', 'gold', 'lakehouse']:
            try:
                location = spark.conf.get(f"spark.sql.catalog.{catalog_name}.warehouse")
                print(f"{catalog_name:12} → {location}")
            except:
                print(f"{catalog_name:12} → Not configured")
        
        print("\n✅ TEST COMPLETED!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
