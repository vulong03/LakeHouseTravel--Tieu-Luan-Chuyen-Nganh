"""
Test script for dim_province job
Run this to validate the dimension table after job execution
"""

import sys
sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from pyspark.sql import functions as F

def test_dim_province():
    """Run validation tests on dim_province table"""
    
    print("=" * 80)
    print("Testing Gold Layer - dim_province")
    print("=" * 80)
    
    # Initialize Spark
    spark = get_spark_session(app_name="Test_Dim_Province")
    
    try:
        # Test 1: Table exists
        print("\n✓ Test 1: Checking table exists...")
        tables = spark.sql("SHOW TABLES IN gold").collect()
        table_names = [row.tableName for row in tables]
        assert 'dim_province' in table_names, "Table dim_province not found!"
        print("  ✅ PASS: Table exists")
        
        # Load table
        df = spark.table("gold.dim_province")
        
        # Test 2: Record count
        print("\n✓ Test 2: Checking record count...")
        count = df.count()
        assert count == 63, f"Expected 63 records, got {count}"
        print(f"  ✅ PASS: Found {count} records")
        
        # Test 3: Schema validation
        print("\n✓ Test 3: Validating schema...")
        expected_columns = [
            'province_sk', 'province_name', 'province_name_afterLaw',
            'region', 'is_city', 'created_at', 'updated_at', 'is_active'
        ]
        actual_columns = df.columns
        assert actual_columns == expected_columns, f"Schema mismatch: {actual_columns}"
        print("  ✅ PASS: Schema is correct")
        
        # Test 4: No nulls in required fields
        print("\n✓ Test 4: Checking for nulls in required fields...")
        null_sk = df.filter(F.col("province_sk").isNull()).count()
        null_name = df.filter(F.col("province_name").isNull()).count()
        null_created = df.filter(F.col("created_at").isNull()).count()
        null_active = df.filter(F.col("is_active").isNull()).count()
        
        assert null_sk == 0, f"Found {null_sk} null province_sk"
        assert null_name == 0, f"Found {null_name} null province_name"
        assert null_created == 0, f"Found {null_created} null created_at"
        assert null_active == 0, f"Found {null_active} null is_active"
        print("  ✅ PASS: No nulls in required fields")
        
        # Test 5: Business key uniqueness
        print("\n✓ Test 5: Checking business key uniqueness...")
        distinct_names = df.select("province_name").distinct().count()
        assert distinct_names == count, f"Duplicate province names found!"
        print("  ✅ PASS: Business keys are unique")
        
        # Test 6: Surrogate key uniqueness
        print("\n✓ Test 6: Checking surrogate key uniqueness...")
        distinct_sk = df.select("province_sk").distinct().count()
        assert distinct_sk == count, f"Duplicate surrogate keys found!"
        print("  ✅ PASS: Surrogate keys are unique")
        
        # Test 7: Central cities count
        print("\n✓ Test 7: Checking central municipalities...")
        city_count = df.filter(F.col("is_city") == True).count()
        assert city_count == 5, f"Expected 5 cities, got {city_count}"
        print(f"  ✅ PASS: Found {city_count} central municipalities")
        
        # Test 8: is_active flag
        print("\n✓ Test 8: Checking is_active flag...")
        active_count = df.filter(F.col("is_active") == True).count()
        assert active_count == count, f"All records should be active"
        print(f"  ✅ PASS: All {active_count} records are active")
        
        # Test 9: province_name_afterLaw populated
        print("\n✓ Test 9: Checking province_name_afterLaw...")
        null_after_law = df.filter(F.col("province_name_afterLaw").isNull()).count()
        assert null_after_law == 0, f"Found {null_after_law} null afterLaw values"
        print("  ✅ PASS: All provinces have afterLaw mapping")
        
        # Test 10: Sample data validation
        print("\n✓ Test 10: Validating sample data...")
        
        # Check Hà Nội is a city
        hanoi = df.filter(F.col("province_name") == "Hà Nội").collect()[0]
        assert hanoi.is_city == True, "Hà Nội should be marked as city"
        assert hanoi.province_name_afterLaw == "Hà Nội", "Hà Nội should keep its name"
        
        # Check a merged province (example: Hà Giang → Tuyên Quang)
        ha_giang = df.filter(F.col("province_name") == "Hà Giang").collect()
        if ha_giang:
            ha_giang = ha_giang[0]
            assert ha_giang.province_name_afterLaw == "Tuyên Quang", \
                f"Hà Giang should map to Tuyên Quang, got {ha_giang.province_name_afterLaw}"
        
        print("  ✅ PASS: Sample data is correct")
        
        # Summary Statistics
        print("\n" + "=" * 80)
        print("Summary Statistics")
        print("=" * 80)
        
        print(f"\n📊 Total Provinces/Cities: {count}")
        print(f"🏙️  Central Municipalities: {city_count}")
        print(f"🏞️  Provinces: {count - city_count}")
        
        # Count by region
        print("\n📍 Distribution by Region:")
        df.groupBy("region") \
            .agg(F.count("*").alias("count")) \
            .orderBy(F.desc("count")) \
            .show(truncate=False)
        
        # Show central cities
        print("\n🏙️  Central Municipalities:")
        df.filter(F.col("is_city") == True) \
            .select("province_sk", "province_name", "province_name_afterLaw", "region") \
            .orderBy("province_name") \
            .show(truncate=False)
        
        # Show provinces with name changes
        print("\n🔄 Provinces with Name Changes:")
        df.filter(F.col("province_name") != F.col("province_name_afterLaw")) \
            .select("province_sk", "province_name", "province_name_afterLaw", "region") \
            .orderBy("province_name") \
            .show(20, truncate=False)
        
        print("\n" + "=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        raise
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    test_dim_province()

