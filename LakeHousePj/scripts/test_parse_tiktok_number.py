#!/usr/bin/env python3
"""
Test script for parse_tiktok_number() function

Tests all edge cases before running full pipeline
"""

import sys
sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

def parse_tiktok_number(value_col):
    """
    Parse TikTok number format to integer (copy from step_02_clean_load.py)
    """
    trimmed = F.trim(value_col)
    
    # Case 1: Plain number (all digits)
    plain_number = F.when(
        trimmed.rlike("^\\d+$"),
        trimmed.cast(IntegerType())
    )
    
    # Case 2: K format (thousands) - e.g., "36.6K" → 36600, "22K" → 22000
    k_pattern = F.regexp_extract(trimmed, r"^([\d.]+)K$", 1)
    k_value = F.when(
        trimmed.rlike("^[\\d.]+K$"),
        (k_pattern.cast("double") * 1000).cast(IntegerType())
    )
    
    # Case 3: M format (millions) - e.g., "1.5M" → 1500000, "1M" → 1000000
    m_pattern = F.regexp_extract(trimmed, r"^([\d.]+)M$", 1)
    m_value = F.when(
        trimmed.rlike("^[\\d.]+M$"),
        (m_pattern.cast("double") * 1000000).cast(IntegerType())
    )
    
    # Combine: try plain number first, then K, then M, else NULL
    return F.when(
        value_col.isNotNull() & (trimmed != "") & (trimmed != "N/A"),
        F.coalesce(plain_number, k_value, m_value)
    ).otherwise(F.lit(None).cast(IntegerType()))


def test_parse_tiktok_number():
    """Test parse_tiktok_number() with various test cases"""
    
    spark = get_spark_session(app_name="Test_Parse_TikTok_Number")
    
    print("=" * 80)
    print("🧪 TESTING parse_tiktok_number() FUNCTION")
    print("=" * 80)
    
    # Test cases: (input_value, expected_output, description)
    test_cases = [
        # Plain numbers
        ("1234", 1234, "Plain number"),
        ("0", 0, "Zero"),
        ("999999", 999999, "Large plain number"),
        
        # K format (thousands)
        ("36.6K", 36600, "K format with decimal"),
        ("22K", 22000, "K format without decimal"),
        ("1K", 1000, "K format single digit"),
        ("0.5K", 500, "K format less than 1"),
        ("100.5K", 100500, "K format with decimal"),
        ("999.9K", 999900, "K format max"),
        
        # M format (millions)
        ("1.5M", 1500000, "M format with decimal"),
        ("1M", 1000000, "M format without decimal"),
        ("0.5M", 500000, "M format less than 1"),
        ("10.5M", 10500000, "M format with decimal"),
        ("999.9M", 999900000, "M format max"),
        
        # Edge cases - should return NULL
        ("N/A", None, "N/A value"),
        ("Chia sẻ", None, "Vietnamese text"),
        ("", None, "Empty string"),
        (None, None, "NULL value"),
        ("abc", None, "Non-numeric text"),
        ("12.34", None, "Decimal without K/M"),
        ("12K5", None, "Invalid K format"),
        ("12M5", None, "Invalid M format"),
        ("K", None, "Only K"),
        ("M", None, "Only M"),
        (" 1234 ", 1234, "Number with spaces"),
        (" 36.6K ", 36600, "K format with spaces"),
        (" 1.5M ", 1500000, "M format with spaces"),
    ]
    
    # Create test DataFrame
    test_data = [(case[0], case[1], case[2]) for case in test_cases]
    schema = StructType([
        StructField("input_value", StringType(), True),
        StructField("expected_output", IntegerType(), True),
        StructField("description", StringType(), True)
    ])
    
    df = spark.createDataFrame(test_data, schema)
    
    # Apply parse_tiktok_number function
    df_result = df.withColumn("actual_output", parse_tiktok_number(F.col("input_value")))
    
    # Compare expected vs actual
    df_result = df_result.withColumn(
        "test_passed",
        F.when(
            (F.col("expected_output").isNull() & F.col("actual_output").isNull()) |
            (F.col("expected_output") == F.col("actual_output")),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    
    # Show results
    print("\n📊 TEST RESULTS:")
    print("=" * 80)
    
    # Count passed/failed
    total = df_result.count()
    passed = df_result.filter(F.col("test_passed") == True).count()
    failed = df_result.filter(F.col("test_passed") == False).count()
    
    print(f"\n✅ Total tests: {total}")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    
    # Show all results
    print("\n" + "=" * 80)
    print("📋 DETAILED TEST RESULTS:")
    print("=" * 80)
    
    df_result.select(
        "description",
        "input_value",
        "expected_output",
        "actual_output",
        "test_passed"
    ).show(total, truncate=False)
    
    # Show failed tests only
    failed_tests = df_result.filter(F.col("test_passed") == False).collect()
    
    if failed_tests:
        print("\n" + "=" * 80)
        print("❌ FAILED TESTS:")
        print("=" * 80)
        for row in failed_tests:
            print(f"\n   Description: {row.description}")
            print(f"   Input: '{row.input_value}'")
            print(f"   Expected: {row.expected_output}")
            print(f"   Actual: {row.actual_output}")
        print("\n" + "=" * 80)
        print("❌ SOME TESTS FAILED! Please review the logic.")
        return False
    else:
        print("\n" + "=" * 80)
        print("✅ ALL TESTS PASSED!")
        print("=" * 80)
        return True


def test_with_real_data_sample():
    """Test with a sample of real data from Scratch"""
    
    spark = get_spark_session(app_name="Test_Real_Data")
    
    print("\n" + "=" * 80)
    print("🧪 TESTING WITH REAL DATA SAMPLE")
    print("=" * 80)
    
    try:
        # Read sample from Scratch
        scratch_path = "s3a://scratch/pipeline/silver/tiktok_post_metadata/"
        df = spark.read.parquet(scratch_path).limit(100)
        
        print(f"\n📂 Loaded {df.count()} records from Scratch")
        
        # Test parse_tiktok_number on real data
        print("\n🔧 Testing parse_tiktok_number on real data...")
        
        for col in ['likes', 'saves', 'shares']:
            print(f"\n   Testing column: {col}")
            
            # Get distinct values before parsing
            distinct_before = df.select(col).distinct().limit(20).collect()
            print(f"   Sample values before parsing:")
            for row in distinct_before[:10]:
                val = row[col]
                print(f"      - '{val}'")
            
            # Apply parsing
            df_test = df.select(col).withColumn(
                f"{col}_parsed",
                parse_tiktok_number(F.col(col))
            )
            
            # Show results
            print(f"\n   Sample parsing results:")
            df_test.filter(F.col(col).isNotNull()).limit(10).show(truncate=False)
            
            # Count NULL before and after
            null_before = df.filter(F.col(col).isNull() | (F.col(col) == "") | (F.col(col) == "N/A")).count()
            null_after = df_test.filter(F.col(f"{col}_parsed").isNull()).count()
            
            print(f"\n   NULL count before parsing: {null_before}")
            print(f"   NULL count after parsing: {null_after}")
            
            # Count successfully parsed
            parsed_count = df_test.filter(F.col(f"{col}_parsed").isNotNull()).count()
            print(f"   Successfully parsed: {parsed_count}")
        
    except Exception as e:
        print(f"\n⚠️  Could not test with real data: {e}")
        print("   (This is OK if Scratch layer is empty)")
    
    finally:
        spark.stop()


def main():
    """Run all tests"""
    
    print("\n" + "=" * 80)
    print("🚀 STARTING TESTS FOR parse_tiktok_number()")
    print("=" * 80)
    
    # Test 1: Unit tests with test cases
    test_passed = test_parse_tiktok_number()
    
    # Test 2: Test with real data sample
    test_with_real_data_sample()
    
    print("\n" + "=" * 80)
    if test_passed:
        print("✅ ALL TESTS COMPLETED SUCCESSFULLY!")
        print("   You can now run the full pipeline with confidence.")
    else:
        print("❌ SOME TESTS FAILED!")
        print("   Please fix the issues before running the full pipeline.")
    print("=" * 80)


if __name__ == "__main__":
    main()

