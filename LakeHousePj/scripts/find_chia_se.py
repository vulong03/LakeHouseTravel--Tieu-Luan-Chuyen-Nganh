#!/usr/bin/env python3
"""
Find CSV files with "Chia sẻ" to understand the parsing issue
"""

import sys
sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session

def find_chia_se():
    spark = get_spark_session(app_name="Find_Chia_Se")
    
    bronze_path = "s3a://bronze/lakehouse/tiktok_comments/raw/"
    
    print("=" * 80)
    print("🔍 Searching for 'Chia sẻ' in Bronze CSV files...")
    print("=" * 80)
    
    try:
        # Read all CSV files as text
        df = spark.read.text(bronze_path + "*.csv")
        
        # Find lines containing "Chia sẻ"
        lines_with_chia_se = df.filter(df.value.contains("Chia sẻ")).limit(20).collect()
        
        print(f"\n✅ Found {len(lines_with_chia_se)} lines with 'Chia sẻ':\n")
        
        for i, row in enumerate(lines_with_chia_se, 1):
            line = row.value
            print(f"{i}. {line}")
            
            # Check if it's a header line or data line
            if "Số lượt" in line or "share" in line.lower() or "lưu" in line.lower():
                print("   ⚠️  This looks like a header/metadata line!")
            print()
        
        # Also check for files that might have parsing issues
        print("\n" + "=" * 80)
        print("🔍 Checking file structure around 'Chia sẻ'...")
        print("=" * 80)
        
        # Get file paths
        file_paths = spark.read.format("binaryFile").load(bronze_path + "*.csv").select("path").distinct().limit(10).collect()
        
        for file_row in file_paths:
            file_path = file_row.path
            print(f"\n📄 File: {file_path.split('/')[-1]}")
            
            # Read first 20 lines
            file_df = spark.read.text(file_path)
            lines = file_df.limit(20).collect()
            
            for i, line_row in enumerate(lines, 1):
                line = line_row.value
                if "Chia sẻ" in line or "Số lượt" in line:
                    print(f"   Line {i}: {line}")
                    # Show context
                    if i > 1:
                        prev_line = lines[i-2].value
                        print(f"      ← Prev: {prev_line[:80]}")
                    if i < len(lines):
                        next_line = lines[i-1].value if i < len(lines) else ""
                        if next_line:
                            print(f"      → Next: {next_line[:80]}")
                    break
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    find_chia_se()

