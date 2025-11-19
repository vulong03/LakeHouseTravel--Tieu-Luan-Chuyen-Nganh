#!/usr/bin/env python3
"""
Quick script to check raw data in Scratch layer
"""

import sys
sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session

def check_scratch_data():
    spark = get_spark_session(app_name="Check_Scratch_Data")
    
    # Read from Scratch
    scratch_path = "s3a://scratch/pipeline/silver/tiktok_post_metadata/"
    
    print("=" * 80)
    print("📂 Reading from Scratch layer...")
    print(f"   Path: {scratch_path}")
    print("=" * 80)
    
    try:
        df = spark.read.parquet(scratch_path)
        
        print(f"\n✅ Total records in Scratch: {df.count():,}")
        print(f"\n📋 Schema:")
        df.printSchema()
        
        # Check specific records with NULL values in Silver
        print("\n" + "=" * 80)
        print("🔍 Checking records that have NULL in Silver...")
        print("=" * 80)
        
        test_urls = [
            'https://www.tiktok.com/@yeah1.vivu/video/6958751527704071426',
            'https://www.tiktok.com/@yangbaynhatrang/video/7369796824577723656',
            'https://www.tiktok.com/@yeudulich_90/video/7332680374255930642'
        ]
        
        for url in test_urls:
            print(f"\n📄 Post URL: {url}")
            print("-" * 80)
            record = df.filter(df.post_url == url).collect()
            
            if record:
                r = record[0]
                print(f"   author: '{r.author}' (type: {type(r.author).__name__})")
                print(f"   post_date: '{r.post_date}' (type: {type(r.post_date).__name__})")
                print(f"   likes: '{r.likes}' (type: {type(r.likes).__name__})")
                print(f"   saves: '{r.saves}' (type: {type(r.saves).__name__})")
                print(f"   shares: '{r.shares}' (type: {type(r.shares).__name__})")
            else:
                print("   ⚠️  Not found in Scratch")
        
        # Check for "N/A" values
        print("\n" + "=" * 80)
        print("🔍 Checking for 'N/A' values in numeric fields...")
        print("=" * 80)
        
        for col in ['likes', 'saves', 'shares', 'comments_count']:
            na_count = df.filter(df[col] == 'N/A').count()
            if na_count > 0:
                print(f"\n   {col}: {na_count:,} records with 'N/A'")
                # Show sample
                sample = df.filter(df[col] == 'N/A').select('post_url', col).limit(3).collect()
                for s in sample:
                    print(f"      - {s.post_url}: {col} = '{s[col]}'")
        
        # Check for empty strings
        print("\n" + "=" * 80)
        print("🔍 Checking for empty strings in numeric fields...")
        print("=" * 80)
        
        for col in ['likes', 'saves', 'shares', 'comments_count']:
            empty_count = df.filter((df[col] == '') | (df[col].isNull())).count()
            if empty_count > 0:
                print(f"   {col}: {empty_count:,} records with empty/NULL")
        
        # Check for non-numeric values
        print("\n" + "=" * 80)
        print("🔍 Checking for non-numeric values (sample)...")
        print("=" * 80)
        
        for col in ['likes', 'saves', 'shares']:
            # Get distinct non-numeric values
            distinct_vals = df.select(col).distinct().collect()
            non_numeric = []
            for val in distinct_vals:
                v = val[col]
                if v and v != '' and v != 'N/A':
                    # Check if it's a number (plain, K, or M format)
                    import re
                    if not re.match(r'^[\d.]+[KM]?$', v):
                        non_numeric.append(v)
            
            if non_numeric:
                print(f"\n   {col}: Found {len(non_numeric)} non-numeric values:")
                # Count occurrences
                for val in non_numeric:
                    count = df.filter(df[col] == val).count()
                    print(f"      - '{val}': {count:,} records")
                    # Show sample post URLs
                    if count > 0:
                        samples = df.filter(df[col] == val).select('post_url', col).limit(3).collect()
                        for s in samples:
                            print(f"        → {s.post_url}")
        
        # Check specifically for "Chia sẻ" in saves column
        print("\n" + "=" * 80)
        print("🔍 Checking 'Chia sẻ' in saves column...")
        print("=" * 80)
        chia_se_count = df.filter(df.saves == 'Chia sẻ').count()
        if chia_se_count > 0:
            print(f"   Found {chia_se_count:,} records with saves = 'Chia sẻ'")
            samples = df.filter(df.saves == 'Chia sẻ').select('post_url', 'saves', 'shares', 'likes').limit(5).collect()
            print(f"\n   Sample records:")
            for s in samples:
                print(f"      Post: {s.post_url}")
                print(f"        saves: '{s.saves}'")
                print(f"        shares: '{s.shares}'")
                print(f"        likes: '{s.likes}'")
                print()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    check_scratch_data()

