#!/usr/bin/env python3
"""
Trace comment data back to original CSV file
Check data quality at each layer: Scratch → Bronze → CSV
"""
import sys
import os
sys.path.insert(0, '/opt/spark/jobs')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.spark_session import get_spark_session

def main():
    spark = get_spark_session("Trace_Comment_Source")
    
    target_url = "https://www.tiktok.com/@halleygoround/video/7344974438342905089"
    
    print("=" * 80)
    print("🔍 TRACING COMMENT DATA SOURCE")
    print("=" * 80)
    print(f"\n🎯 Target URL: {target_url}")
    
    # ========================================================================
    # LAYER 1: SCRATCH (Parquet)
    # ========================================================================
    print("\n" + "=" * 80)
    print("📂 LAYER 1: SCRATCH (Parquet - after Step 1)")
    print("=" * 80)
    
    # Find latest Scratch run folder dynamically
    scratch_base = "s3a://scratch/pipeline/silver/tiktok_post_comments"
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(scratch_base),
        hadoop_conf
    )
    base_path = spark._jvm.org.apache.hadoop.fs.Path(scratch_base)
    
    if not fs.exists(base_path):
        print(f"❌ Scratch path not found: {scratch_base}")
        return
    
    # List all run_* folders
    run_folders = []
    status_list = fs.listStatus(base_path)
    for status in status_list:
        path = str(status.getPath())
        folder_name = path.split("/")[-1]
        if folder_name.startswith("run_"):
            run_folders.append(folder_name)
    
    if not run_folders:
        print(f"❌ No run folders found in {scratch_base}")
        return
    
    # Get latest run
    latest_run = sorted(run_folders)[-1]
    scratch_path = f"{scratch_base}/{latest_run}"
    
    print(f"\n📁 Found {len(run_folders)} run(s)")
    print(f"🔍 Latest run: {latest_run}")
    print(f"📂 Path: {scratch_path}")
    
    df_scratch = spark.read.parquet(scratch_path)
    df_scratch_filtered = df_scratch.filter(F.col("post_url") == target_url)
    
    scratch_count = df_scratch_filtered.count()
    print(f"📊 Total comments: {scratch_count}")
    
    # Get source file info
    source_files = df_scratch_filtered.select("source_file", "source_file_checksum").distinct().collect()
    
    if len(source_files) > 0:
        source_file = source_files[0]["source_file"]
        source_checksum = source_files[0]["source_file_checksum"]
        print(f"\n📄 Source file: {source_file}")
        print(f"🔐 Checksum: {source_checksum}")
        
        # Show sample from Scratch
        print(f"\n📋 Sample Scratch data (first 10 comments):")
        df_scratch_filtered.select(
            "stt", "ten", "comment", "time", "likes", "level_comment"
        ).show(10, truncate=False)
        
        # Analyze comment field in Scratch
        empty_scratch = df_scratch_filtered.filter(
            (F.col("comment").isNull()) | (F.col("comment") == "")
        ).count()
        valid_scratch = df_scratch_filtered.filter(
            (F.col("comment").isNotNull()) & (F.col("comment") != "")
        ).count()
        
        print(f"\n📊 Scratch comment analysis:")
        print(f"   Empty/NULL: {empty_scratch}")
        print(f"   Valid (non-empty): {valid_scratch}")
        
        # ========================================================================
        # LAYER 2: BRONZE (Parquet from CSV)
        # ========================================================================
        print("\n" + "=" * 80)
        print("📂 LAYER 2: BRONZE (Parquet - converted from CSV)")
        print("=" * 80)
        
        bronze_path = f"s3a://bronze/raw/tiktok/comments/{source_file}"
        print(f"\n📁 Path: {bronze_path}")
        
        try:
            df_bronze = spark.read.parquet(bronze_path)
            
            # Bronze doesn't have post_url, filter by level_comment != "0" (comments only)
            df_bronze_comments = df_bronze.filter(F.col("level_comment") != "0")
            
            bronze_count = df_bronze_comments.count()
            print(f"📊 Total comments in Bronze: {bronze_count}")
            
            # Show sample from Bronze
            print(f"\n📋 Sample Bronze data (first 10 comments):")
            df_bronze_comments.select(
                "stt", "ten", "comment", "time", "likes", "level_comment", "url"
            ).show(10, truncate=False)
            
            # Analyze comment field in Bronze
            empty_bronze = df_bronze_comments.filter(
                (F.col("comment").isNull()) | (F.col("comment") == "")
            ).count()
            valid_bronze = df_bronze_comments.filter(
                (F.col("comment").isNotNull()) & (F.col("comment") != "")
            ).count()
            
            print(f"\n📊 Bronze comment analysis:")
            print(f"   Empty/NULL: {empty_bronze}")
            print(f"   Valid (non-empty): {valid_bronze}")
            
            # Check if URL matches
            urls_in_bronze = df_bronze_comments.select("url").distinct().collect()
            print(f"\n🔗 URLs found in Bronze:")
            for row in urls_in_bronze[:5]:
                print(f"   {row['url']}")
            
        except Exception as e:
            print(f"⚠️  Cannot read Bronze: {e}")
        
        # ========================================================================
        # LAYER 3: CSV ORIGINAL
        # ========================================================================
        print("\n" + "=" * 80)
        print("📂 LAYER 3: CSV ORIGINAL (raw crawler output)")
        print("=" * 80)
        
        # Try to find CSV file (might have different naming)
        csv_filename = source_file.replace('.parquet', '.csv')
        csv_path = f"s3a://bronze/raw/tiktok/comments/{csv_filename}"
        
        print(f"\n📁 Looking for CSV: {csv_filename}")
        
        # List files in Bronze to find the actual CSV
        print(f"\n🔍 Listing files in Bronze directory...")
        
        try:
            # Use hadoop fs to list
            import subprocess
            result = subprocess.run(
                ["docker", "exec", "lakehouse_minio", "mc", "ls", 
                 "local/bronze/raw/tiktok/comments/"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                files = result.stdout.strip().split('\n')
                matching_files = [f for f in files if target_url.split('/')[-1] in f or source_checksum[:8] in f]
                
                if matching_files:
                    print(f"\n📄 Found matching files:")
                    for f in matching_files:
                        print(f"   {f}")
                else:
                    print(f"\n⚠️  No matching CSV files found")
                    print(f"\n📋 Sample files in Bronze:")
                    for f in files[:10]:
                        print(f"   {f}")
        except Exception as e:
            print(f"⚠️  Cannot list Bronze files: {e}")
        
        # Try direct CSV read
        try:
            print(f"\n📖 Attempting to read CSV directly...")
            
            # Try multiple possible paths
            possible_paths = [
                f"s3a://bronze/raw/tiktok/comments/{csv_filename}",
                f"s3a://bronze/raw/tiktok/comments/tiktok_comments_{source_checksum[:8]}.csv"
            ]
            
            csv_found = False
            for path in possible_paths:
                try:
                    print(f"\n   Trying: {path}")
                    df_csv = spark.read.option("header", "true").csv(path)
                    
                    csv_count = df_csv.count()
                    print(f"   ✅ Success! Total rows: {csv_count}")
                    
                    # Show sample
                    print(f"\n   📋 Sample CSV data (first 10 rows):")
                    df_csv.select("stt", "ten", "comment", "time", "likes").show(10, truncate=False)
                    
                    # Analyze comment field in CSV
                    empty_csv = df_csv.filter(
                        (F.col("comment").isNull()) | (F.col("comment") == "")
                    ).count()
                    valid_csv = df_csv.filter(
                        (F.col("comment").isNotNull()) & (F.col("comment") != "")
                    ).count()
                    
                    print(f"\n   📊 CSV comment analysis:")
                    print(f"      Empty/NULL: {empty_csv}")
                    print(f"      Valid (non-empty): {valid_csv}")
                    
                    csv_found = True
                    break
                except Exception as e:
                    print(f"   ❌ Not found")
            
            if not csv_found:
                print(f"\n⚠️  CSV file not accessible via Spark")
                
        except Exception as e:
            print(f"⚠️  Cannot read CSV: {e}")
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "=" * 80)
    print("📊 SUMMARY")
    print("=" * 80)
    
    print(f"""
Data flow for URL: {target_url}

CSV (Original)     → Bronze (Parquet) → Scratch (Parquet)
   ???                   ???                 {scratch_count} comments
                                         {empty_scratch} empty
                                         {valid_scratch} valid

Conclusion:
    """)
    
    if valid_scratch == 0 and empty_scratch == scratch_count:
        print("❌ ALL comments are empty in Scratch")
        print("   → Need to check Bronze and CSV to find where data was lost!")
    else:
        print("✅ Data looks good in Scratch")
    
    print("\n" + "=" * 80)
    
    spark.stop()

if __name__ == "__main__":
    main()
