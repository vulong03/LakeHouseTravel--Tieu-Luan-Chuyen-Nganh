#!/usr/bin/env python3
"""
Check comments in Scratch for specific post_url
"""
import sys
import os
sys.path.insert(0, '/opt/spark/jobs')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import from jobs/utils
sys.path.insert(0, '/opt/spark/jobs')
from utils.spark_session import get_spark_session

def main():
    # Create Spark session with proper MinIO config
    spark = get_spark_session("Check_Scratch_Comments")

    # Path to latest Scratch comments
    scratch_path = "s3a://scratch/pipeline/silver/tiktok_post_comments/run_20251111_150809"
    
    # Target URL
    target_url = "https://www.tiktok.com/@_nguyen.hoa2403_/video/7545117738658761991"
    
    print("=" * 80)
    print("🔍 CHECKING SCRATCH COMMENTS")
    print("=" * 80)
    print(f"\n📂 Scratch path: {scratch_path}")
    print(f"🎯 Target URL: {target_url}")
    
    # Read all comments
    print("\n📖 Reading Scratch data...")
    df = spark.read.parquet(scratch_path)
    
    # Filter by post_url
    df_post = df.filter(F.col("post_url") == target_url)
    
    comment_count = df_post.count()
    print(f"\n📊 Total comments in Scratch: {comment_count}")
    
    if comment_count == 0:
        print("\n⚠️  No comments found for this URL in Scratch!")
        spark.stop()
        return
    
    # Show schema
    print("\n📋 Schema:")
    df_post.printSchema()
    
    # Show all comments (raw data)
    print(f"\n📋 All {comment_count} comments (raw data from Scratch):")
    print("=" * 80)
    df_post.select("stt", "ten", "comment", "time", "likes").show(comment_count, truncate=False)
    
    # Analyze comment field
    print("\n" + "=" * 80)
    print("🔍 COMMENT FIELD ANALYSIS")
    print("=" * 80)
    
    null_count = df_post.filter(F.col("comment").isNull()).count()
    empty_count = df_post.filter(
        (F.col("comment").isNotNull()) & 
        (F.col("comment") == "")
    ).count()
    whitespace_count = df_post.filter(
        (F.col("comment").isNotNull()) & 
        (F.col("comment") != "") & 
        (F.trim(F.col("comment")) == "")
    ).count()
    valid_count = df_post.filter(
        (F.col("comment").isNotNull()) & 
        (F.trim(F.col("comment")) != "")
    ).count()
    
    print(f"\n📊 Breakdown:")
    print(f"   ❌ NULL comments: {null_count}")
    print(f"   ❌ Empty string (''): {empty_count}")
    print(f"   ❌ Only whitespace: {whitespace_count}")
    print(f"   ✅ Valid (non-empty): {valid_count}")
    print(f"\n   Total: {comment_count}")
    
    # Show samples of each category
    if null_count > 0:
        print(f"\n🔍 Sample NULL comments ({null_count} total):")
        df_post.filter(F.col("comment").isNull()) \
            .select("stt", "ten", "comment", "time", "likes") \
            .show(min(10, null_count), truncate=False)
    
    if empty_count > 0:
        print(f"\n🔍 Sample empty string comments ({empty_count} total):")
        df_post.filter(
            (F.col("comment").isNotNull()) & 
            (F.col("comment") == "")
        ).select("stt", "ten", "comment", "time", "likes") \
            .show(min(10, empty_count), truncate=False)
    
    if whitespace_count > 0:
        print(f"\n🔍 Sample whitespace-only comments ({whitespace_count} total):")
        df_post.filter(
            (F.col("comment").isNotNull()) & 
            (F.col("comment") != "") & 
            (F.trim(F.col("comment")) == "")
        ).select("stt", "ten", "comment", "time", "likes") \
            .show(min(10, whitespace_count), truncate=False)
    
    if valid_count > 0:
        print(f"\n✅ Sample valid comments ({valid_count} total):")
        df_post.filter(
            (F.col("comment").isNotNull()) & 
            (F.trim(F.col("comment")) != "")
        ).select("stt", "ten", "comment", "time", "likes") \
            .show(min(10, valid_count), truncate=False)
    
    # Test filter logic
    print("\n" + "=" * 80)
    print("🧪 TESTING FILTER LOGIC")
    print("=" * 80)
    
    # Apply trim first (like in clean_and_transform_comments)
    df_trimmed = df_post.withColumn("comment", F.trim(F.col("comment")))
    
    # Apply filter
    df_filtered = df_trimmed.filter(
        (F.col("comment").isNotNull()) & 
        (F.col("comment") != "")
    )
    
    filtered_count = df_filtered.count()
    removed_count = comment_count - filtered_count
    
    print(f"\n📊 Filter results:")
    print(f"   Original: {comment_count} comments")
    print(f"   After filter: {filtered_count} comments")
    print(f"   Removed: {removed_count} comments ({removed_count/comment_count*100:.2f}%)")
    
    if filtered_count > 0:
        print(f"\n✅ Comments that would be KEPT after filter:")
        df_filtered.select("stt", "ten", "comment", "time", "likes") \
            .show(min(10, filtered_count), truncate=False)
    else:
        print(f"\n⚠️  ALL comments would be FILTERED OUT!")
    
    print("\n" + "=" * 80)
    print("✅ ANALYSIS COMPLETE")
    print("=" * 80)
    
    spark.stop()

if __name__ == "__main__":
    main()
