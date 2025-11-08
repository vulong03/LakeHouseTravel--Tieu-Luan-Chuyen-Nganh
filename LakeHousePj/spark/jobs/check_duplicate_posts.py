"""
Script to check for duplicate posts in Silver layer
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
import sys

def main():
    print("=" * 60)
    print("   CHECKING FOR DUPLICATE POSTS")
    print("=" * 60)
    print()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Check Duplicate Posts") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://silver/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # Use silver database
        spark.sql("USE silver")
        
        print("📊 TIKTOK POST METADATA ANALYSIS")
        print("-" * 60)
        
        df = spark.table("silver.tiktok_post_metadata")
        
        # Total records
        total_records = df.count()
        print(f"Total records: {total_records:,}")
        
        # Unique post URLs
        unique_posts = df.select("post_url").distinct().count()
        print(f"Unique post_url: {unique_posts:,}")
        
        # Unique source files
        unique_files = df.select("source_file").distinct().count()
        print(f"Unique source_file: {unique_files:,}")
        
        # Unique checksums
        unique_checksums = df.select("source_file_checksum").distinct().count()
        print(f"Unique source_file_checksum: {unique_checksums:,}")
        
        print()
        print("🔍 DUPLICATE ANALYSIS:")
        print("-" * 60)
        
        # Check duplicates by post_url
        duplicate_posts = df.groupBy("post_url").agg(count("*").alias("count")) \
            .filter(col("count") > 1) \
            .orderBy(col("count").desc())
        
        duplicate_count = duplicate_posts.count()
        print(f"Number of post_url with duplicates: {duplicate_count:,}")
        
        if duplicate_count > 0:
            print("\nTop 10 most duplicated posts:")
            duplicate_posts.show(10, truncate=False)
            
            # Show one example of duplicate
            print("\n📋 Example of duplicate records for one post:")
            first_dup = duplicate_posts.first()
            if first_dup:
                example_url = first_dup['post_url']
                df.filter(col("post_url") == example_url) \
                    .select("post_url", "source_file", "source_file_checksum", "ingestion_timestamp") \
                    .show(truncate=False)
        
        print()
        print("📁 SOURCE FILE ANALYSIS:")
        print("-" * 60)
        
        # Check records per source file
        records_per_file = df.groupBy("source_file").agg(count("*").alias("count")) \
            .orderBy(col("count").desc())
        
        print(f"Total unique source files: {records_per_file.count():,}")
        print("\nTop 10 files with most records:")
        records_per_file.show(10, truncate=False)
        
        # Check if same file appears multiple times with different checksums
        print()
        print("🔐 CHECKSUM VERIFICATION:")
        print("-" * 60)
        
        file_checksum_combos = df.groupBy("source_file", "source_file_checksum") \
            .agg(count("*").alias("count")) \
            .orderBy(col("count").desc())
        
        print(f"Unique file+checksum combinations: {file_checksum_combos.count():,}")
        print("\nTop 10 file+checksum combinations:")
        file_checksum_combos.show(10, truncate=False)
        
        print()
        print("=" * 60)
        print("✅ Analysis completed!")
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
