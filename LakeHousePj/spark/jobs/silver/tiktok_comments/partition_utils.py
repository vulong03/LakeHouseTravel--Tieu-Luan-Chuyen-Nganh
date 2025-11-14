"""
Partition Utilities for Scratch Layer Processing

Purpose: Helper functions to work with partitioned data in Scratch bucket
- List partitions from filesystem
- Build mapping: partition → source file metadata
- URL encoding/decoding for partition directories
"""

from typing import List, Dict, Tuple
from urllib.parse import quote, unquote
from pyspark.sql import SparkSession, functions as F


def list_scratch_partitions(spark: SparkSession, scratch_path: str) -> List[str]:
    """
    List all unique post_urls from Scratch data (NOT from filesystem).
    
    CRITICAL: We read from DATA instead of listing directories because:
    - Spark's partitionBy() uses its own URL encoding (different from urllib.quote)
    - Reading data ensures we get exact post_url values as stored
    - No encoding/decoding mismatch issues
    
    Args:
        spark: SparkSession instance
        scratch_path: Path to Scratch run folder (e.g., s3a://scratch/.../run_20251103_052216)
    
    Returns:
        List of post_url strings (as stored in data)
        Example: ["https://www.tiktok.com/@user/video/123", ...]
    """
    try:
        # Read data and get unique post_urls (fast with partition pruning)
        df = spark.read.parquet(scratch_path)
        
        # Get distinct post_urls
        post_urls = [row.post_url for row in df.select("post_url").distinct().collect()]
        
        if not post_urls:
            raise ValueError(f"No post_urls found in {scratch_path}")
        
        print(f"📂 Found {len(post_urls)} unique post_urls in Scratch data")
        return post_urls
        
    except Exception as e:
        print(f"❌ Error listing partitions: {e}")
        raise


def decode_partition_value(partition_dir: str) -> str:
    """
    Decode partition directory name to get original post_url.
    
    Args:
        partition_dir: URL-encoded partition directory name
                      Example: "post_url=https%3A%2F%2Fwww.tiktok.com%2F@user%2Fvideo%2F123"
    
    Returns:
        Decoded post_url
        Example: "https://www.tiktok.com/@user/video/123"
    """
    if not partition_dir.startswith("post_url="):
        raise ValueError(f"Invalid partition directory format: {partition_dir}")
    
    encoded_url = partition_dir[len("post_url="):]  # Remove "post_url=" prefix
    decoded_url = unquote(encoded_url)  # URL decode
    return decoded_url


def encode_partition_value(post_url: str) -> str:
    """
    Build partition filter for reading data (NOT for directory paths).
    
    CRITICAL: We don't need to match filesystem encoding anymore.
    We use post_url directly in Spark filters: .filter(F.col("post_url") == post_url)
    
    This function kept for backward compatibility but returns the filter format.
    
    Args:
        post_url: Original post URL
                 Example: "https://www.tiktok.com/@user/video/123"
    
    Returns:
        Partition filter string
        Example: "post_url=https://www.tiktok.com/@user/video/123"
    """
    return f"post_url={post_url}"


def build_partition_to_file_mapping(
    spark: SparkSession,
    scratch_path: str,
    post_urls: List[str]
) -> Dict[str, Dict[str, any]]:
    """
    Build mapping: post_url → source file metadata for tracking.
    
    Read metadata from Scratch data to extract:
    - source_file (Bronze file path)
    - source_file_checksum (for PostgreSQL tracking)
    - source_file_size_bytes (file size)
    
    Args:
        spark: SparkSession instance
        scratch_path: Base Scratch path (run folder)
        post_urls: List of post_url strings (raw URLs from list_scratch_partitions)
    
    Returns:
        Dict mapping post_url → metadata
        Example: {
            "https://www.tiktok.com/@user/video/123": {
                "partition_dir": "post_url=https%3A%2F%2F...",
                "source_file": "tiktok_comments_2025-09-27T10-40-30.csv",
                "source_file_checksum": "20251103052216",
                "source_file_size_bytes": 45678
            },
            ...
        }
    """
    print(f"\n📋 Building partition → source file mapping...")
    print(f"   Input partition count: {len(post_urls)}")
    
    mapping = {}
    
    try:
        # Read metadata columns from all partitions at once (more efficient)
        df_metadata = spark.read.parquet(scratch_path) \
            .select("post_url", "source_file", "source_file_checksum", "source_file_size_bytes") \
            .distinct()
        
        # DEBUG: Check for duplicates in data
        total_rows = df_metadata.count()
        unique_urls = df_metadata.select("post_url").distinct().count()
        
        print(f"   🔍 DEBUG: Total metadata rows: {total_rows}")
        print(f"   🔍 DEBUG: Unique post_urls in data: {unique_urls}")
        
        if total_rows != unique_urls:
            duplicate_count = total_rows - unique_urls
            print(f"   ⚠️  WARNING: Found {duplicate_count} duplicate post_urls in Scratch data!")
            
            # Show duplicates
            df_duplicates = df_metadata.groupBy("post_url").count().filter(F.col("count") > 1)
            dup_urls = df_duplicates.count()
            print(f"   📋 {dup_urls} post_urls have duplicates:")
            df_duplicates.orderBy(F.desc("count")).show(10, truncate=False)
        
        # Collect to driver (small dataset)
        metadata_rows = df_metadata.collect()
        
        for row in metadata_rows:
            post_url = row.post_url
            mapping[post_url] = {
                "source_file": row.source_file,
                "source_file_checksum": row.source_file_checksum,
                "source_file_size_bytes": row.source_file_size_bytes
            }
        
        print(f"   ✅ Mapped {len(mapping)} unique post_urls to source files")
        
        # Validate: All input post_urls should be in mapping
        input_urls = set(post_urls)
        mapped_urls = set(mapping.keys())
        missing = input_urls - mapped_urls
        
        if missing:
            print(f"   ⚠️  WARNING: {len(missing)} partitions exist but NOT found in data!")
            print(f"   📋 Sample missing post_urls:")
            for url in list(missing)[:10]:
                print(f"      - {url}")
            
            # These partitions might be empty or corrupt
            print(f"   ℹ️  These partitions will be SKIPPED (no metadata available)")
        
        # Show extra URLs in data but not in partition list
        extra = mapped_urls - input_urls
        if extra:
            print(f"   ⚠️  WARNING: {len(extra)} post_urls in data but NOT in partition list!")
            print(f"   📋 Sample extra post_urls:")
            for url in list(extra)[:10]:
                print(f"      - {url}")
        
        return mapping
        
    except Exception as e:
        print(f"❌ Error building partition mapping: {e}")
        raise


def filter_unprocessed_partitions(
    spark: SparkSession,
    mapping: Dict[str, Dict[str, any]],
    postgres_conn_params: dict,
    layer: str = 'silver'
) -> List[str]:
    """
    Filter out already-processed partitions based on PostgreSQL tracking.
    
    Args:
        spark: SparkSession instance
        mapping: Partition → source file mapping (from build_partition_to_file_mapping)
        postgres_conn_params: PostgreSQL connection params
        layer: Data layer to check ('silver' by default)
    
    Returns:
        List of unprocessed post_urls
    """
    from utils.file_tracker import check_if_file_ingested
    
    print(f"\n🔍 Filtering unprocessed partitions...")
    
    unprocessed_urls = []
    skipped_count = 0
    
    for post_url, metadata in mapping.items():
        checksum = metadata["source_file_checksum"]
        
        if check_if_file_ingested(checksum, postgres_conn_params, layer):
            skipped_count += 1
        else:
            unprocessed_urls.append(post_url)
    
    print(f"   Total partitions: {len(mapping)}")
    print(f"   Already processed: {skipped_count}")
    print(f"   To process: {len(unprocessed_urls)}")
    
    return unprocessed_urls


def get_partition_count(spark: SparkSession, scratch_path: str, post_url: str) -> int:
    """
    Get record count for a specific partition.
    
    Args:
        spark: SparkSession instance
        scratch_path: Base Scratch path
        post_url: Post URL to count
    
    Returns:
        Number of records in partition
    """
    try:
        df = spark.read.parquet(scratch_path).filter(F.col("post_url") == post_url)
        return df.count()
    except Exception as e:
        print(f"⚠️  Warning: Could not count partition {post_url}: {e}")
        return 0
