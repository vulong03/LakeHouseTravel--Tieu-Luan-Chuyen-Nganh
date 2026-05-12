"""
Partition Utilities for Scratch Layer Processing

Purpose: Helper functions to work with partitioned data in Scratch bucket
- List partitions from filesystem
- Build mapping: partition → source file metadata
- URL encoding/decoding for partition directories
"""

from typing import List, Dict, Tuple
# from urllib.parse import quote, unquote  # DEAD CODE: only used by dead decode_partition_value below
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

def list_scratch_partitions(spark: SparkSession, scratch_path: str) -> List[Tuple[str, str]]:
    """
    Deduplicate post_urls WITHIN the current Scratch run.

    DESIGN DECISION (no cross-run re-crawl):
    We DO NOT re-crawl post_urls that already exist in Silver. This is enforced
    LATER by `filter_unprocessed_partitions`, which skips any post_url already
    present in the Silver table. Reasons:
      - Re-crawl produces hard-to-merge edge cases (spam comments, repeated
        comments from same user across crawls, changing engagement metrics).
      - Current project scale has no link crawled twice in practice.
      - Keeping logic simple > handling rare edge cases.

    WHAT THIS FUNCTION ACTUALLY DOES (intra-run dedup only):
    If the SAME Scratch run somehow contains the same post_url twice
    (e.g., two Bronze files for the same post uploaded in the same run),
    we keep ONLY ONE record — the one with the highest `source_file_checksum`
    (lexicographically latest, treated as "newest"). This is a defensive
    safeguard, NOT a cross-run re-crawl strategy.

    Args:
        spark: SparkSession instance
        scratch_path: Path to Scratch run folder (e.g., s3a://scratch/.../run_20251103_052216)

    Returns:
        List of (post_url, source_file_checksum) tuples - ONE per unique post_url
        within this Scratch run.
        Example: [
            ("https://www.tiktok.com/@user/video/123", "20251031092145"),
            ("https://www.tiktok.com/@user/video/456", "20251030081316"),
            ...
        ]
    """
    try:
        # Read data and get unique (post_url, checksum) combinations
        df = spark.read.parquet(scratch_path)
        
        # Get all unique combinations first (for statistics)
        df_all_combinations = df.select("post_url", "source_file_checksum").distinct()
        total_combinations = df_all_combinations.count()
        
        # Intra-run dedup: if the same Scratch run has the same post_url twice,
        # keep the row with the highest checksum. (Defensive — should be rare.)
        window_spec = Window.partitionBy("post_url").orderBy(F.desc("source_file_checksum"))
        
        df_latest = df_all_combinations \
            .withColumn("rank", F.row_number().over(window_spec)) \
            .filter(F.col("rank") == 1) \
            .select("post_url", "source_file_checksum")
        
        # Collect results
        combinations = [(row.post_url, row.source_file_checksum) for row in df_latest.collect()]
        
        if not combinations:
            raise ValueError(f"No file combinations found in {scratch_path}")
        
        # Statistics
        unique_urls = len(combinations)
        skipped_files = total_combinations - unique_urls
        
        print(f"Found {total_combinations} total (post_url, checksum) combinations in Scratch run")
        print(f"After intra-run dedup: {unique_urls} unique post_urls")
        if skipped_files > 0:
            print(f"Dropped {skipped_files} intra-run duplicate rows (same post_url in same run)")
            print(f"NOTE: Cross-run dedup (skip post_urls already in Silver) happens later")
            print(f"      in filter_unprocessed_partitions() - we DO NOT re-crawl by design.")
        
        return combinations
        
    except Exception as e:
        print(f"Error listing partitions: {e}")
        raise

# ============================================================
# DEAD CODE: decode_partition_value - never called from any module
# ============================================================
# def decode_partition_value(partition_dir: str) -> str:
#     """
#     Decode partition directory name to get original post_url.
#
#     Args:
#         partition_dir: URL-encoded partition directory name
#                       Example: "post_url=https%3A%2F%2Fwww.tiktok.com%2F@user%2Fvideo%2F123"
#
#     Returns:
#         Decoded post_url
#         Example: "https://www.tiktok.com/@user/video/123"
#     """
#     if not partition_dir.startswith("post_url="):
#         raise ValueError(f"Invalid partition directory format: {partition_dir}")
#
#     encoded_url = partition_dir[len("post_url="):]  # Remove "post_url=" prefix
#     decoded_url = unquote(encoded_url)  # URL decode
#     return decoded_url


# ============================================================
# DEAD CODE: encode_partition_value - never called from any module
# ============================================================
# def encode_partition_value(post_url: str) -> str:
#     """
#     Build partition filter for reading data (NOT for directory paths).
#
#     CRITICAL: We don't need to match filesystem encoding anymore.
#     We use post_url directly in Spark filters: .filter(F.col("post_url") == post_url)
#
#     This function kept for backward compatibility but returns the filter format.
#
#     Args:
#         post_url: Original post URL
#                  Example: "https://www.tiktok.com/@user/video/123"
#
#     Returns:
#         Partition filter string
#         Example: "post_url=https://www.tiktok.com/@user/video/123"
#     """
#     return f"post_url={post_url}"


def build_partition_to_file_mapping(
    spark: SparkSession,
    scratch_path: str,
    file_combinations: List[Tuple[str, str]]  # CHANGED: now receives tuples
) -> Dict[str, Dict[str, any]]:
    """
    Build mapping: (post_url, checksum) → source file metadata for tracking.
    
    Read metadata from Scratch data to extract:
    - source_file (Bronze file path)
    - source_file_checksum (for PostgreSQL tracking)
    - source_file_size_bytes (file size)
    
    Args:
        spark: SparkSession instance
        scratch_path: Base Scratch path (run folder)
        file_combinations: List of (post_url, source_file_checksum) tuples (FILTERED by Option A)
    
    Returns:
        Dict mapping composite_key → metadata
        Example: {
            "https://...||20251103052216": {
                "post_url": "https://...",
                "source_file": "tiktok_comments_2025-09-27T10-40-30.csv",
                "source_file_checksum": "20251103052216",
                "source_file_size_bytes": 45678
            },
            ...
        }
    """
    print(f"\nBuilding partition → source file mapping...")
    print(f"Input file combinations: {len(file_combinations)}")
    
    mapping = {}
    
    try:
        # Read ALL metadata from Scratch
        df_metadata = spark.read.parquet(scratch_path) \
            .select("post_url", "source_file", "source_file_checksum", "source_file_size_bytes") \
            .distinct()
        
        # FIX: Filter to keep ONLY the 1415 files selected by list_scratch_partitions()
        # Create DataFrame from file_combinations to use as filter
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("post_url", StringType(), False),
            StructField("source_file_checksum", StringType(), False)
        ])
        
        df_filter = spark.createDataFrame(file_combinations, schema)
        
        # Inner join: Keep only metadata for files in file_combinations
        df_metadata_filtered = df_metadata.join(
            df_filter,
            on=["post_url", "source_file_checksum"],
            how="inner"
        )
        
        # Collect to driver
        metadata_rows = df_metadata_filtered.collect()
        
        # Build mapping using composite key
        for row in metadata_rows:
            composite_key = f"{row.post_url}||{row.source_file_checksum}"
            mapping[composite_key] = {
                "post_url": row.post_url,
                "source_file": row.source_file,
                "source_file_checksum": row.source_file_checksum,
                "source_file_size_bytes": row.source_file_size_bytes
            }
        
        print(f"Mapped {len(mapping)} entries (LATEST files only)")
        
        # Verify count matches
        if len(mapping) != len(file_combinations):
            print(f"WARNING: Mapping count mismatch!")
            print(f"Expected: {len(file_combinations)}, Got: {len(mapping)}")
        
        return mapping
        
    except Exception as e:
        print(f"Error building partition mapping: {e}")
        raise

def filter_unprocessed_partitions(
    spark: SparkSession,
    mapping: Dict[str, Dict[str, any]],
    postgres_conn_params: dict,
    layer: str = 'silver',
    silver_table_posts: str = None
) -> List[Dict[str, any]]:
    """
    Filter out already-processed partitions based on:
    1. PostgreSQL tracking (file_checksum level) — prevents reprocessing
       files whose Step 2 already succeeded.
    2. Silver table (post_url level) — by DESIGN, we DO NOT re-crawl/re-process
       post_urls that already exist in Silver. Re-crawl edge cases (spam,
       comment changes, engagement updates) are intentionally not handled.

    NOTE: This is the layer that enforces the "no re-crawl" policy.
    `list_scratch_partitions` only does intra-run dedup; cross-run skip
    happens here.

    Args:
        spark: SparkSession instance
        mapping: Partition → source file mapping (composite keys: "post_url||checksum")
        postgres_conn_params: PostgreSQL connection params
        layer: Data layer to check ('silver' by default)
        silver_table_posts: Silver table name to check post_url existence (optional)
    
    Returns:
        List of unprocessed metadata dicts (each contains post_url, checksum, etc.)
    """
    from utils.file_tracker import check_if_file_ingested
    
    print(f"\nFiltering unprocessed partitions...")
    
    unprocessed_items = []
    skipped_by_checksum = 0
    skipped_by_post_url = 0
    
    # Get existing post_urls from Silver (if table exists and silver_table_posts provided)
    existing_post_urls = set()
    if silver_table_posts:
        try:
            df_existing = spark.table(silver_table_posts).select("post_url").distinct()
            existing_post_urls = {row.post_url for row in df_existing.collect()}
            print(f"Found {len(existing_post_urls):,} existing post_urls in Silver table")
        except Exception as e:
            print(f"Could not check Silver table (may not exist yet): {e}")
            existing_post_urls = set()
    
    for composite_key, metadata in mapping.items():
        checksum = metadata["source_file_checksum"]
        post_url = metadata["post_url"]
        
        # Check 1: File already logged in PostgreSQL?
        if check_if_file_ingested(checksum, postgres_conn_params, layer):
            skipped_by_checksum += 1
            continue
        
        # Check 2: post_url already exists in Silver?
        if post_url in existing_post_urls:
            skipped_by_post_url += 1
            print(f"Skipping {post_url[:50]}... (already in Silver)")
            continue
        
        # Both checks passed → add to unprocessed
        unprocessed_items.append(metadata)
    
    print(f"Total entries: {len(mapping)}")
    print(f"Skipped by checksum (already logged): {skipped_by_checksum}")
    print(f"Skipped by post_url (already in Silver): {skipped_by_post_url}")
    print(f"To process: {len(unprocessed_items)}")
    
    return unprocessed_items


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
        print(f"Warning: Could not count partition {post_url}: {e}")
        return 0
