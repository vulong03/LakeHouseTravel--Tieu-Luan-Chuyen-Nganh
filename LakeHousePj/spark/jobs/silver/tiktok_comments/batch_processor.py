"""
Batch Processor for Silver Layer - TikTok Comments

Purpose: Process multiple partitions in batches to reduce Iceberg append operations
Strategy:
  1. Read N partitions (batch size = 30)
  2. Union all DataFrames into single DataFrame
  3. Apply cleaning and transformations
  4. Single APPEND to Iceberg table
  5. Log each file individually to PostgreSQL

Benefits:
  - Reduce Hive Metastore snapshots: 1379 → ~46 (96.7% reduction)
  - Reduce DNS/network overhead
  - Maintain granular file-level tracking
"""

from typing import List, Dict, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import TimestampType

from utils.file_tracker import log_ingestion_to_postgres
from utils.merge_utils import calculate_row_checksum


def read_partitions_batch(
    spark: SparkSession,
    scratch_path: str,
    post_urls: List[str],
    mapping: Dict[str, Dict[str, any]]
) -> Tuple[DataFrame, List[Dict]]:
    """
    Read multiple partitions and union into single DataFrame.
    
    Args:
        spark: SparkSession instance
        scratch_path: Base Scratch path (run folder)
        post_urls: List of post_urls to read (batch)
        mapping: Partition → source file mapping
    
    Returns:
        Tuple of (unioned_df, batch_metadata)
        - unioned_df: Single DataFrame containing all batch data
        - batch_metadata: List of dicts with file tracking info
    """
    print(f"   📖 Reading {len(post_urls)} partitions...")
    
    dataframes = []
    batch_metadata = []
    
    for post_url in post_urls:
        try:
            # Read single partition
            df_partition = spark.read.parquet(scratch_path) \
                .filter(F.col("post_url") == post_url)
            
            # Check if partition has data
            if df_partition.count() > 0:
                dataframes.append(df_partition)
                
                # Store metadata for logging
                metadata = mapping.get(post_url)
                if metadata:
                    batch_metadata.append({
                        "post_url": post_url,
                        "source_file": metadata["source_file"],
                        "source_file_checksum": metadata["source_file_checksum"],
                        "source_file_size_bytes": metadata["source_file_size_bytes"],
                        "record_count": df_partition.count()
                    })
            else:
                print(f"   ⚠️  Empty partition: {post_url}")
        
        except Exception as e:
            print(f"   ⚠️  Error reading partition {post_url}: {e}")
            continue
    
    if not dataframes:
        raise ValueError("No valid partitions found in batch")
    
    # Union all DataFrames
    print(f"   🔗 Unioning {len(dataframes)} DataFrames...")
    df_unioned = dataframes[0]
    for df in dataframes[1:]:
        df_unioned = df_unioned.union(df)
    
    total_records = sum(m["record_count"] for m in batch_metadata)
    print(f"   ✅ Unioned {len(dataframes)} partitions → {total_records:,} records")
    
    return df_unioned, batch_metadata


def process_posts_batch(
    spark: SparkSession,
    scratch_path: str,
    batch_post_urls: List[str],
    mapping: Dict[str, Dict[str, any]],
    silver_table: str,
    business_columns: List[str],
    cleaning_func,
    postgres_conn: dict,
    bronze_base_path: str,
    batch_id: int
) -> Tuple[int, int]:
    """
    Process a batch of posts: Read → Clean → Union → Append → Log.
    
    Args:
        spark: SparkSession instance
        scratch_path: Scratch path for posts
        batch_post_urls: List of post_urls in this batch
        mapping: Partition → source file mapping
        silver_table: Target Silver table name (e.g., "silver.silver.tiktok_post_metadata")
        business_columns: Columns for checksum calculation
        cleaning_func: Function to apply data cleaning (e.g., clean_and_transform_posts)
        postgres_conn: PostgreSQL connection params
        bronze_base_path: Bronze bucket path (for logging)
        batch_id: Batch number (for tracking)
    
    Returns:
        Tuple of (records_loaded, files_skipped)
    """
    print(f"\n{'='*80}")
    print(f"📦 BATCH {batch_id}: Processing {len(batch_post_urls)} posts")
    print(f"{'='*80}")
    
    try:
        # Read and union batch
        df_batch, batch_metadata = read_partitions_batch(
            spark, scratch_path, batch_post_urls, mapping
        )
        
        # Apply cleaning
        print(f"   🧹 Applying data cleaning...")
        df_cleaned = cleaning_func(df_batch)
        
        # Calculate row_checksum
        print(f"   🔐 Calculating row_checksum...")
        df_with_checksum = calculate_row_checksum(df_cleaned, business_columns)
        
        # Update ingestion_timestamp
        df_final = df_with_checksum.withColumn(
            "ingestion_timestamp",
            F.lit(datetime.now()).cast(TimestampType())
        )
        
        # Single APPEND for entire batch
        total_records = df_final.count()
        print(f"   💾 Appending {total_records:,} records to {silver_table}...")
        
        df_final.writeTo(silver_table) \
            .using("iceberg") \
            .append()
        
        print(f"   ✅ Batch appended successfully!")
        
        # Log each file individually to PostgreSQL
        print(f"   📝 Logging {len(batch_metadata)} files to PostgreSQL...")
        
        for file_meta in batch_metadata:
            bronze_file_path = f"{bronze_base_path}/{file_meta['source_file']}"
            
            ingestion_details = {
                "batch_id": batch_id,
                "batch_size": len(batch_post_urls),
                "post_url": file_meta["post_url"],
                "strategy": "Batch processing (30 partitions per batch)",
                "records_in_file": file_meta["record_count"]
            }
            
            log_ingestion_to_postgres(
                file_path=bronze_file_path,
                file_checksum=file_meta["source_file_checksum"],
                records_ingested=file_meta["record_count"],
                table_name=silver_table.split('.')[-1],  # Extract table name
                status="success",
                postgres_conn_params=postgres_conn,
                layer='silver',
                ingestion_details=ingestion_details,
                file_size_bytes=file_meta["source_file_size_bytes"]
            )
        
        print(f"   ✅ All {len(batch_metadata)} files logged")
        
        return total_records, 0
    
    except Exception as e:
        print(f"   ❌ Batch {batch_id} FAILED: {e}")
        
        # Log failed files
        print(f"   📝 Logging {len(batch_post_urls)} files as FAILED...")
        
        for post_url in batch_post_urls:
            metadata = mapping.get(post_url)
            if metadata:
                bronze_file_path = f"{bronze_base_path}/{metadata['source_file']}"
                
                ingestion_details = {
                    "batch_id": batch_id,
                    "batch_size": len(batch_post_urls),
                    "post_url": post_url,
                    "strategy": "Batch processing (30 partitions per batch)",
                    "error": str(e)
                }
                
                log_ingestion_to_postgres(
                    file_path=bronze_file_path,
                    file_checksum=metadata["source_file_checksum"],
                    records_ingested=0,
                    table_name=silver_table.split('.')[-1],
                    status="failed",
                    postgres_conn_params=postgres_conn,
                    layer='silver',
                    error_message=str(e),
                    ingestion_details=ingestion_details,
                    file_size_bytes=metadata["source_file_size_bytes"]
                )
        
        print(f"   📝 Logged {len(batch_post_urls)} failed files")
        
        # Return 0 loaded, all skipped (failed)
        return 0, len(batch_post_urls)


def process_comments_batch(
    spark: SparkSession,
    scratch_path: str,
    batch_post_urls: List[str],
    mapping: Dict[str, Dict[str, any]],
    silver_table: str,
    business_columns: List[str],
    cleaning_func,
    postgres_conn: dict,
    bronze_base_path: str,
    batch_id: int
) -> Tuple[int, int]:
    """
    Process a batch of comments: Read → Clean → Union → Append → Log.
    
    Same logic as process_posts_batch, but for comments table.
    Handles empty partitions (posts with no comments) differently.
    
    Args:
        spark: SparkSession instance
        scratch_path: Scratch path for comments
        batch_post_urls: List of post_urls in this batch
        mapping: Partition → source file mapping
        silver_table: Target Silver table name (e.g., "silver.silver.tiktok_post_comments")
        business_columns: Columns for checksum calculation
        cleaning_func: Function to apply data cleaning (e.g., clean_and_transform_comments)
        postgres_conn: PostgreSQL connection params
        bronze_base_path: Bronze bucket path (for logging)
        batch_id: Batch number (for tracking)
    
    Returns:
        Tuple of (records_loaded, files_skipped)
    """
    print(f"\n{'='*80}")
    print(f"📦 BATCH {batch_id}: Processing {len(batch_post_urls)} comment partitions")
    print(f"{'='*80}")
    
    try:
        # Read and union batch (may have empty partitions)
        valid_dataframes = []
        batch_metadata = []
        empty_partitions = []
        
        print(f"   📖 Reading {len(batch_post_urls)} partitions...")
        
        for post_url in batch_post_urls:
            try:
                df_partition = spark.read.parquet(scratch_path) \
                    .filter(F.col("post_url") == post_url)
                
                count = df_partition.count()
                
                if count > 0:
                    valid_dataframes.append(df_partition)
                    
                    metadata = mapping.get(post_url)
                    if metadata:
                        batch_metadata.append({
                            "post_url": post_url,
                            "source_file": metadata["source_file"],
                            "source_file_checksum": metadata["source_file_checksum"],
                            "source_file_size_bytes": metadata["source_file_size_bytes"],
                            "record_count": count
                        })
                else:
                    # Track empty partition for logging
                    metadata = mapping.get(post_url)
                    if metadata:
                        empty_partitions.append({
                            "post_url": post_url,
                            "source_file": metadata["source_file"],
                            "source_file_checksum": metadata["source_file_checksum"],
                            "source_file_size_bytes": metadata["source_file_size_bytes"]
                        })
            
            except Exception as e:
                print(f"   ⚠️  Error reading partition {post_url}: {e}")
                continue
        
        # Log empty partitions as skipped
        if empty_partitions:
            print(f"   📝 Logging {len(empty_partitions)} empty partitions as skipped...")
            
            for empty_meta in empty_partitions:
                bronze_file_path = f"{bronze_base_path}/{empty_meta['source_file']}"
                
                ingestion_details = {
                    "batch_id": batch_id,
                    "batch_size": len(batch_post_urls),
                    "post_url": empty_meta["post_url"],
                    "strategy": "Batch processing (30 partitions per batch)",
                    "skip_reason": "No comments in partition"
                }
                
                log_ingestion_to_postgres(
                    file_path=bronze_file_path,
                    file_checksum=empty_meta["source_file_checksum"],
                    records_ingested=0,
                    table_name=silver_table.split('.')[-1],
                    status="success",
                    postgres_conn_params=postgres_conn,
                    layer='silver',
                    ingestion_details=ingestion_details,
                    file_size_bytes=empty_meta["source_file_size_bytes"]
                )
        
        # If no valid data, return early
        if not valid_dataframes:
            print(f"   ⏭️  Batch {batch_id}: All partitions empty - skipping")
            return 0, len(empty_partitions)
        
        # Union valid DataFrames
        print(f"   🔗 Unioning {len(valid_dataframes)} DataFrames...")
        df_unioned = valid_dataframes[0]
        for df in valid_dataframes[1:]:
            df_unioned = df_unioned.union(df)
        
        total_records = sum(m["record_count"] for m in batch_metadata)
        print(f"   ✅ Unioned {len(valid_dataframes)} partitions → {total_records:,} records")
        
        # Apply cleaning
        print(f"   🧹 Applying data cleaning...")
        df_cleaned = cleaning_func(df_unioned)
        
        # Calculate row_checksum
        print(f"   🔐 Calculating row_checksum...")
        df_with_checksum = calculate_row_checksum(df_cleaned, business_columns)
        
        # Update ingestion_timestamp
        df_final = df_with_checksum.withColumn(
            "ingestion_timestamp",
            F.lit(datetime.now()).cast(TimestampType())
        )
        
        # Single APPEND for entire batch
        final_count = df_final.count()
        print(f"   💾 Appending {final_count:,} records to {silver_table}...")
        
        df_final.writeTo(silver_table) \
            .using("iceberg") \
            .append()
        
        print(f"   ✅ Batch appended successfully!")
        
        # Log each file with data to PostgreSQL
        print(f"   📝 Logging {len(batch_metadata)} files to PostgreSQL...")
        
        for file_meta in batch_metadata:
            bronze_file_path = f"{bronze_base_path}/{file_meta['source_file']}"
            
            ingestion_details = {
                "batch_id": batch_id,
                "batch_size": len(batch_post_urls),
                "post_url": file_meta["post_url"],
                "strategy": "Batch processing (30 partitions per batch)",
                "records_in_file": file_meta["record_count"]
            }
            
            log_ingestion_to_postgres(
                file_path=bronze_file_path,
                file_checksum=file_meta["source_file_checksum"],
                records_ingested=file_meta["record_count"],
                table_name=silver_table.split('.')[-1],
                status="success",
                postgres_conn_params=postgres_conn,
                layer='silver',
                ingestion_details=ingestion_details,
                file_size_bytes=file_meta["source_file_size_bytes"]
            )
        
        print(f"   ✅ All {len(batch_metadata)} files logged")
        
        return final_count, len(empty_partitions)
    
    except Exception as e:
        print(f"   ❌ Batch {batch_id} FAILED: {e}")
        
        # Log all files as failed
        print(f"   📝 Logging {len(batch_post_urls)} files as FAILED...")
        
        for post_url in batch_post_urls:
            metadata = mapping.get(post_url)
            if metadata:
                bronze_file_path = f"{bronze_base_path}/{metadata['source_file']}"
                
                ingestion_details = {
                    "batch_id": batch_id,
                    "batch_size": len(batch_post_urls),
                    "post_url": post_url,
                    "strategy": "Batch processing (30 partitions per batch)",
                    "error": str(e)
                }
                
                log_ingestion_to_postgres(
                    file_path=bronze_file_path,
                    file_checksum=metadata["source_file_checksum"],
                    records_ingested=0,
                    table_name=silver_table.split('.')[-1],
                    status="failed",
                    postgres_conn_params=postgres_conn,
                    layer='silver',
                    error_message=str(e),
                    ingestion_details=ingestion_details,
                    file_size_bytes=metadata["source_file_size_bytes"]
                )
        
        print(f"   📝 Logged {len(batch_post_urls)} failed files")
        
        return 0, len(batch_post_urls)


def create_batches(post_urls: List[str], batch_size: int) -> List[List[str]]:
    """
    Split list of post_urls into batches.
    
    Args:
        post_urls: Full list of post_urls to process
        batch_size: Number of partitions per batch
    
    Returns:
        List of batches (each batch is a list of post_urls)
    """
    batches = []
    for i in range(0, len(post_urls), batch_size):
        batch = post_urls[i:i + batch_size]
        batches.append(batch)
    
    print(f"\n📦 Created {len(batches)} batches (batch size: {batch_size})")
    print(f"   Total partitions: {len(post_urls)}")
    print(f"   Full batches: {len([b for b in batches if len(b) == batch_size])}")
    print(f"   Last batch size: {len(batches[-1])}")
    
    return batches
