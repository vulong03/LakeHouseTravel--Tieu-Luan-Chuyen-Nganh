"""
Per-File Processor for Silver Layer - TikTok Comments

Purpose: Process a single post_url (1 file) completely:
  1. Read partition for posts
  2. Clean & append posts (with duplicate check)
  3. Read partition for comments
  4. Clean & append comments
  5. Log 1 entry to PostgreSQL (atomic - success only if BOTH OK)

Benefits:
  - Atomic per-file processing
  - Simple error handling (1 file fail doesn't affect others)
  - Transparent logging (1 entry per file)
  - Easy rerun (only failed files reprocessed)
"""

from typing import Tuple, Dict
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import TimestampType

from utils.file_tracker import log_ingestion_to_postgres
from utils.merge_utils import calculate_row_checksum

def process_single_post_url(
    spark: SparkSession,
    post_url: str,
    file_meta: Dict[str, any],
    scratch_path_posts: str,
    scratch_path_comments: str,
    silver_table_posts: str,
    silver_table_comments: str,
    business_columns_posts: list,
    business_columns_comments: list,
    cleaning_func_posts,
    cleaning_func_comments,
    postgres_conn: dict,
    bronze_base_path: str,
    batch_id: int
) -> Tuple[int, int, str, str]:
    """
    Process BOTH posts and comments for a single post_url.
    Log 1 entry after completing both.
    
    Args:
        spark: SparkSession instance
        post_url: Post URL to process
        file_meta: File metadata dict with source_file, checksum, size
        scratch_path_posts: Scratch path for posts
        scratch_path_comments: Scratch path for comments
        silver_table_posts: Target Silver table for posts
        silver_table_comments: Target Silver table for comments
        business_columns_posts: Columns for posts checksum
        business_columns_comments: Columns for comments checksum
        cleaning_func_posts: Function to clean posts data
        cleaning_func_comments: Function to clean comments data
        postgres_conn: PostgreSQL connection params
        bronze_base_path: Bronze bucket path (for logging)
        batch_id: Batch number (for tracking)
    
    Returns:
        Tuple of (posts_count, comments_count, status, error_msg)
        - posts_count: Number of posts appended (0 or 1)
        - comments_count: Number of comments appended
        - status: 'success' or 'failed'
        - error_msg: Error message if failed, None if success
    """
    file_name = file_meta['source_file']
    file_checksum = file_meta['source_file_checksum']
    file_size = file_meta['source_file_size_bytes']
    
    print(f"\nProcessing: {file_name}")
    print(f"Post URL: {post_url[:80]}...")
    
    posts_count = 0
    comments_count = 0
    error_msg = None
    
    try:
        # ============================================================
        # STEP 1: Process POST
        # ============================================================
        print(f"Processing post metadata...")
        
        # Read partition using data filter (NOT directory path)
        # This avoids URL encoding mismatch between Spark and Python
        df_post = spark.read.parquet(scratch_path_posts) \
            .filter(F.col("post_url") == post_url)
        
        # Clean & transform
        df_post_cleaned = cleaning_func_posts(df_post)
        
        # Check duplicate (prevent reprocessing if post already exists)
        existing = spark.table(silver_table_posts) \
            .filter(F.col("post_url") == post_url) \
            .count()
        
        if existing == 0:
            # Calculate row checksum
            df_post_with_checksum = calculate_row_checksum(
                df_post_cleaned,
                business_columns_posts
            )
            
            # Add source file metadata
            df_post_final = df_post_with_checksum \
                .withColumn("source_file_checksum", F.lit(file_checksum)) \
                .withColumn("source_file", F.lit(file_name)) \
                .withColumn("ingestion_timestamp", F.lit(datetime.now()).cast(TimestampType()))
            
            # Append
            df_post_final.writeTo(silver_table_posts) \
                .using("iceberg") \
                .append()
            
            posts_count = 1
            print(f"Post appended")
        else:
            posts_count = 0
            print(f"Post already exists in Silver (skipped)")
        
        # ============================================================
        # STEP 2: Process COMMENTS
        # ============================================================
        print(f"Processing comments...")
        
        # Read partition using data filter (NOT directory path)
        df_comments = spark.read.parquet(scratch_path_comments) \
            .filter(F.col("post_url") == post_url)
        comments_count_raw = df_comments.count()
        
        if comments_count_raw == 0:
            print(f"No comments for this post")
            comments_count = 0
        else:
            # Clean & transform
            df_comments_cleaned = cleaning_func_comments(df_comments)
            
            # Calculate row checksum
            df_comments_with_checksum = calculate_row_checksum(
                df_comments_cleaned,
                business_columns_comments
            )
            
            # Add source file metadata
            df_comments_final = df_comments_with_checksum \
                .withColumn("source_file_checksum", F.lit(file_checksum)) \
                .withColumn("source_file", F.lit(file_name)) \
                .withColumn("ingestion_timestamp", F.lit(datetime.now()).cast(TimestampType()))
            
            # Repartition and append
            df_comments_final.repartition("post_url") \
                .writeTo(silver_table_comments) \
                .using("iceberg") \
                .append()
            
            comments_count = df_comments_final.count()
            print(f"{comments_count} comments appended")
        
        # ============================================================
        # STEP 3: LOG SUCCESS (Both tables processed successfully)
        # ============================================================
        status = 'success'
        
        ingestion_details = {
            "tables": [
                {"name": silver_table_posts.split(".")[-1], "status": "success", "records": posts_count},
                {"name": silver_table_comments.split(".")[-1], "status": "success", "records": comments_count}
            ],
            "post_url": post_url,
            "source_size_bytes": file_size,
            "batch_id": batch_id
        }
        
        try:
            log_ingestion_to_postgres(
                file_path=f"{bronze_base_path}/{file_name}",
                file_checksum=file_checksum,
                records_ingested=posts_count + comments_count,
                table_name=f"{silver_table_posts.split('.')[-1]} + {silver_table_comments.split('.')[-1]}",
                status=status,
                layer='silver',
                ingestion_details=ingestion_details,
                file_size_bytes=file_size,
                postgres_conn_params=postgres_conn
            )
            print(f"Completed: {posts_count} post + {comments_count} comments (logged to PostgreSQL)")
        except Exception as log_error:
            print(f"WARNING: Processing succeeded but logging failed: {log_error}")
            print(f"Data is in Iceberg but NOT tracked in PostgreSQL!")
            # Don't fail - data already in Iceberg
        
        return posts_count, comments_count, status, None
    
    except Exception as e:
        # ============================================================
        # STEP 4: LOG FAILURE (Either step failed)
        # ============================================================
        error_msg = str(e)
        print(f"Failed: {error_msg}")
        
        # Determine which step failed
        if posts_count == 0 and comments_count == 0:
            failure_reason = "Posts processing failed"
        else:
            failure_reason = "Comments processing failed (posts already appended)"
        
        ingestion_details = {
            "tables": [
                {"name": silver_table_posts.split(".")[-1], "status": "success" if posts_count > 0 else "failed", "records": posts_count},
                {"name": silver_table_comments.split(".")[-1], "status": "failed", "records": 0}
            ],
            "post_url": post_url,
            "source_size_bytes": file_size,
            "batch_id": batch_id,
            "failure_reason": failure_reason
        }
        
        # Log failed
        try:
            log_ingestion_to_postgres(
                file_path=f"{bronze_base_path}/{file_name}",
                file_checksum=file_checksum,
                records_ingested=0,
                table_name=f"{silver_table_posts.split('.')[-1]} + {silver_table_comments.split('.')[-1]}",
                status='failed',
                layer='silver',
                error_message=error_msg,
                ingestion_details=ingestion_details,
                file_size_bytes=file_size,
                postgres_conn_params=postgres_conn
            )
        except Exception as log_error:
            print(f"WARNING: Failed to log to PostgreSQL: {log_error}")
            # Continue anyway - don't fail the whole batch because of logging issue
        
        return posts_count, comments_count, 'failed', error_msg

def create_batches(items: list, batch_size: int) -> list:
    """
    Split list of items into batches for organized processing.
    
    Args:
        items: Full list of items (metadata dicts) to process
        batch_size: Number of items per batch
    
    Returns:
        List of batches (each batch is a list of items)
    """
    batches = []
    for i in range(0, len(items), batch_size):
        batches.append(items[i:i+batch_size])
    
    print(f"\nCreated {len(batches)} batches (batch size: {batch_size})")
    print(f"Total items: {len(items)}")
    print(f"Full batches: {len([b for b in batches if len(b) == batch_size])}")
    if batches:
        print(f"Last batch size: {len(batches[-1])}")
    
    return batches
