"""
Step 1: Transform - TikTok Comments (Bronze → Scratch)

Purpose: Parse special CSV format and preserve ALL data with minimal transformation
Strategy:
  - List all Bronze CSV files (batch processing)
  - Check if file already processed (PostgreSQL tracking with checksum)
  - Parse each file:
    * Lines 1-17: Post metadata (extract key-value pairs)
    * Lines 18+: Comment CSV data (handle commas in text with proper quoting)
  - Add metadata: source_file, source_file_checksum, source_file_size_bytes
  - Write to 2 Scratch buckets as Parquet:
    * s3a://scratch/.../tiktok_post_metadata/run_YYYYMMDD_HHMMSS/
    * s3a://scratch/.../tiktok_post_comments/run_YYYYMMDD_HHMMSS/
  - NO deduplication, NO data cleaning (preserve Bronze as-is)

Input: Bronze CSV files (s3a://bronze/lakehouse/tiktok_comments/raw/*.csv)
Output: Scratch Parquet files (2 separate folders for posts + comments)
"""

import sys
import os
import re
import csv
import io

sys.path.append('/opt/spark/jobs')

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from utils.spark_session import get_spark_session
from utils.file_tracker import check_if_file_ingested
from silver.tiktok_comments.config import (
    SCRATCH_BASE_PATH_POSTS,
    SCRATCH_BASE_PATH_COMMENTS,
    BRONZE_BASE_PATH,
    BRONZE_FILE_PATTERN,
    NOT_NULL_COLUMNS_POSTS,
    NOT_NULL_COLUMNS_COMMENTS,
    POSTGRES_CONN,
    BATCH_SIZE
)


def get_s3_file_size(spark, file_path):
    """Get file size from S3 using Hadoop FileSystem API"""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs_uri = spark._jvm.java.net.URI(file_path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(fs_uri, hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
        file_status = fs.getFileStatus(path)
        size_bytes = file_status.getLen()
        return size_bytes
    except Exception as e:
        print(f"Warning: Could not get file size for {file_path}: {e}")
        return 0


def extract_value(line, label):
    """
    Extract value after label from metadata line
    
    Args:
        line: Line to parse
        label: Label to search for (e.g., "Post URL:")
    
    Returns:
        Extracted value (string or None)
    """
    if label not in line:
        return None
    
    parts = line.split(label, 1)
    if len(parts) != 2:
        return None
    
    value = parts[1].strip()
    
    # Remove surrounding quotes if present
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    
    return value if value else None


def extract_multiline_description(desc_lines):
    """
    Extract full multiline description from all lines between anchors
    
    Args:
        desc_lines: All lines from "Mô tả của bài đăng:" to before "Số bình luận cấp 1:"
    
    Returns:
        Full description text (may contain newlines)
    """
    if not desc_lines:
        return None
    
    # First line: "Mô tả của bài đăng:  "value..."
    first_line = desc_lines[0]
    label = 'Mô tả của bài đăng:'
    
    if label not in first_line:
        return None
    
    parts = first_line.split(label, 1)
    if len(parts) != 2:
        return None
    
    # Start accumulating text
    full_text = parts[1].strip()
    
    # Append subsequent lines (if multiline)
    for line in desc_lines[1:]:
        full_text += "\n" + line.strip()
    
    # Remove surrounding quotes if present
    if full_text.startswith('"') and full_text.endswith('"'):
        full_text = full_text[1:-1]
    
    return full_text.strip() if full_text else None


def parse_tiktok_file_metadata(lines):
    """
    Parse metadata using SEQUENTIAL PARSING approach
    
    Strategy:
    1. Parse 10 single-line fields before description
    2. Find anchors: "Mô tả của bài đăng:" and "Số bình luận cấp 1:"
    3. Extract FULL multiline description between anchors
    4. Parse 5 single-line fields after description
    
    This handles multiline descriptions robustly while maintaining
    the exact same output format as the original implementation.
    
    Args:
        lines: List of strings (file lines)
    
    Returns:
        dict with 16 metadata fields (all as strings)
    """
    if len(lines) < 17:
        print(f"File too short: {len(lines)} lines (expected ≥17)")
        return None
    
    try:
        # ============================================
        # STEP 1: Find two anchor points
        # ============================================
        desc_start_idx = None
        comments_level1_idx = None
        
        for i, line in enumerate(lines):
            if 'Mô tả của bài đăng:' in line and desc_start_idx is None:
                desc_start_idx = i
            if 'Số bình luận cấp 1:' in line and comments_level1_idx is None:
                comments_level1_idx = i
                break  # Found both anchors
        
        if desc_start_idx is None:
            print(f"Cannot find 'Mô tả của bài đăng:' label")
            return None
        
        if comments_level1_idx is None:
            print(f"Cannot find 'Số bình luận cấp 1:' label")
            return None
        
        # ============================================
        # STEP 2: Parse fields in sequential order
        # ============================================
        metadata = {}
        
        # Part 1: 10 single-line fields BEFORE description
        field_definitions_part1 = [
            ('crawl_time', 'Thời gian cào:'),
            ('post_url', 'Post URL:'),
            ('author', 'Người đăng:'),
            ('author_tag', 'Tag người đăng:'),
            ('author_url', 'URL người đăng:'),
            ('post_date', 'Thời gian đăng:'),
            ('likes', 'Số lượt tym:'),
            ('comments_count', 'Số lượt comment:'),
            ('saves', 'Số lượt lưu:'),
            ('shares', 'Số lượt share:')
        ]
        
        for field_name, label in field_definitions_part1:
            # Search in lines before description
            value = None
            for i in range(0, desc_start_idx):
                if label in lines[i]:
                    value = extract_value(lines[i], label)
                    break
            metadata[field_name] = value
        
        # Part 2: MULTILINE description field
        desc_lines = lines[desc_start_idx:comments_level1_idx]
        metadata['post_description'] = extract_multiline_description(desc_lines)
        
        # Part 3: 5 single-line fields AFTER description
        field_definitions_part3 = [
            ('comments_level1', 'Số bình luận cấp 1:'),
            ('comments_level2', 'Số bình luận cấp 2:'),
            ('comments_loaded', 'Tổng số bình luận thực tế đã load:'),
            ('comments_displayed_tiktok', 'Số bình luận TikTok hiển thị:'),
            ('comments_difference', 'Chênh lệch số bình luận:')
        ]
        
        for field_name, label in field_definitions_part3:
            # Search in lines from comments_level1_idx onwards
            value = None
            for i in range(comments_level1_idx, min(comments_level1_idx + 10, len(lines))):
                if label in lines[i]:
                    value = extract_value(lines[i], label)
                    break
            metadata[field_name] = value
        
        # ============================================
        # STEP 3: Validate critical fields
        # ============================================
        if not metadata.get('post_url'):
            print(f"Critical field 'post_url' is empty or missing")
            return None
        
        return metadata
        
    except Exception as e:
        print(f"Error parsing metadata: {e}")
        import traceback
        traceback.print_exc()
        return None


def parse_tiktok_file_comments(lines, post_url):
    """
    Parse comments from CSV section of TikTok file
    
    NOTE: Header line position varies because "Mô tả của bài đăng" can contain newlines!
    - If description is 1 line: Header at line 17
    - If description is 2 lines: Header at line 18
    - If description is 3 lines: Header at line 19
    
    Solution: Find header dynamically by looking for "STT,Tên,Tag tên"
    
    Args:
        lines: List of strings (file lines)
        post_url: Post URL to link comments
    
    Returns:
        list of dicts with comment data (all as strings for now)
    """
    if len(lines) < 18:
        return []
    
    comments_data = []
    
    try:
        # Find CSV header line dynamically with FULL pattern matching (100% accuracy)
        # Expected complete header: "STT,Tên,Tag tên,URL,Comment,Time,Likes,Level Comment,Replied To Tag Name,Number of Replies"
        # Check ALL columns to guarantee no false positives
        # NOTE: Use 'in line' to handle potential whitespace variations
        header_index = None
        for i, line in enumerate(lines):
            # Normalize line: remove extra spaces for robust matching
            line_normalized = line.strip()
            if (line_normalized.startswith('STT,') and 
                'Tên' in line_normalized and
                'Tag tên' in line_normalized and
                'URL' in line_normalized and
                'Comment' in line_normalized and
                'Time' in line_normalized and
                'Likes' in line_normalized and
                'Level Comment' in line_normalized and
                'Replied To Tag Name' in line_normalized and
                'Number of Replies' in line_normalized):
                header_index = i
                break
        
        if header_index is None:
            print(f"Cannot find CSV header (expected: STT,Tên,Tag tên,URL,Comment,Time,Likes,Level Comment,Replied To Tag Name,Number of Replies) in file")
            return []
        
        # CSV lines = header + data rows
        csv_lines = lines[header_index:]
        
        if len(csv_lines) <= 1:
            return []  # Only header, no data
        
        # Parse CSV using Python csv module (handles commas in text)
        csv_str = '\n'.join(csv_lines)
        reader = csv.DictReader(io.StringIO(csv_str))
        
        for row in reader:
            comments_data.append({
                'post_url': post_url,
                'stt': row.get('STT', ''),
                'ten': row.get('Tên', ''),
                'tag_ten': row.get('Tag tên', ''),
                'url': row.get('URL', ''),
                'comment': row.get('Comment', ''),
                'time': row.get('Time', ''),
                'likes': row.get('Likes', ''),
                'level_comment': row.get('Level Comment', ''),
                'replied_to_tag_name': row.get('Replied To Tag Name', ''),
                'number_of_replies': row.get('Number of Replies', '')
            })
    except Exception as e:
        print(f"Error parsing comments CSV: {e}")
    
    return comments_data


def get_all_bronze_files(spark, bronze_base_path):
    """
    Get all Bronze CSV files
    
    Returns:
        list of tuples: [(file_path, file_name, scrape_timestamp), ...]
    """
    try:
        files_df = spark.read.format("binaryFile") \
            .load(f"{bronze_base_path}/*.csv") \
            .select("path")
        
        file_list = [row.path for row in files_df.collect()]
        
        if not file_list:
            print(f"No Bronze files found in {bronze_base_path}")
            return []
        
        print(f"Found {len(file_list)} Bronze CSV files")
        
        # Parse filenames to extract scrape timestamp
        files_with_timestamp = []
        
        for file_path in file_list:
            file_name = os.path.basename(file_path)
            match = re.search(BRONZE_FILE_PATTERN, file_name)
            
            if match:
                scrape_timestamp = match.group(1)  # YYYY-MM-DDTHH-MM-SS
                files_with_timestamp.append((file_path, file_name, scrape_timestamp))
        
        # Sort by timestamp (ascending - oldest first)
        files_with_timestamp.sort(key=lambda x: x[2])
        
        return files_with_timestamp
        
    except Exception as e:
        print(f"Error listing Bronze files: {e}")
        import traceback
        traceback.print_exc()
        return []


def process_single_file(spark, file_path, file_name, scrape_timestamp):
    """
    Process single TikTok comment file
    
    Returns:
        tuple: (post_dict, comments_list, file_size_bytes)
        or (None, None, 0) if failed
    """
    try:
        # Get file size
        file_size_bytes = get_s3_file_size(spark, file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        print(f"\nProcessing: {file_name}")
        print(f"Size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
        
        # Read file as text lines
        lines_df = spark.read.text(file_path)
        lines = [row.value for row in lines_df.collect()]
        
        if len(lines) < 18:
            print(f"Skipped: File too short (< 18 lines)")
            return None, None, 0
        
        # Parse metadata (lines 1-17)
        metadata = parse_tiktok_file_metadata(lines)
        
        if not metadata:
            print(f"Failed to parse metadata")
            return None, None, 0
        
        # Add ingestion metadata (WITHOUT ingestion_timestamp - will add later with F.lit)
        metadata['scrape_timestamp'] = scrape_timestamp
        metadata['source_file'] = file_name
        metadata['source_file_checksum'] = scrape_timestamp.replace('-', '').replace('T', '')
        metadata['source_file_size_bytes'] = file_size_bytes
        
        print(f"Post: {metadata['post_url']}")
        print(f"Author: {metadata['author']} (@{metadata['author_tag']})")
        
        # Parse comments (lines 18+)
        comments_data = parse_tiktok_file_comments(lines, metadata['post_url'])
        
        # Add ingestion metadata to comments (WITHOUT ingestion_timestamp - will add later with F.lit)
        for comment in comments_data:
            comment['scrape_timestamp'] = scrape_timestamp
            comment['source_file'] = file_name
            comment['source_file_checksum'] = scrape_timestamp.replace('-', '').replace('T', '')
            comment['source_file_size_bytes'] = file_size_bytes
        
        print(f"Comments: {len(comments_data)}")
        print(f"Parsed successfully")
        
        return metadata, comments_data, file_size_bytes
        
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return None, None, 0


def validate_posts_data(df):
    """Validate NOT NULL constraints for posts and return clean DataFrame"""
    null_checks = {}
    for col_name in NOT_NULL_COLUMNS_POSTS:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_checks[col_name] = null_count
    
    total_nulls = sum(null_checks.values())
    
    if total_nulls > 0:
        print(f"Warning: Found {total_nulls} NULL values in post critical columns")
        for col_name, count in null_checks.items():
            if count > 0:
                print(f"{col_name}: {count} NULLs")
        
        print(f"Filtering out posts with NULL critical values")
        df_clean = df
        for col_name in NOT_NULL_COLUMNS_POSTS:
            df_clean = df_clean.filter(F.col(col_name).isNotNull())
        
        return df_clean, total_nulls
    
    print(f"Posts validation passed - no NULL critical values")
    return df, 0


def validate_comments_data(df):
    """Validate NOT NULL constraints for comments and return clean DataFrame"""
    null_checks = {}
    for col_name in NOT_NULL_COLUMNS_COMMENTS:
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_checks[col_name] = null_count
    
    total_nulls = sum(null_checks.values())
    
    if total_nulls > 0:
        print(f"Warning: Found {total_nulls} NULL values in comment critical columns")
        for col_name, count in null_checks.items():
            if count > 0:
                print(f"{col_name}: {count} NULLs")
        
        print(f"Filtering out comments with NULL critical values")
        df_clean = df
        for col_name in NOT_NULL_COLUMNS_COMMENTS:
            df_clean = df_clean.filter(F.col(col_name).isNotNull())
        
        return df_clean, total_nulls
    
    print(f"Comments validation passed - no NULL critical values")
    return df, 0


def transform_bronze_to_scratch(spark):
    """
    Transform: Read all Bronze CSV files → Write to 2 Scratch Parquet folders
    
    Process in batches to avoid memory issues with many files
    """
    print(f"STEP 1: Transform Bronze → Scratch")
    print(f"Source: {BRONZE_BASE_PATH}")
    print(f"Target 1 (Posts): {SCRATCH_BASE_PATH_POSTS}")
    print(f"Target 2 (Comments): {SCRATCH_BASE_PATH_COMMENTS}")
    
    # Get all Bronze files
    all_files = get_all_bronze_files(spark, BRONZE_BASE_PATH)
    
    if not all_files:
        print(f"No Bronze files found")
        return 0, 0
    
    total_files = len(all_files)
    print(f"\nTotal Bronze files: {total_files}")
    
    # Filter unprocessed files (check PostgreSQL tracking)
    print(f"\nChecking which files are already processed...")
    unprocessed = []
    
    for file_path, file_name, scrape_timestamp in all_files:
        # Use scrape_timestamp as checksum (unique per file)
        file_checksum = scrape_timestamp.replace('-', '').replace('T', '')
        
        if not check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
            unprocessed.append((file_path, file_name, scrape_timestamp))
    
    unprocessed_count = len(unprocessed)
    skipped_count = total_files - unprocessed_count
    
    print(f"New files to process: {unprocessed_count}")
    print(f"Already processed: {skipped_count}")
    
    if unprocessed_count == 0:
        print(f"\nAll files already processed in Silver layer!")
        return 0, 0
    
    # Generate unique run ID for this transform run
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path_posts = f"{SCRATCH_BASE_PATH_POSTS}/run_{run_id}"
    output_path_comments = f"{SCRATCH_BASE_PATH_COMMENTS}/run_{run_id}"
    
    print(f"\nOutput paths:")
    print(f"Posts: {output_path_posts}")
    print(f"Comments: {output_path_comments}")
    
    # Process in batches and write incrementally
    num_batches = (unprocessed_count + BATCH_SIZE - 1) // BATCH_SIZE
    
    print(f"\nProcessing {unprocessed_count} files in {num_batches} batch(es) ({BATCH_SIZE} files/batch)")
    print(f"Strategy: Process batch → Validate → Write (incremental)")
    print(f"=" * 80)
    
    total_posts_written = 0
    total_comments_written = 0
    
    for batch_idx in range(0, unprocessed_count, BATCH_SIZE):
        batch_files = unprocessed[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = batch_idx // BATCH_SIZE + 1
        
        print(f"\n{'='*80}")
        print(f"BATCH {batch_num}/{num_batches}: Processing {len(batch_files)} files")
        print(f"{'='*80}")
        
        # Collect batch data in memory (only 1 batch at a time)
        batch_posts = []
        batch_comments = []
        
        # Process each file in batch
        for file_path, file_name, scrape_timestamp in batch_files:
            post_metadata, comments_data, file_size = process_single_file(
                spark, file_path, file_name, scrape_timestamp
            )
            
            if post_metadata:
                batch_posts.append(post_metadata)
            
            if comments_data:
                batch_comments.extend(comments_data)
        
        # Skip if no data parsed in this batch
        if not batch_posts:
            print(f"No data parsed in batch {batch_num} - skipping write")
            continue
        
        # Create DataFrames for this batch
        print(f"\nCreating batch DataFrames...")
        df_batch_posts = spark.createDataFrame(batch_posts)
        df_batch_comments = spark.createDataFrame(batch_comments) if batch_comments else None
        
        # Add ingestion_timestamp
        print(f"Adding ingestion_timestamp...")
        df_batch_posts = df_batch_posts.withColumn("ingestion_timestamp", F.lit(datetime.now()))
        if df_batch_comments:
            df_batch_comments = df_batch_comments.withColumn("ingestion_timestamp", F.lit(datetime.now()))
        
        # Validate posts
        print(f"Validating posts...")
        df_batch_posts_clean, posts_nulls = validate_posts_data(df_batch_posts)
        if posts_nulls > 0:
            print(f"Removed {posts_nulls} posts with NULL critical values")
        
        # Validate comments
        if df_batch_comments:
            print(f"Validating comments...")
            df_batch_comments_clean, comments_nulls = validate_comments_data(df_batch_comments)
            if comments_nulls > 0:
                print(f"Removed {comments_nulls} comments with NULL critical values")
        else:
            df_batch_comments_clean = None
        
        # Write batch to Scratch (append mode after first batch)
        write_mode = "overwrite" if batch_num == 1 else "append"
        
        print(f"\nWriting batch {batch_num} to Scratch ({write_mode} mode)...")
        
        # Write posts
        posts_in_batch = df_batch_posts_clean.count()
        df_batch_posts_clean.write \
            .mode(write_mode) \
            .partitionBy("post_url") \
            .parquet(output_path_posts)
        
        total_posts_written += posts_in_batch
        print(f"Posts: +{posts_in_batch:,} (total: {total_posts_written:,})")
        
        # Write comments
        if df_batch_comments_clean:
            comments_in_batch = df_batch_comments_clean.count()
            df_batch_comments_clean.write \
                .mode(write_mode) \
                .partitionBy("post_url") \
                .parquet(output_path_comments)
            
            total_comments_written += comments_in_batch
            print(f"Comments: +{comments_in_batch:,} (total: {total_comments_written:,})")
        
        print(f"\nBatch {batch_num}/{num_batches} completed and written!")
    
    print(f"\n" + "=" * 80)
    print(f"TRANSFORM SUMMARY:")
    print(f"Total posts written: {total_posts_written:,}")
    print(f"Total comments written: {total_comments_written:,}")
    print(f"Output run ID: {run_id}")
    print(f"=" * 80)
    
    return total_posts_written, total_comments_written


def main():
    print("=" * 80)
    print("SILVER TIKTOK COMMENTS - STEP 1: TRANSFORM (Bronze → Scratch)")
    print("=" * 80)
    
    spark = None
    
    try:
        spark = get_spark_session(app_name="Silver_TikTok_Comments_Step1_Transform")
        
        posts_count, comments_count = transform_bronze_to_scratch(spark)
        
        print("\n" + "=" * 80)
        if posts_count > 0 or comments_count > 0:
            print(f"STEP 1 COMPLETED:")
            print(f"Posts transformed: {posts_count:,}")
            print(f"Comments transformed: {comments_count:,}")
        else:
            print(f"STEP 1 COMPLETED: No new data to process")
        print("=" * 80)
        print(f"\nNext: Run Step 2 (Clean & Load to Silver)")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
