"""
Silver Layer - Transform TikTok Comment Files
Source: s3a://bronze/lakehouse/tiktok_comments/raw/*.csv
Target: 
  - lakehouse.silver.tiktok_post_metadata   (metadata from lines 1-16)
  - lakehouse.silver.tiktok_post_comments (comments from lines 18+)

Strategy: APPEND mode with incremental loading (checksum-based deduplication)
Note: Crawler creates new files, never updates existing files - APPEND is correct
"""

import sys
import os
from datetime import datetime
import csv
import io

# Add parent directory to path
sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.iceberg_utils import create_iceberg_table_if_not_exists
from utils.file_tracker import (
    calculate_file_checksum,
    check_if_file_ingested,
    log_ingestion_to_postgres
)
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# PostgreSQL connection parameters
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}


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
        print(f"⚠️  Warning: Could not get file size for {file_path}: {e}")
        return 0


def create_silver_posts_table(spark):
    """Create Silver table for posts metadata if not exists"""
    
    schema = StructType([
        # Primary key
        StructField("post_url", StringType(), False),
        
        # Author info
        StructField("author", StringType(), True),
        StructField("author_tag", StringType(), True),
        StructField("author_url", StringType(), True),
        
        # Post info
        StructField("post_date", StringType(), True),
        StructField("post_description", StringType(), True),
        
        # Engagement metrics
        StructField("likes", StringType(), True),
        StructField("comments_count", StringType(), True),
        StructField("saves", StringType(), True),
        StructField("shares", StringType(), True),
        
        # Comment statistics
        StructField("comments_level1", StringType(), True),
        StructField("comments_level2", StringType(), True),
        StructField("comments_loaded", StringType(), True),
        StructField("comments_displayed_tiktok", StringType(), True),
        StructField("comments_difference", StringType(), True),
        
        # Crawl metadata
        StructField("crawl_time", StringType(), True),
        
        # Ingestion metadata
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False)
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database="silver",
        table_name="tiktok_post_metadata",
        schema=schema,
        partition_by=[],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def create_silver_comments_table(spark):
    """Create Silver table for comments if not exists"""
    
    schema = StructType([
        # Link to post
        StructField("post_url", StringType(), False),
        
        # Comment data (10 columns from CSV)
        StructField("stt", StringType(), True),
        StructField("ten", StringType(), True),
        StructField("tag_ten", StringType(), True),
        StructField("url", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("time", StringType(), True),
        StructField("likes", StringType(), True),
        StructField("level_comment", StringType(), True),
        StructField("replied_to_tag_name", StringType(), True),
        StructField("number_of_replies", StringType(), True),
        
        # Ingestion metadata
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("source_file_checksum", StringType(), False)
    ])
    
    create_iceberg_table_if_not_exists(
        spark=spark,
        database="silver",
        table_name="tiktok_post_comments",
        schema=schema,
        partition_by=["post_url"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def parse_comment_file(file_path):
    """
    Parse TikTok comment CSV file
    
    Returns:
        post_metadata: dict with 16 metadata fields
        comments_data: list of dicts with comment data
    """
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Helper function to extract value from metadata line
    def extract_value(line):
        """Extract value after colon and spaces"""
        if ':' in line:
            return line.split(':', 1)[1].strip()
        return ''
    
    # Parse metadata (lines 0-15, which are lines 1-16 in file)
    post_metadata = {}
    
    try:
        post_metadata['crawl_time'] = extract_value(lines[0])
        post_metadata['post_url'] = extract_value(lines[1])
        post_metadata['author'] = extract_value(lines[2])
        post_metadata['author_tag'] = extract_value(lines[3])
        post_metadata['author_url'] = extract_value(lines[4])
        post_metadata['post_date'] = extract_value(lines[5])
        post_metadata['likes'] = extract_value(lines[6])
        post_metadata['comments_count'] = extract_value(lines[7])
        post_metadata['saves'] = extract_value(lines[8])
        post_metadata['shares'] = extract_value(lines[9])
        post_metadata['post_description'] = extract_value(lines[10])
        post_metadata['comments_level1'] = extract_value(lines[11])
        post_metadata['comments_level2'] = extract_value(lines[12])
        post_metadata['comments_loaded'] = extract_value(lines[13])
        post_metadata['comments_displayed_tiktok'] = extract_value(lines[14])
        post_metadata['comments_difference'] = extract_value(lines[15])
    
    except Exception as e:
        print(f"⚠️ Error parsing metadata: {e}")
        raise
    
    # Parse comments (line 17 is header, line 18+ is data)
    comments_data = []
    post_url = post_metadata['post_url']
    
    # Read CSV data starting from line 17 (index 17)
    csv_content = ''.join(lines[17:])
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
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
            print(f"⚠️ Error parsing comment row: {e}")
            continue
    
    return post_metadata, comments_data


def ingest_comment_file(spark, file_path, file_checksum, file_name, file_size):
    """Ingest single comment file"""
    
    print(f"\n📄 Processing: {file_name}")
    
    # Parse file
    post_metadata, comments_data = parse_comment_file(file_path)
    
    print(f"   Post URL: {post_metadata['post_url']}")
    print(f"   Comments: {len(comments_data)} records")
    
    # Add metadata to post
    post_metadata['ingestion_timestamp'] = datetime.now()
    post_metadata['source_file'] = file_name
    post_metadata['source_file_checksum'] = file_checksum
    
    # Create DataFrame for post
    post_df = spark.createDataFrame([post_metadata])
    
    # Write to posts table
    post_df.writeTo("lakehouse.silver.tiktok_post_metadata") \
        .using("iceberg") \
        .append()
    
    print(f"   ✅ Post metadata ingested")
    
    # Add metadata to comments
    for comment in comments_data:
        comment['ingestion_timestamp'] = datetime.now()
        comment['source_file'] = file_name
        comment['source_file_checksum'] = file_checksum
    
    # Create DataFrame for comments
    if comments_data:
        comments_df = spark.createDataFrame(comments_data)
        
        # Write to comments table
        comments_df.writeTo("lakehouse.silver.tiktok_post_comments") \
            .using("iceberg") \
            .append()
        
        print(f"   ✅ {len(comments_data)} comments ingested")
    
    return 1, len(comments_data)  # 1 post, N comments


def transform_tiktok_comments(spark, source_pattern):
    """
    Transform TikTok comment files from Bronze into 2 Silver tables
    Each file has: 16-line metadata header + CSV comments
    """
    print(f"🚀 Starting transformation: {source_pattern}")
    
    # Read all text files from Bronze (NOT as CSV yet - need to parse header)
    df_raw = spark.read.text(source_pattern)
    
    # Get list of files using input_file_name() function
    file_paths = df_raw.select(F.input_file_name().alias("file_path")).distinct().collect()
    file_paths = [row.file_path for row in file_paths]
    
    print(f"📝 Found {len(file_paths)} comment files in Bronze")
    
    total_posts = 0
    total_comments = 0
    skipped = 0
    
    for file_path in file_paths:
        file_name = os.path.basename(file_path)
        
        # Extract checksum from filename (Bronze already calculated it)
        # Format: tiktok_comments_2025-09-27T04-22-16_20251029_203916_d0e0e6d9.csv
        #                                                             ^^^^^^^^ checksum
        file_checksum = file_name.replace('.csv', '').split('_')[-1]
        
        # Get file size from S3
        file_size_bytes = get_s3_file_size(spark, file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        # Check if already processed in Silver layer
        if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='silver'):
            print(f"\n📄 {file_name}")
            print(f"   📦 File size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
            print(f"   ⏭️  Already processed in Silver layer (checksum: {file_checksum})")
            skipped += 1
            continue
        
        print(f"\n📄 Processing: {file_name}")
        print(f"   📦 File size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
        
        # Read this specific file as text lines
        lines_df = spark.read.text(file_path)
        lines = [row.value for row in lines_df.collect()]
        
        if len(lines) < 18:
            print(f"   ⚠️  Skipped: File too short (< 18 lines)")
            skipped += 1
            continue
        
        # Parse metadata (lines 0-15 = first 16 lines)
        try:
            def extract_value(line):
                return line.split(':', 1)[1].strip() if ':' in line else ''
            
            post_metadata = {
                'crawl_time': extract_value(lines[0]),
                'post_url': extract_value(lines[1]),
                'author': extract_value(lines[2]),
                'author_tag': extract_value(lines[3]),
                'author_url': extract_value(lines[4]),
                'post_date': extract_value(lines[5]),
                'likes': extract_value(lines[6]),
                'comments_count': extract_value(lines[7]),
                'saves': extract_value(lines[8]),
                'shares': extract_value(lines[9]),
                'post_description': extract_value(lines[10]),
                'comments_level1': extract_value(lines[11]),
                'comments_level2': extract_value(lines[12]),
                'comments_loaded': extract_value(lines[13]),
                'comments_displayed_tiktok': extract_value(lines[14]),
                'comments_difference': extract_value(lines[15]),
                'ingestion_timestamp': datetime.now(),
                'source_file': file_name,
                'source_file_checksum': file_name  # Use filename as checksum for simplicity
            }
            
            print(f"   Post: {post_metadata['post_url']}")
            print(f"   Author: {post_metadata['author']} ({post_metadata['author_tag']})")
            
            # Create DataFrame for post metadata and APPEND
            post_df = spark.createDataFrame([post_metadata])
            post_df.writeTo("lakehouse.silver.tiktok_post_metadata") \
                .using("iceberg") \
                .append()
            
            total_posts += 1
            print(f"   ✅ Post metadata appended")
            
        except Exception as e:
            print(f"   ❌ Error parsing metadata: {e}")
            skipped += 1
            continue
        
        # Parse comments (line 17 is header, line 18+ is data)
        # Read CSV from line 18 onwards
        try:
            # Create temp CSV string from lines 17+
            csv_lines = lines[17:]  # Line 17 (index 17) = header, 18+ = data
            
            if len(csv_lines) <= 1:
                print(f"   ⚠️  No comment data (only header)")
                continue
            
            # Use Spark to parse CSV from lines
            from io import StringIO
            import csv
            
            csv_str = '\n'.join(csv_lines)
            
            # Parse with Python csv module first to get column names
            reader = csv.DictReader(StringIO(csv_str))
            comments_data = []
            
            for row in reader:
                comments_data.append({
                    'post_url': post_metadata['post_url'],
                    'stt': row.get('STT', ''),
                    'ten': row.get('Tên', ''),
                    'tag_ten': row.get('Tag tên', ''),
                    'url': row.get('URL', ''),
                    'comment': row.get('Comment', ''),
                    'time': row.get('Time', ''),
                    'likes': row.get('Likes', ''),
                    'level_comment': row.get('Level Comment', ''),
                    'replied_to_tag_name': row.get('Replied To Tag Name', ''),
                    'number_of_replies': row.get('Number of Replies', ''),
                    'ingestion_timestamp': datetime.now(),
                    'source_file': file_name,
                    'source_file_checksum': file_name
                })
            
            if comments_data:
                comments_df = spark.createDataFrame(comments_data)
                comments_df.writeTo("lakehouse.silver.tiktok_post_comments") \
                    .using("iceberg") \
                    .append()
                
                total_comments += len(comments_data)
                print(f"   ✅ {len(comments_data)} comments appended")
            
            # Log successful processing to tracking table (Silver layer)
            ingestion_details = {
                "tables": [
                    {
                        "name": "tiktok_post_metadata",
                        "records": 1
                    },
                    {
                        "name": "tiktok_post_comments",
                        "records": len(comments_data) if comments_data else 0
                    }
                ],
                "post_url": post_metadata['post_url'],
                "author": post_metadata['author'],
                "source_size_bytes": file_size_bytes
            }
            
            log_ingestion_to_postgres(
                file_path=file_path,
                file_checksum=file_checksum,
                records_ingested=1 + (len(comments_data) if comments_data else 0),
                table_name="silver.tiktok_post_metadata + tiktok_post_comments",
                status="success",
                postgres_conn_params=POSTGRES_CONN,
                layer='silver',  # Track in Silver layer
                ingestion_details=ingestion_details,
                file_size_bytes=file_size_bytes
            )
            
        except Exception as e:
            print(f"   ⚠️  Error parsing comments: {e}")
            
            # Log failed processing
            log_ingestion_to_postgres(
                file_path=file_path,
                file_checksum=file_checksum,
                records_ingested=0,
                table_name="silver.tiktok_post_metadata + tiktok_post_comments",
                status="failed",
                postgres_conn_params=POSTGRES_CONN,
                layer='silver',
                error_message=str(e),
                file_size_bytes=file_size_bytes
            )
            
            # Continue - we already saved post metadata
    
    return total_posts, total_comments, skipped


def main():
    """Main execution"""
    
    print("=" * 80)
    print("🔄 SILVER TRANSFORMATION - TikTok Comment Files (APPEND MODE)")
    print("=" * 80)
    
    # Source from Bronze S3
    source_pattern = "s3a://bronze/lakehouse/tiktok_comments/raw/*.csv"
    
    # Get Spark session (same as hotels_detail pattern)
    spark = get_spark_session(
        app_name="Silver_Transform_TikTok_Comments"
    )
    
    try:
        # Create tables if not exists
        print("\n1️⃣  Creating Silver table schemas...")
        create_silver_posts_table(spark)
        create_silver_comments_table(spark)
        
        # Transform files
        print("\n2️⃣  Transforming comment files from Bronze...")
        posts, comments, skipped = transform_tiktok_comments(spark, source_pattern)
        # Transform files
        print("\n2️⃣  Transforming comment files from Bronze...")
        posts, comments, skipped = transform_tiktok_comments(spark, source_pattern)
        
        print("\n" + "=" * 80)
        print(f"✅ COMPLETED:")
        print(f"   Posts appended: {posts}")
        print(f"   Comments appended: {comments}")
        print(f"   Files skipped: {skipped}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
