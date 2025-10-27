"""
Bronze Layer - Ingest TikTok Comment Files
Source: data/raw/tiktok/comments/tiktok_comments_*.csv
Target: 
  - lakehouse.bronze.raw_tiktok_post_metadata   (metadata from lines 1-16)
  - lakehouse.bronze.raw_tiktok_post_comments (comments from lines 18+)

Strategy: APPEND mode with incremental loading (checksum-based deduplication)
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


def create_bronze_posts_table(spark):
    """Create Bronze table for posts metadata if not exists"""
    
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
        database="bronze",
        table_name="raw_tiktok_post_metadata  ",
        schema=schema,
        partition_by=[],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def create_bronze_comments_table(spark):
    """Create Bronze table for comments if not exists"""
    
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
        database="bronze",
        table_name="raw_tiktok_post_comments",
        schema=schema,
        partition_by=["post_url"],
        table_properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "snappy"
        }
    )


def check_if_file_ingested(spark, file_checksum):
    """Check if file already ingested by checksum"""
    
    try:
        result = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/metastore_db") \
            .option("dbtable", "file_ingestion_log") \
            .option("user", "lakehouse_user") \
            .option("password", "lakehouse_pass") \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .filter(F.col("file_checksum") == file_checksum) \
            .filter(F.col("status") == "success") \
            .count()
        
        return result > 0
    
    except Exception as e:
        print(f"⚠️ Could not check tracking log: {e}")
        return False


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
    post_df.writeTo("lakehouse.bronze.raw_tiktok_post_metadata  ") \
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
        comments_df.writeTo("lakehouse.bronze.raw_tiktok_post_comments") \
            .using("iceberg") \
            .append()
        
        print(f"   ✅ {len(comments_data)} comments ingested")
    
    return 1, len(comments_data)  # 1 post, N comments


def main():
    """Main execution"""
    
    print("=" * 80)
    print("🔄 BRONZE INGESTION - TikTok Comment Files")
    print("=" * 80)
    
    # Source directory (mounted at /data in container)
    source_dir = "/data/raw/tiktok/comments"
    
    # Get Spark session
    spark = get_spark_session(
        app_name="Bronze_Ingest_TikTok_Comments"
    )
    
    try:
        # Create tables if not exists
        print("\n1️⃣ Creating Bronze table schemas...")
        create_bronze_posts_table(spark)
        create_bronze_comments_table(spark)
        
        # Find all comment CSV files
        print("\n2️⃣ Scanning for comment files...")
        comment_files = [
            os.path.join(source_dir, f) 
            for f in os.listdir(source_dir) 
            if f.startswith('tiktok_comments_') and f.endswith('.csv')
        ]
        
        print(f"   Found {len(comment_files)} files")
        
        # Process each file
        total_posts = 0
        total_comments = 0
        skipped_files = 0
        
        print("\n3️⃣ Processing files...")
        
        for file_path in comment_files:
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            file_checksum = calculate_file_checksum(file_path)
            
            # Check if already ingested
            if check_if_file_ingested(file_checksum, POSTGRES_CONN):
                print(f"\n📄 {file_name}")
                print(f"   ⏭️  Already ingested (checksum: {file_checksum[:8]}...)")
                skipped_files += 1
                continue
            
            try:
                # Ingest file
                posts, comments = ingest_comment_file(
                    spark, file_path, file_checksum, file_name, file_size
                )
                
                total_posts += posts
                total_comments += comments
                
                # Prepare ingestion_details for multi-table tracking
                ingestion_details = {
                    "tables": [
                        {
                            "name": "raw_tiktok_post_metadata",
                            "records": posts
                        },
                        {
                            "name": "raw_tiktok_post_comments",
                            "records": comments
                        }
                    ]
                }
                
                # Log to tracking table WITH ingestion_details
                log_ingestion_to_postgres(
                    file_path=file_path,
                    file_checksum=file_checksum,
                    records_ingested=posts + comments,
                    table_name="bronze.raw_tiktok_post_metadata + raw_tiktok_post_comments",
                    status="success",
                    postgres_conn_params=POSTGRES_CONN,
                    ingestion_details=ingestion_details
                )
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                log_ingestion_to_postgres(
                    file_path=file_path,
                    file_checksum=file_checksum,
                    records_ingested=0,
                    table_name="bronze.raw_tiktok_post_metadata + raw_tiktok_post_comments",
                    status="failed",
                    postgres_conn_params=POSTGRES_CONN,
                    error_message=str(e)
                )
                continue
        
        print("\n" + "=" * 80)
        print(f"✅ COMPLETED:")
        print(f"   Posts ingested: {total_posts}")
        print(f"   Comments ingested: {total_comments}")
        print(f"   Files skipped (already ingested): {skipped_files}")
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
