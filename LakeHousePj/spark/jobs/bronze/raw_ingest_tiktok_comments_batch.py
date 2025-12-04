"""
Bronze Layer - BATCH RAW File Ingestion: TikTok Comments
Source: /data/raw/tiktok/comments/*.csv (multiple files, special format)
Target: s3a://bronze/lakehouse/tiktok_comments/raw/*.csv (versioned)
Purpose:
- Process ALL TikTok comment files in directory
- Each file has 16-line metadata header + CSV comments
- Copy each file with timestamp + checksum versioning
- Track each file in PostgreSQL (layer='bronze')
- Skip files already ingested (checksum-based deduplication)
Use Case:
- TikTok crawler creates NEW file each run (not updating existing)
- This job ingests all comment files from crawl directory
- Runs incrementally: only new files are ingested
"""

import sys
import os
import glob
from datetime import datetime

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session
from utils.file_tracker import (
    calculate_file_checksum,
    check_if_file_ingested,
    log_ingestion_to_postgres
)
from utils.s3_utf8_uploader import copy_file_to_bronze_with_utf8

# PostgreSQL connection parameters
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}

def ingest_single_comment_file(spark, source_path: str, bronze_bucket: str, source_type: str):
    """
    Ingest a single TikTok comments file
    Returns: dict with status info
    """
    try:
        file_name = os.path.basename(source_path)
        file_size = os.path.getsize(source_path)
        
        print(f"Processing: {file_name}")
        print(f"Size: {file_size / 1024:.2f} KB")
        
        # Calculate checksum
        file_checksum = calculate_file_checksum(source_path)
        print(f"1. Checksum: {file_checksum[:16]}...")
        
        # Check duplicate
        if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='bronze'):
            print(f"Already ingested - SKIP")
            return {
                'file': file_name,
                'status': 'skipped',
                'reason': 'already_exists'
            }
        
        # Read file to count lines (với encoding UTF-8)
        try:
            with open(source_path, 'r', encoding='utf-8', errors='replace') as f:
                total_lines = len(f.readlines())
        except UnicodeDecodeError:
            # Fallback to latin-1 nếu UTF-8 failed
            with open(source_path, 'r', encoding='latin-1') as f:
                total_lines = len(f.readlines())
        
        # Generate versioned filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(file_name)[0]
        new_filename = f"{base_name}_{timestamp}_{file_checksum[:8]}.csv"
        
        # Copy to Bronze với UTF-8 encoding
        # Note: Giữ nguyên format gốc (16-line metadata + CSV data)
        # Copy to Bronze với UTF-8 encoding preservation
        # Use utility function for proper UTF-8 handling
        print(f"2. Copying to Bronze with UTF-8 encoding...")
        
        upload_result = copy_file_to_bronze_with_utf8(
            source_path=source_path,
            bronze_bucket=bronze_bucket,
            source_type=source_type,
            new_filename=new_filename,
            spark_session=spark
        )
        
        if upload_result['status'] != 'success':
            raise Exception(f"Upload failed: {upload_result.get('error', 'Unknown error')}")
        
        bronze_output = upload_result['location']
        print(f"Uploaded to: {bronze_output}")
        
        # Log to PostgreSQL
        ingestion_details = {
            "source_format": "text",
            "file_structure": "16_line_metadata_header + csv_data",
            "total_lines": total_lines,
            "original_filename": file_name,
            "bronze_location": bronze_output,
            "file_size_kb": round(file_size / 1024, 2)
        }
        
        log_ingestion_to_postgres(
            file_path=source_path,
            file_checksum=file_checksum,
            records_ingested=total_lines,
            table_name=f"{source_type}_raw",
            status='success',
            layer='bronze',
            postgres_conn_params=POSTGRES_CONN,
            ingestion_details=ingestion_details,
            file_size_bytes=file_size  # Pass file size in bytes
        )
        
        print(f"SUCCESS - {total_lines} lines ingested")
        
        return {
            'file': file_name,
            'status': 'success',
            'lines': total_lines,
            'checksum': file_checksum[:8],
            'bronze_file': new_filename
        }
        
    except Exception as e:
        print(f"FAILED: {str(e)}")
        
        try:
            log_ingestion_to_postgres(
                file_path=source_path,
                file_checksum=file_checksum if 'file_checksum' in locals() else 'unknown',
                records_ingested=0,
                table_name=f"{source_type}_raw",
                status='failed',
                layer='bronze',
                error_message=str(e),
                postgres_conn_params=POSTGRES_CONN,
                file_size_bytes=file_size if 'file_size' in locals() else 0
            )
        except:
            pass
        
        return {
            'file': file_name,
            'status': 'failed',
            'error': str(e)
        }

def ingest_all_comment_files(comments_dir: str, bronze_bucket: str, source_type: str):
    """
    Batch ingest ALL TikTok comment files from directory
    """
    
    spark = get_spark_session(app_name=f"Bronze_RAW_Batch_Ingest_{source_type}")
    
    try:
        print(f"=" * 80)
        print(f"BRONZE RAW BATCH INGESTION: TikTok Comments")
        print(f"=" * 80)
        print(f"Source Directory: {comments_dir}")
        print(f"Target: s3a://{bronze_bucket}/lakehouse/{source_type}/raw/")
        print(f"Searching for *.csv files...")
        
        # Find all CSV files
        csv_pattern = os.path.join(comments_dir, "*.csv")
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            print(f"No CSV files found in {comments_dir}")
            return
        
        print(f"Found {len(csv_files)} file(s)")
        print(f"\n" + "=" * 80)
        
        # Process each file
        results = []
        for idx, csv_file in enumerate(csv_files, 1):
            print(f"\n[{idx}/{len(csv_files)}] {'-' * 60}")
            result = ingest_single_comment_file(spark, csv_file, bronze_bucket, source_type)
            results.append(result)
        
        # Summary
        print(f"\n" + "=" * 80)
        print(f"BATCH INGESTION SUMMARY")
        print(f"=" * 80)
        
        success_count = len([r for r in results if r['status'] == 'success'])
        skipped_count = len([r for r in results if r['status'] == 'skipped'])
        failed_count = len([r for r in results if r['status'] == 'failed'])
        
        print(f"Success: {success_count}")
        print(f"Skipped: {skipped_count}")
        print(f"Failed:  {failed_count}")
        print(f"Total:   {len(results)}")
        
        # Total lines ingested
        total_lines = sum([r.get('lines', 0) for r in results if r['status'] == 'success'])
        print(f"Total Lines Ingested: {total_lines:,}")
        
        print(f"\n" + "=" * 80)
        
        if success_count > 0:
            print(f"BATCH INGESTION COMPLETED: {success_count} file(s) ingested")
        else:
            print(f"No new files to ingest")
        
        print(f"=" * 80)
        
    except Exception as e:
        print(f"BATCH INGESTION ERROR: {str(e)}")
        raise
    finally:
        spark.stop()

def main():
    """
    Main entry point
    Args:
      1. comments_dir - Directory containing TikTok comment CSV files
      2. bronze_bucket - Bronze S3 bucket name (default: 'bronze')
      3. source_type - Source type identifier (default: 'tiktok_comments')
    """
    
    comments_dir = "/data/raw/tiktok/comments"
    bronze_bucket = "bronze"
    source_type = "tiktok_comments"
    
    if len(sys.argv) >= 2:
        comments_dir = sys.argv[1]
    if len(sys.argv) >= 3:
        bronze_bucket = sys.argv[2]
    if len(sys.argv) >= 4:
        source_type = sys.argv[3]
    
    ingest_all_comment_files(comments_dir, bronze_bucket, source_type)

if __name__ == "__main__":
    main()
