"""
Bronze Layer - RAW CSV Ingestion: TikTok Videos
Source: /data/raw/tiktok/links/merged_videos.csv
Target: s3a://bronze/tiktok/raw/*.csv (versioned by timestamp + checksum)

Purpose:
- Copy raw CSV AS-IS (no transformation)
- Add filename with timestamp + checksum for versioning
- Track in PostgreSQL file_ingestion_log (layer='bronze')
- Skip if file with same checksum already ingested
"""

import sys
import os
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


def ingest_raw_csv_to_bronze(source_path: str, bronze_bucket: str, source_type: str):
    """Ingest RAW TikTok videos CSV to Bronze layer"""
    
    spark = get_spark_session(app_name=f"Bronze_RAW_Ingest_{source_type}")
    
    try:
        file_name = os.path.basename(source_path)
        file_size = os.path.getsize(source_path)
        
        print(f"=" * 80)
        print(f"📥 BRONZE RAW INGESTION: TikTok Videos")
        print(f"=" * 80)
        print(f"📁 Source: {source_path}")
        
        # Calculate checksum
        print(f"\n1️⃣  Calculating checksum...")
        file_checksum = calculate_file_checksum(source_path)
        print(f"   🔐 Checksum: {file_checksum}")
        
        # Check duplicate
        print(f"\n2️⃣  Checking if already ingested...")
        if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='bronze'):
            print(f"   ⏭️  Already exists - Skipping")
            return {'status': 'skipped'}
        print(f"   ✅ New file - Proceeding")
        
        # Read CSV raw
        print(f"\n3️⃣  Reading CSV...")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("encoding", "UTF-8") \
            .csv(source_path)
        
        record_count = df.count()
        columns = df.columns
        print(f"   📊 Records: {record_count}")
        print(f"   📋 Columns: {len(columns)}")
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(file_name)[0]
        new_filename = f"{base_name}_{timestamp}_{file_checksum[:8]}.csv"
        
        # Upload ORIGINAL FILE directly
        bronze_output = f"s3a://{bronze_bucket}/lakehouse/{source_type}/raw/{new_filename}"
        print(f"\n4️⃣  Uploading ORIGINAL file to Bronze...")
        print(f"   📁 Target: {bronze_output}")
        
        # Use single file uploader
        upload_result = copy_file_to_bronze_with_utf8(
            source_path=source_path,
            bronze_bucket=bronze_bucket,
            source_type=source_type,
            new_filename=new_filename
        )
        
        if upload_result['status'] != 'success':
            raise Exception(f"Upload failed: {upload_result.get('error', 'Unknown error')}")
        
        bronze_output = upload_result['location']
        print(f"   ✅ Write completed")
        
        # Log to PostgreSQL
        print(f"\n5️⃣  Logging to PostgreSQL...")
        log_ingestion_to_postgres(
            file_path=source_path,
            file_checksum=file_checksum,
            records_ingested=record_count,
            table_name=f"{source_type}_raw",
            status='success',
            layer='bronze',
            postgres_conn_params=POSTGRES_CONN,
            ingestion_details={
                "source_format": "csv",
                "columns": columns,
                "original_filename": file_name,
                "bronze_location": bronze_output,
                "file_size_mb": round(file_size / 1024 / 1024, 2)
            },
            file_size_bytes=file_size
        )
        
        print(f"\n" + "=" * 80)
        print(f"✅ BRONZE INGESTION COMPLETED")
        print(f"   Records: {record_count} | Version: {timestamp}")
        print(f"=" * 80)
        
        return {'status': 'success', 'filename': new_filename, 'records': record_count}
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
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
        raise
    finally:
        spark.stop()


def main():
    source_path = "/data/raw/tiktok/links/merged_videos.csv"
    bronze_bucket = "bronze"
    source_type = "tiktok_videos"
    
    if len(sys.argv) >= 2:
        source_path = sys.argv[1]
    if len(sys.argv) >= 3:
        bronze_bucket = sys.argv[2]
    if len(sys.argv) >= 4:
        source_type = sys.argv[3]
    
    result = ingest_raw_csv_to_bronze(source_path, bronze_bucket, source_type)
    
    # Print result for test scripts
    if result and result.get('status') == 'skipped':
        print("\n⏭️  SKIPPED - File already ingested")
        sys.exit(0)


if __name__ == "__main__":
    main()
