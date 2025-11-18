"""
Bronze Layer - RAW CSV Ingestion: Booking Hotels Detail
Source: /data/raw/booking/vietnam_hotels_detail.csv
Target: s3a://bronze/booking/raw/*.csv (versioned by timestamp + checksum)

Purpose:
- Copy raw CSV AS-IS (no transformation, multiLine support for long descriptions)
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
    """
    Ingest RAW CSV to Bronze layer (with multiLine support for detail descriptions)
    
    Args:
        source_path: Local path to CSV file
        bronze_bucket: MinIO bucket name
        source_type: Data source identifier
    """
    
    spark = get_spark_session(app_name=f"Bronze_RAW_Ingest_{source_type}")
    
    try:
        file_name = os.path.basename(source_path)
        file_size = os.path.getsize(source_path)
        
        print(f"=" * 80)
        print(f"📥 BRONZE RAW INGESTION: {file_name}")
        print(f"=" * 80)
        print(f"📁 Source: {source_path}")
        print(f"📊 Size: {file_size / 1024 / 1024:.2f} MB")
        
        # Calculate checksum
        print(f"\n1️⃣  Calculating file checksum...")
        file_checksum = calculate_file_checksum(source_path)
        print(f"   🔐 Checksum: {file_checksum}")
        
        # Check if already ingested
        print(f"\n2️⃣  Checking if file already ingested...")
        if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='bronze'):
            print(f"   ⏭️  File already exists in Bronze - Skipping")
            return {'status': 'skipped', 'checksum': file_checksum}
        print(f"   ✅ File is new - proceeding")
        
        # Read CSV raw (multiLine for descriptions) - ONLY FOR VALIDATION
        print(f"\n3️⃣  Validating CSV file (multiLine mode)...")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("multiLine", "true") \
            .option("escape", '"') \
            .option("encoding", "UTF-8") \
            .csv(source_path)
        
        record_count = df.count()
        columns = df.columns
        print(f"   📊 Records: {record_count}")
        print(f"   📋 Columns: {len(columns)}")
        
        # Generate timestamped filename
        print(f"\n4️⃣  Generating versioned filename...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(file_name)[0]
        new_filename = f"{base_name}_{timestamp}_{file_checksum[:8]}.csv"
        print(f"   📝 New filename: {new_filename}")
        
        # Upload ORIGINAL FILE directly (no repartition to preserve multiLine structure)
        bronze_output = f"s3a://{bronze_bucket}/lakehouse/{source_type}/raw/{new_filename}"
        print(f"\n5️⃣  Uploading ORIGINAL file to Bronze...")
        print(f"   📁 Target: {bronze_output}")
        print(f"   ⚠️  Using direct file upload (no DataFrame repartition) to preserve multiLine CSV structure")
        
        # Use single file uploader (preserves multiLine structure)
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
        print(f"\n6️⃣  Logging to PostgreSQL...")
        
        ingestion_details = {
            "source_format": "csv",
            "multiline_support": True,
            "columns": columns,
            "original_filename": file_name,
            "bronze_location": bronze_output,
            "file_size_mb": round(file_size / 1024 / 1024, 2)
        }
        
        log_ingestion_to_postgres(
            file_path=source_path,
            file_checksum=file_checksum,
            records_ingested=record_count,
            table_name=f"{source_type}_raw",
            status='success',
            layer='bronze',
            postgres_conn_params=POSTGRES_CONN,
            ingestion_details=ingestion_details,
            file_size_bytes=file_size
        )
        
        print(f"\n" + "=" * 80)
        print(f"✅ BRONZE INGESTION COMPLETED")
        print(f"=" * 80)
        print(f"   Records: {record_count} | Checksum: {file_checksum[:8]}")
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
    source_path = "/data/raw/booking/vietnam_hotels_detail.csv"
    bronze_bucket = "bronze"
    source_type = "booking_hotels_detail"
    
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
