"""
Bronze Layer - RAW CSV Ingestion: Booking Hotels List
Source: /data/raw/booking/vietnam_hotels_list.csv
Target: s3a://bronze/booking/raw/*.csv (versioned by timestamp + checksum)

Purpose:
- Copy raw CSV AS-IS (no transformation, no validation)
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
from pyspark.sql.functions import current_timestamp, lit

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
    Ingest RAW CSV to Bronze layer
    
    Args:
        source_path: Local path to CSV file (e.g., /data/raw/booking/vietnam_hotels_list.csv)
        bronze_bucket: MinIO bucket name (e.g., 'bronze')
        source_type: Data source identifier (e.g., 'booking_hotels_list')
    """
    
    spark = get_spark_session(app_name=f"Bronze_RAW_Ingest_{source_type}")
    
    try:
        # Get file info
        file_name = os.path.basename(source_path)
        file_size = os.path.getsize(source_path)
        
        print(f"=" * 80)
        print(f"📥 BRONZE RAW INGESTION: {file_name}")
        print(f"=" * 80)
        print(f"📁 Source: {source_path}")
        print(f"📊 Size: {file_size / 1024 / 1024:.2f} MB")
        
        # Step 1: Calculate checksum
        print(f"\n1️⃣  Calculating file checksum...")
        file_checksum = calculate_file_checksum(source_path)
        print(f"   🔐 Checksum: {file_checksum}")
        
        # Step 2: Check if already ingested
        print(f"\n2️⃣  Checking if file already ingested...")
        if check_if_file_ingested(file_checksum, POSTGRES_CONN, layer='bronze'):
            print(f"   ⏭️  File with checksum {file_checksum} already exists in Bronze")
            print(f"   ℹ️  Skipping ingestion (duplicate detected)")
            return {
                'status': 'skipped',
                'reason': 'duplicate_checksum',
                'checksum': file_checksum
            }
        print(f"   ✅ File is new - proceeding with ingestion")
        
        # Step 3: Read CSV raw (ONLY FOR VALIDATION)
        print(f"\n3️⃣  Validating CSV file...")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("encoding", "UTF-8") \
            .csv(source_path)
        
        record_count = df.count()
        columns = df.columns
        print(f"   📊 Records: {record_count}")
        print(f"   📋 Columns: {len(columns)} - {', '.join(columns[:5])}...")
        
        # Step 4: Generate timestamped filename
        print(f"\n4️⃣  Generating versioned filename...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(file_name)[0]
        new_filename = f"{base_name}_{timestamp}_{file_checksum[:8]}.csv"
        print(f"   📝 New filename: {new_filename}")
        
        # Step 5: Upload ORIGINAL FILE directly (no repartition)
        bronze_output = f"s3a://{bronze_bucket}/lakehouse/{source_type}/raw/{new_filename}"
        print(f"\n5️⃣  Uploading ORIGINAL file to Bronze...")
        print(f"   📁 Target: {bronze_output}")
        print(f"   ⚠️  Using direct file upload to preserve data integrity")
        
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
        
        # Step 6: Log to PostgreSQL
        print(f"\n6️⃣  Logging to PostgreSQL tracking table...")
        
        ingestion_details = {
            "source_format": "csv",
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
            ingestion_details=ingestion_details
        )
        
        print(f"\n" + "=" * 80)
        print(f"✅ BRONZE INGESTION COMPLETED")
        print(f"=" * 80)
        print(f"📊 Summary:")
        print(f"   - Records ingested: {record_count}")
        print(f"   - Checksum: {file_checksum}")
        print(f"   - Bronze location: {bronze_output}")
        print(f"   - Version: {timestamp}")
        print(f"=" * 80)
        
        return {
            'status': 'success',
            'filename': new_filename,
            'checksum': file_checksum,
            'records': record_count,
            'location': bronze_output
        }
        
    except Exception as e:
        print(f"\n❌ ERROR during Bronze ingestion:")
        print(f"   {str(e)}")
        
        # Log failure to PostgreSQL
        try:
            log_ingestion_to_postgres(
                file_path=source_path,
                file_checksum=file_checksum if 'file_checksum' in locals() else 'unknown',
                records_ingested=0,
                table_name=f"{source_type}_raw",
                status='failed',
                layer='bronze',
                error_message=str(e),
                postgres_conn_params=POSTGRES_CONN
            )
        except:
            pass
        
        raise
    finally:
        spark.stop()


def main():
    """Main entry point"""
    
    # Default paths (can be overridden by command line args)
    source_path = "/data/raw/booking/vietnam_hotels_list.csv"
    bronze_bucket = "bronze"
    source_type = "booking_hotels_list"
    
    # Override with command line args if provided
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
