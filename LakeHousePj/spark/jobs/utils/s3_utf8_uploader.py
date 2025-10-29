"""
S3 UTF-8 Uploader Utility
Purpose: Upload files to MinIO/S3 with proper UTF-8 encoding
Workaround: Spark's .text() and .csv() writers don't always preserve UTF-8 correctly
"""

import subprocess
import os


def upload_file_to_s3_with_utf8(
    local_file_path: str,
    s3_bucket: str,
    s3_key: str,
    minio_endpoint: str = "http://minio:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin123"
) -> bool:
    """
    Upload a file to MinIO/S3 with guaranteed UTF-8 encoding
    
    Uses MinIO client (mc) which is pre-installed in Spark container
    and properly handles UTF-8 encoding.
    
    Args:
        local_file_path: Path to local file to upload
        s3_bucket: Target S3 bucket name (e.g., 'bronze')
        s3_key: S3 key/path (e.g., 'lakehouse/booking/raw/file.csv')
        minio_endpoint: MinIO endpoint URL
        access_key: MinIO access key
        secret_key: MinIO secret key
    
    Returns:
        bool: True if upload successful, False otherwise
    
    Example:
        upload_file_to_s3_with_utf8(
            local_file_path='/data/raw/vietnam_hotels.csv',
            s3_bucket='bronze',
            s3_key='lakehouse/booking_hotels_list/raw/vietnam_hotels_20251029_123456_abc123.csv'
        )
    """
    
    try:
        # Verify file exists and is readable
        if not os.path.exists(local_file_path):
            print(f"   ❌ ERROR: File not found: {local_file_path}")
            return False
        
        # Get file size for logging
        file_size_mb = os.path.getsize(local_file_path) / 1024 / 1024
        print(f"   📊 File size: {file_size_mb:.2f} MB")
        
        # Use MinIO client to upload DIRECTLY (stream, no memory load)
        # mc cp preserves file encoding and streams large files efficiently
        target_path = f"minio/{s3_bucket}/{s3_key}"
        
        # Set MC_CONFIG_DIR to writable location
        env = os.environ.copy()
        env['MC_CONFIG_DIR'] = '/tmp/.mc'
        
        # Configure MinIO alias (if not already configured)
        subprocess.run(
            ['mc', 'alias', 'set', 'minio', minio_endpoint, access_key, secret_key],
            capture_output=True,
            text=True,
            env=env
        )
        
        # Upload file DIRECTLY (stream mode, no memory load)
        print(f"   📤 Uploading {os.path.basename(local_file_path)} → {target_path}")
        result = subprocess.run(
            ['mc', 'cp', local_file_path, target_path],
            capture_output=True,
            text=True,
            env=env
        )
        
        if result.returncode == 0:
            print(f"   ✅ Uploaded: s3://{s3_bucket}/{s3_key}")
            return True
        else:
            print(f"   ❌ Upload failed (returncode={result.returncode})")
            print(f"   🔍 DEBUG: stderr={result.stderr}")
            return False
    
    except Exception as e:
        print(f"   ❌ ERROR during upload: {str(e)}")
        return False


def copy_file_to_bronze_with_utf8(
    source_path: str,
    bronze_bucket: str,
    source_type: str,
    new_filename: str,
    spark_session=None
) -> dict:
    """
    Copy a raw file to Bronze layer with UTF-8 encoding preservation
    
    This is the main function to be called from Bronze ingestion jobs.
    It handles both CSV and text files with proper UTF-8 encoding.
    
    Args:
        source_path: Local path to source file
        bronze_bucket: Bronze bucket name (e.g., 'bronze')
        source_type: Source type identifier (e.g., 'booking_hotels_list')
        new_filename: Versioned filename with timestamp and checksum
        spark_session: Optional Spark session (not used, for API compatibility)
    
    Returns:
        dict: Status info with 'status', 'location', 'method'
    
    Example:
        result = copy_file_to_bronze_with_utf8(
            source_path='/data/raw/booking/vietnam_hotels_list.csv',
            bronze_bucket='bronze',
            source_type='booking_hotels_list',
            new_filename='vietnam_hotels_list_20251029_123456_abc123.csv'
        )
    """
    
    try:
        # Construct S3 key
        s3_key = f"lakehouse/{source_type}/raw/{new_filename}"
        
        print(f"   📤 Uploading with UTF-8 encoding...")
        
        # Upload with UTF-8 preservation
        success = upload_file_to_s3_with_utf8(
            local_file_path=source_path,
            s3_bucket=bronze_bucket,
            s3_key=s3_key
        )
        
        if success:
            return {
                'status': 'success',
                'location': f"s3a://{bronze_bucket}/{s3_key}",
                'method': 'minio_client_utf8'
            }
        else:
            return {
                'status': 'failed',
                'error': 'Upload failed',
                'method': 'minio_client_utf8'
            }
    
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e),
            'method': 'minio_client_utf8'
        }


def upload_spark_df_to_s3_with_utf8(
    df,
    bronze_bucket: str,
    source_type: str,
    new_filename: str,
    file_format: str = 'csv',
    header: bool = True,
    num_partitions: int = 4  # Default: 4 parts for parallel processing
) -> dict:
    """
    Write Spark DataFrame to S3 with UTF-8 encoding - DISTRIBUTED MODE
    
    For CSV files that are already loaded into Spark DataFrames.
    Uses repartition() for parallel write, then uploads multiple part files to S3.
    
    Args:
        df: Spark DataFrame
        bronze_bucket: Bronze bucket name
        source_type: Source type identifier
        new_filename: Versioned filename (used as folder name)
        file_format: 'csv' or 'text'
        header: Include header for CSV (default: True)
        num_partitions: Number of part files (default: 4 for 4-core worker)
    
    Returns:
        dict: Status info with number of parts uploaded
    """
    
    try:
        # Remove .csv extension from filename to use as folder name
        base_name = new_filename.replace('.csv', '').replace('.txt', '')
        temp_path = f"/tmp/spark_temp_{base_name}"
        
        # Estimate data size for logging
        row_count = df.count()
        
        if file_format == 'csv':
            print(f"   📝 Writing DataFrame with DISTRIBUTED mode ({num_partitions} partitions)...")
            print(f"      Dataset: {row_count:,} rows → {num_partitions} part files")
            
            # Use repartition() instead of coalesce(1) for parallel write
            df.repartition(num_partitions).write \
                .mode("overwrite") \
                .option("header", str(header).lower()) \
                .option("encoding", "UTF-8") \
                .csv(temp_path)
            
            # Find all CSV part files (Spark creates part-*.csv or part-* without extension)
            import glob
            csv_files = glob.glob(f"{temp_path}/part-*.csv")
            if not csv_files:
                # Try without extension (some Spark versions don't add .csv)
                csv_files = [f for f in glob.glob(f"{temp_path}/part-*") if os.path.isfile(f)]
            
            if not csv_files:
                all_files = os.listdir(temp_path) if os.path.exists(temp_path) else []
                return {'status': 'failed', 'error': f'No CSV files generated. Found: {all_files}'}
            
            print(f"   ✅ Generated {len(csv_files)} part files (parallel write)")
        
        else:  # text format
            print(f"   📝 Writing DataFrame to temp text ({num_partitions} partitions)...")
            df.repartition(num_partitions).write \
                .mode("overwrite") \
                .text(temp_path)
            
            # Find all text part files
            import glob
            csv_files = [f for f in glob.glob(f"{temp_path}/part-*") if os.path.isfile(f)]
            
            if not csv_files:
                all_files = os.listdir(temp_path) if os.path.exists(temp_path) else []
                return {'status': 'failed', 'error': f'No text files generated. Found: {all_files}'}
            
            print(f"   ✅ Generated {len(csv_files)} part files")
        
        # Upload ALL part files to S3 (in a folder structure)
        s3_folder = f"lakehouse/{source_type}/raw/{base_name}"
        uploaded_parts = []
        total_size_mb = 0
        
        for i, part_file in enumerate(sorted(csv_files)):
            file_size_mb = os.path.getsize(part_file) / 1024 / 1024
            total_size_mb += file_size_mb
            
            # Generate part filename
            part_name = f"part-{i:05d}.csv" if file_format == 'csv' else f"part-{i:05d}.txt"
            s3_key = f"{s3_folder}/{part_name}"
            
            print(f"      [{i+1}/{len(csv_files)}] Uploading {part_name} ({file_size_mb:.2f} MB)...")
            
            # Upload with UTF-8 preservation
            success = upload_file_to_s3_with_utf8(
                local_file_path=part_file,
                s3_bucket=bronze_bucket,
                s3_key=s3_key
            )
            
            if success:
                uploaded_parts.append(s3_key)
            else:
                print(f"      ⚠️  Failed to upload {part_name}")
        
        # Cleanup temp files
        import shutil
        if os.path.exists(temp_path):
            shutil.rmtree(temp_path)
        
        if len(uploaded_parts) == len(csv_files):
            print(f"   ✅ Uploaded {len(uploaded_parts)} parts, Total: {total_size_mb:.2f} MB")
            return {
                'status': 'success',
                'location': f"s3a://{bronze_bucket}/{s3_folder}/",  # Folder path
                'method': 'spark_df_utf8_distributed',
                'num_parts': len(uploaded_parts),
                'total_size_mb': round(total_size_mb, 2),
                'rows': row_count
            }
        else:
            return {
                'status': 'partial',
                'uploaded': len(uploaded_parts),
                'total': len(csv_files),
                'location': f"s3a://{bronze_bucket}/{s3_folder}/"
            }
    
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e),
            'method': 'spark_df_utf8'
        }
