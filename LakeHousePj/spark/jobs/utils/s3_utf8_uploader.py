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
