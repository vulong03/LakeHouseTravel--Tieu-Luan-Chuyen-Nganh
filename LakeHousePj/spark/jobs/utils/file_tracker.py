"""
File Tracking Utilities with Checksum Support
Provides functions to track ingested files and detect content changes
"""

import hashlib
from pathlib import Path
from typing import Optional, Dict, Tuple

# =============================================================================
# Standalone Functions for Bronze Layer Ingestion
# =============================================================================

def calculate_file_checksum(file_path: str) -> str:
    """
    Calculate MD5 checksum for a file.
    Standalone version for use in ingestion scripts.
    
    Args:
        file_path: Full path to the file
        
    Returns:
        MD5 hash as hex string
    """
    import hashlib
    
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def check_if_file_ingested(file_checksum: str, postgres_conn_params: dict, layer: str) -> bool:
    """
    Check if a file with the given checksum has already been ingested for a specific layer.
    
    Args:
        file_checksum: MD5 hash of the file
        postgres_conn_params: Dict with host, port, database, user, password
        layer: Data layer to check ('bronze', 'silver', or 'gold') - REQUIRED, no default
        
    Returns:
        bool: True if file already ingested in this layer, False otherwise
    """
    import psycopg2
    
    try:
        conn = psycopg2.connect(**postgres_conn_params)
        cursor = conn.cursor()
        
        query = """
            SELECT COUNT(*) FROM file_ingestion_log 
            WHERE file_checksum = %s AND layer = %s AND status = 'success'
        """
        cursor.execute(query, (file_checksum, layer))
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count > 0
    except Exception as e:
        print(f"Error checking file ingestion status: {str(e)}")
        return False


def log_ingestion_to_postgres(
    file_path: str,
    file_checksum: str,
    records_ingested: int,
    table_name: str,
    status: str,
    postgres_conn_params: dict,
    layer: str,
    error_message: str = None,
    ingestion_details: dict = None,
    file_size_bytes: int = 0
):
    """
    Log file ingestion details to PostgreSQL tracking table.
    Supports optional ingestion_details for multi-table ingestion tracking.
    
    Args:
        file_path: Full path to the ingested file
        file_checksum: MD5 hash of the file
        records_ingested: Number of records ingested (total across all tables)
        table_name: Primary target table name (e.g., 'booking_hotels_list_raw' for bronze)
        status: 'success', 'failed', or 'in_progress'
        postgres_conn_params: Dict with host, port, database, user, password
        layer: Data layer ('bronze', 'silver', or 'gold') - REQUIRED, no default
        error_message: Optional error message if status='failed'
        ingestion_details: Optional dict for multi-table ingestion breakdown or processing metadata
                          Example: {
                              "tables": [
                                  {"name": "raw_tiktok_post_metadata", "records": 1},
                                  {"name": "raw_tiktok_post_comments", "records": 150}
                              ]
                          }
        file_size_bytes: Optional file size in bytes (default 0 if not provided)
    """
    import psycopg2
    import json
    import os
    from datetime import datetime
    
    try:
        # DEBUG: Show connection attempt
        print(f"Connecting to PostgreSQL: {postgres_conn_params.get('host')}:{postgres_conn_params.get('port')}/{postgres_conn_params.get('database')}")
        
        conn = psycopg2.connect(**postgres_conn_params)
        cursor = conn.cursor()
        
        file_name = os.path.basename(file_path)
        # Use the provided file_size_bytes parameter (calculated from S3/HDFS)
        # instead of os.path.getsize which doesn't work for S3 paths
        
        # Convert ingestion_details dict to JSON string for PostgreSQL JSONB column
        ingestion_details_json = json.dumps(ingestion_details) if ingestion_details else None
        
        insert_query = """
            INSERT INTO file_ingestion_log (
                file_path, file_name, file_size_bytes, file_checksum,
                layer, ingestion_timestamp, records_ingested, table_name, 
                status, error_message, ingestion_details
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (file_checksum, layer) DO UPDATE SET
                ingestion_timestamp = EXCLUDED.ingestion_timestamp,
                records_ingested = EXCLUDED.records_ingested,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                ingestion_details = EXCLUDED.ingestion_details,
                file_size_bytes = EXCLUDED.file_size_bytes
        """
        
        cursor.execute(insert_query, (
            file_path,
            file_name,
            file_size_bytes,
            file_checksum,
            layer,
            datetime.now(),
            records_ingested,
            table_name,
            status,
            error_message,
            ingestion_details_json
        ))
        
        conn.commit()
        print(f"PostgreSQL commit successful")
        
        cursor.close()
        conn.close()
        
        print(f"✅ Logged ingestion ({layer}): {file_name} → {table_name} ({records_ingested} records)")
        if ingestion_details:
            print(f"   Details: {ingestion_details}")
            
    except Exception as e:
        print(f"❌ Error logging to PostgreSQL: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
