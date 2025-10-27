"""
File Tracking Utilities with Checksum Support
Provides functions to track ingested files and detect content changes
"""

import hashlib
from pathlib import Path
from typing import Optional, Dict
from pyspark.sql import SparkSession


class FileTracker:
    """Track file ingestion with checksum-based deduplication"""
    
    def __init__(self, spark: SparkSession, jdbc_url: str, jdbc_properties: dict):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_properties = jdbc_properties
    
    @staticmethod
    def calculate_checksum(file_path: str) -> str:
        """
        Calculate MD5 checksum of file content
        
        Args:
            file_path: Absolute path to file
            
        Returns:
            MD5 hash as hex string
        """
        hash_md5 = hashlib.md5()
        
        try:
            with open(file_path, "rb") as f:
                # Read file in chunks to handle large files
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            print(f"❌ Error calculating checksum for {file_path}: {str(e)}")
            raise
    
    def is_file_processed(self, file_checksum: str) -> bool:
        """
        Check if file with this checksum has been successfully processed
        
        Args:
            file_checksum: MD5 checksum of file
            
        Returns:
            True if file already processed, False otherwise
        """
        query = f"""
            SELECT COUNT(*) as count 
            FROM file_ingestion_log 
            WHERE file_checksum = '{file_checksum}' 
            AND status = 'success'
        """
        
        try:
            result = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", query) \
                .option("user", self.jdbc_properties["user"]) \
                .option("password", self.jdbc_properties["password"]) \
                .option("driver", self.jdbc_properties["driver"]) \
                .load()
            
            count = result.first()['count']
            return count > 0
        except Exception as e:
            print(f"⚠️  Warning: Could not check file tracking: {str(e)}")
            return False
    
    def get_file_info(self, file_checksum: str) -> Optional[Dict]:
        """
        Get ingestion info for a file by checksum
        
        Args:
            file_checksum: MD5 checksum of file
            
        Returns:
            Dict with file info or None if not found
        """
        query = f"""
            SELECT file_path, file_name, ingestion_timestamp, records_ingested, table_name
            FROM file_ingestion_log 
            WHERE file_checksum = '{file_checksum}' 
            AND status = 'success'
            ORDER BY ingestion_timestamp DESC
            LIMIT 1
        """
        
        try:
            result = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", query) \
                .option("user", self.jdbc_properties["user"]) \
                .option("password", self.jdbc_properties["password"]) \
                .option("driver", self.jdbc_properties["driver"]) \
                .load()
            
            if result.isEmpty():
                return None
            
            row = result.first()
            return {
                'file_path': row['file_path'],
                'file_name': row['file_name'],
                'ingestion_timestamp': row['ingestion_timestamp'],
                'records_ingested': row['records_ingested'],
                'table_name': row['table_name']
            }
        except Exception as e:
            print(f"⚠️  Warning: Could not get file info: {str(e)}")
            return None
    
    def log_ingestion(
        self, 
        file_path: str,
        file_checksum: str,
        table_name: str,
        records_ingested: int = 0,
        status: str = 'success',
        error_message: str = None
    ):
        """
        Log file ingestion to tracking table
        
        Args:
            file_path: Full path to ingested file
            file_checksum: MD5 checksum of file
            table_name: Target Bronze table name
            records_ingested: Number of records ingested
            status: 'success', 'failed', or 'in_progress'
            error_message: Error description if failed
        """
        path_obj = Path(file_path)
        file_name = path_obj.name
        file_size = path_obj.stat().st_size if path_obj.exists() else 0
        
        # Escape single quotes in strings
        file_path_escaped = file_path.replace("'", "''")
        file_name_escaped = file_name.replace("'", "''")
        table_name_escaped = table_name.replace("'", "''")
        error_message_escaped = error_message.replace("'", "''") if error_message else ''
        
        insert_query = f"""
            INSERT INTO file_ingestion_log 
                (file_path, file_name, file_size_bytes, file_checksum, records_ingested, table_name, status, error_message)
            VALUES 
                ('{file_path_escaped}', '{file_name_escaped}', {file_size}, '{file_checksum}', 
                 {records_ingested}, '{table_name_escaped}', '{status}', 
                 {'NULL' if error_message is None else f"'{error_message_escaped}'"})
            ON CONFLICT (file_checksum) DO UPDATE SET
                ingestion_timestamp = CURRENT_TIMESTAMP,
                records_ingested = EXCLUDED.records_ingested,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message
        """
        
        try:
            # Execute insert via JDBC
            self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", f"({insert_query}) AS tmp") \
                .option("user", self.jdbc_properties["user"]) \
                .option("password", self.jdbc_properties["password"]) \
                .option("driver", self.jdbc_properties["driver"]) \
                .load()
            
            print(f"✅ Logged ingestion: {file_name} ({records_ingested} records)")
        except Exception as e:
            print(f"⚠️  Warning: Could not log ingestion: {str(e)}")
    
    def should_process_file(self, file_path: str) -> tuple[bool, Optional[str], Optional[Dict]]:
        """
        Determine if file should be processed based on checksum
        
        Args:
            file_path: Path to file to check
            
        Returns:
            Tuple of (should_process, checksum, previous_info)
            - should_process: True if file is new or changed
            - checksum: MD5 hash of current file
            - previous_info: Dict with info from previous ingestion (if exists)
        """
        try:
            # Calculate current checksum
            current_checksum = self.calculate_checksum(file_path)
            
            # Check if already processed
            if self.is_file_processed(current_checksum):
                previous_info = self.get_file_info(current_checksum)
                return False, current_checksum, previous_info
            
            return True, current_checksum, None
        except Exception as e:
            print(f"❌ Error checking file {file_path}: {str(e)}")
            raise


def print_skip_message(file_path: str, previous_info: Optional[Dict]):
    """Print formatted skip message with previous ingestion info"""
    file_name = Path(file_path).name
    
    if previous_info:
        print(f"⏭️  SKIP: {file_name}")
        print(f"   Already processed: {previous_info['ingestion_timestamp']}")
        print(f"   Records: {previous_info['records_ingested']} | Table: {previous_info['table_name']}")
    else:
        print(f"⏭️  SKIP: {file_name} (already processed)")
