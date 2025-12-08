"""
Gold Job Logging Utilities
Track Gold layer job executions in PostgreSQL
"""

from datetime import datetime
from typing import Optional, Dict
import json
import psycopg2


class GoldJobLogger:
    """Logger for Gold layer dimension/fact jobs"""
    
    def __init__(self, spark, jdbc_url: str, jdbc_properties: dict):
        """
        Initialize Gold job logger
        
        Args:
            spark: SparkSession
            jdbc_url: JDBC connection URL (jdbc:postgresql://host:port/database)
            jdbc_properties: JDBC connection properties (user, password, driver)
        """
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.jdbc_properties = jdbc_properties.copy()
        
        # Parse JDBC URL for psycopg2 connection
        # Format: jdbc:postgresql://host:port/database
        url_parts = jdbc_url.replace("jdbc:postgresql://", "").split("/")
        host_port = url_parts[0].split(":")
        self.jdbc_properties["host"] = host_port[0]
        self.jdbc_properties["port"] = int(host_port[1]) if len(host_port) > 1 else 5432
        self.jdbc_properties["database"] = url_parts[1] if len(url_parts) > 1 else "lakehouse"
    
    def log_job_success(self,
                       source_path: str,
                       table_name: str,
                       records_processed: int,
                       job_details: Optional[Dict] = None):
        """
        Log successful job completion
        
        Args:
            source_path: Source data path
            table_name: Target Gold table name
            records_processed: Number of records written
            job_details: Optional metadata (e.g., execution time, statistics)
        """
        try:
            # Calculate checksum for this run
            run_checksum = self._calculate_run_checksum(table_name)
            
            # Prepare details JSON
            details = job_details or {}
            details['completed_at'] = datetime.now().isoformat()
            details_json = json.dumps(details).replace("'", "''")
            
            # Update existing log or insert new
            update_query = f"""
                INSERT INTO file_ingestion_log (
                    file_path,
                    file_name,
                    file_checksum,
                    layer,
                    table_name,
                    records_ingested,
                    status,
                    ingestion_details
                ) VALUES (
                    '{source_path}',
                    '{source_path.split("/")[-1]}',
                    '{run_checksum}',
                    'gold',
                    '{table_name}',
                    {records_processed},
                    'success',
                    '{details_json}'::jsonb
                )
                ON CONFLICT (file_checksum, layer) 
                DO UPDATE SET
                    records_ingested = {records_processed},
                    status = 'success',
                    ingestion_timestamp = CURRENT_TIMESTAMP,
                    ingestion_details = '{details_json}'::jsonb
            """
            
            # Execute via JDBC using raw SQL with JSONB cast
            conn = psycopg2.connect(
                host=self.jdbc_properties["host"],
                port=self.jdbc_properties["port"],
                database=self.jdbc_properties["database"],
                user=self.jdbc_properties["user"],
                password=self.jdbc_properties["password"]
            )
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO file_ingestion_log (
                    file_path, file_name, file_checksum, layer,
                    table_name, records_ingested, status, ingestion_details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (file_checksum, layer) 
                DO UPDATE SET
                    records_ingested = EXCLUDED.records_ingested,
                    status = EXCLUDED.status,
                    ingestion_timestamp = CURRENT_TIMESTAMP,
                    ingestion_details = EXCLUDED.ingestion_details
            """, (
                source_path,
                source_path.split("/")[-1],
                run_checksum,
                'gold',
                table_name,
                records_processed,
                'success',
                details_json
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"✅ Gold job logged: {table_name} - {records_processed} records")
            
        except Exception as e:
            print(f"⚠️  Warning: Could not log job success: {str(e)}")
    
    def log_job_failure(self,
                       source_path: str,
                       table_name: str,
                       error_message: str):
        """
        Log job failure
        
        Args:
            source_path: Source data path
            table_name: Target Gold table name
            error_message: Error description
        """
        try:
            run_checksum = self._calculate_run_checksum(table_name)
            
            # Use psycopg2 for direct SQL execution
            conn = psycopg2.connect(
                host=self.jdbc_properties["host"],
                port=self.jdbc_properties["port"],
                database=self.jdbc_properties["database"],
                user=self.jdbc_properties["user"],
                password=self.jdbc_properties["password"]
            )
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO file_ingestion_log (
                    file_path, file_name, file_checksum, layer,
                    table_name, records_ingested, status, error_message, ingestion_details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL)
                ON CONFLICT (file_checksum, layer) 
                DO UPDATE SET
                    records_ingested = EXCLUDED.records_ingested,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    ingestion_timestamp = CURRENT_TIMESTAMP
            """, (
                source_path,
                source_path.split("/")[-1],
                run_checksum,
                'gold',
                table_name,
                0,
                'failed',
                error_message
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"❌ Gold job failed: {table_name}")
            
        except Exception as e:
            print(f"⚠️  Warning: Could not log job failure: {str(e)}")
    
    def _calculate_run_checksum(self, table_name: str) -> str:
        """
        Calculate checksum for job run
        Format: gold_<table>_<date>
        """
        import hashlib
        date_str = datetime.now().strftime("%Y%m%d")
        checksum_input = f"{table_name}_{date_str}"
        return hashlib.md5(checksum_input.encode()).hexdigest()


def get_gold_logger(spark):
    """
    Factory function to create GoldJobLogger with default connection
    
    Args:
        spark: SparkSession
        
    Returns:
        GoldJobLogger instance
    """
    jdbc_url = "jdbc:postgresql://postgres:5432/metastore_db"
    jdbc_properties = {
        "user": "lakehouse_user",
        "password": "lakehouse_pass",
        "driver": "org.postgresql.Driver"
    }
    
    return GoldJobLogger(spark, jdbc_url, jdbc_properties)

