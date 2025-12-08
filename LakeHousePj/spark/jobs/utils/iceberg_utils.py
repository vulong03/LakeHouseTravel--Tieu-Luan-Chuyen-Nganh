"""
Iceberg Utilities
Helper functions for working with Iceberg tables
"""

import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_iceberg_table_if_not_exists(spark, database, table_name, schema, partition_by=None, table_properties=None, catalog="lakehouse"):
    """
    Create an Iceberg table if it doesn't exist
    
    Args:
        spark: SparkSession
        database (str): Database name (e.g., 'bronze', 'silver')
        table_name (str): Table name (e.g., 'tiktok_videos_metadata')
        schema: StructType schema for the table
        partition_by (list): List of column names to partition by
        table_properties (dict): Additional table properties
        catalog (str): Catalog name (default: 'lakehouse' for Bronze, use 'silver' for Silver layer)
    """
    full_table_name = f"{catalog}.{database}.{table_name}"
    
    # Try to check if table exists and is accessible
    try:
        table_exists = spark.catalog.tableExists(full_table_name)
        if table_exists:
            # Table exists and metadata is valid
            logger.info(f"✅ Table already exists: {full_table_name}")
            return
    except Exception as check_error:
        # Table metadata exists but is corrupted (points to deleted files)
        logger.warning(f"⚠️  Table {full_table_name} has corrupted metadata")
        logger.warning(f"   Error: {str(check_error)[:300]}")
        logger.info(f"   Will attempt to drop and recreate...")
    
    # At this point, either table doesn't exist OR has corrupted metadata
    # Create database if not exists
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
    except Exception as e:
        logger.warning(f"⚠️  Database creation warning: {str(e)[:200]}")
    
    # Build CREATE TABLE statement
    columns_ddl = []
    for field in schema.fields:
        null_constraint = "NOT NULL" if not field.nullable else ""
        columns_ddl.append(f"{field.name} {field.dataType.simpleString()} {null_constraint}".strip())
    
    columns_str = ",\n    ".join(columns_ddl)
    
    partition_clause = ""
    if partition_by:
        partition_clause = f"PARTITIONED BY ({', '.join(partition_by)})"
    
    properties_clause = ""
    if table_properties:
        props = [f"'{k}' = '{v}'" for k, v in table_properties.items()]
        properties_clause = f"TBLPROPERTIES ({', '.join(props)})"
    
    # Get warehouse location for the catalog
    warehouse_location = None
    try:
        if catalog == "silver":
            warehouse_location = "s3a://silver/lakehouse"
        elif catalog == "gold":
            warehouse_location = "s3a://gold/lakehouse"
        elif catalog == "bronze" or catalog == "lakehouse":
            warehouse_location = "s3a://bronze/lakehouse"
    except:
        pass
    
    location_clause = ""
    if warehouse_location:
        table_location = f"{warehouse_location}/{database}.db/{table_name}"
        location_clause = f"LOCATION '{table_location}'"
    
    # Try CREATE TABLE IF NOT EXISTS first
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        {columns_str}
    )
    USING iceberg
    {partition_clause}
    {location_clause}
    {properties_clause}
    """
    
    try:
        spark.sql(create_table_sql)
        logger.info(f"✅ Iceberg table created: {full_table_name}")
        if partition_by:
            logger.info(f"   Partitioned by: {', '.join(partition_by)}")
    except Exception as create_error:
        # CREATE IF NOT EXISTS failed (likely due to corrupted metadata)
        # Try CREATE OR REPLACE instead
        logger.warning(f"⚠️  CREATE IF NOT EXISTS failed, trying CREATE OR REPLACE...")
        logger.warning(f"   Error: {str(create_error)[:300]}")
        
        replace_table_sql = f"""
        CREATE OR REPLACE TABLE {full_table_name} (
            {columns_str}
        )
        USING iceberg
        {partition_clause}
        {location_clause}
        {properties_clause}
        """
        
        try:
            spark.sql(replace_table_sql)
            logger.info(f"✅ Iceberg table replaced: {full_table_name}")
            if partition_by:
                logger.info(f"   Partitioned by: {', '.join(partition_by)}")
        except Exception as replace_error:
            logger.error(f"❌ Failed to create/replace table {database}.{table_name}")
            logger.error(f"   Error: {str(replace_error)}")
            raise


if __name__ == "__main__":
    print("Iceberg utilities module loaded successfully")
