"""
Iceberg Utilities
Helper functions for working with Iceberg tables
"""

import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_database(spark, database_name, location=None):
    """
    Create a database in the Iceberg catalog
    
    Args:
        spark: SparkSession
        database_name (str): Name of the database
        location (str): Optional S3 location for the database
    """
    try:
        if location:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse.{database_name} LOCATION '{location}'")
        else:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse.{database_name}")
        
        logger.info(f"✅ Database created/verified: lakehouse.{database_name}")
    except Exception as e:
        logger.error(f"❌ Failed to create database {database_name}: {str(e)}")
        raise


def create_iceberg_table_if_not_exists(spark, database, table_name, schema, partition_by=None, table_properties=None):
    """
    Create an Iceberg table if it doesn't exist
    
    Args:
        spark: SparkSession
        database (str): Database name (e.g., 'bronze')
        table_name (str): Table name (e.g., 'tiktok_videos_metadata')
        schema: StructType schema for the table
        partition_by (list): List of column names to partition by
        table_properties (dict): Additional table properties
    """
    full_table_name = f"lakehouse.{database}.{table_name}"
    
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
        spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse.{database}")
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
    
    # Try CREATE TABLE IF NOT EXISTS first
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
        {columns_str}
    )
    USING iceberg
    {partition_clause}
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


def create_iceberg_table(spark, table_name, df, partition_by=None, mode="overwrite"):
    """
    Create an Iceberg table from a DataFrame
    
    Args:
        spark: SparkSession
        table_name (str): Full table name (e.g., 'lakehouse.bronze.hotels')
        df: DataFrame to write
        partition_by (list): List of column names to partition by
        mode (str): Write mode ('overwrite', 'append', 'errorifexists')
    """
    try:
        writer = df.writeTo(table_name).using("iceberg")
        
        if partition_by:
            writer = writer.partitionedBy(*partition_by)
        
        if mode == "overwrite":
            writer.createOrReplace()
        elif mode == "append":
            writer.append()
        else:
            writer.create()
        
        logger.info(f"✅ Iceberg table created: {table_name}")
        logger.info(f"   Mode: {mode}")
        logger.info(f"   Rows: {df.count()}")
        if partition_by:
            logger.info(f"   Partitioned by: {', '.join(partition_by)}")
    
    except Exception as e:
        logger.error(f"❌ Failed to create table {table_name}: {str(e)}")
        raise


def read_iceberg_table(spark, table_name):
    """
    Read an Iceberg table
    
    Args:
        spark: SparkSession
        table_name (str): Full table name
        
    Returns:
        DataFrame
    """
    try:
        df = spark.table(table_name)
        logger.info(f"✅ Read Iceberg table: {table_name}")
        logger.info(f"   Rows: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"❌ Failed to read table {table_name}: {str(e)}")
        raise


def get_table_snapshots(spark, table_name):
    """
    Get snapshot history of an Iceberg table
    
    Args:
        spark: SparkSession
        table_name (str): Full table name
        
    Returns:
        DataFrame with snapshot information
    """
    try:
        snapshots = spark.sql(f"SELECT * FROM lakehouse.{table_name}.snapshots")
        logger.info(f"✅ Retrieved snapshots for: {table_name}")
        return snapshots
    except Exception as e:
        logger.error(f"❌ Failed to get snapshots for {table_name}: {str(e)}")
        raise


def show_table_info(spark, table_name):
    """
    Display detailed information about an Iceberg table
    
    Args:
        spark: SparkSession
        table_name (str): Full table name
    """
    try:
        print(f"\n{'='*60}")
        print(f"📊 Table Information: {table_name}")
        print(f"{'='*60}")
        
        # Schema
        print("\n📋 Schema:")
        spark.sql(f"DESCRIBE {table_name}").show(truncate=False)
        
        # Partitions
        print("\n🗂️  Partitions:")
        spark.sql(f"SHOW PARTITIONS {table_name}").show(truncate=False)
        
        # Table properties
        print("\n⚙️  Table Properties:")
        spark.sql(f"SHOW TBLPROPERTIES {table_name}").show(truncate=False)
        
        # Snapshots
        print("\n📸 Snapshots (History):")
        spark.sql(f"SELECT * FROM lakehouse.{table_name}.snapshots").show(truncate=False)
        
        # Row count
        count = spark.table(table_name).count()
        print(f"\n📈 Total rows: {count:,}")
        
        logger.info(f"✅ Table info displayed for: {table_name}")
    
    except Exception as e:
        logger.error(f"❌ Failed to show table info: {str(e)}")
        raise


def time_travel_query(spark, table_name, snapshot_id=None, timestamp=None):
    """
    Query an Iceberg table at a specific snapshot or timestamp
    
    Args:
        spark: SparkSession
        table_name (str): Full table name
        snapshot_id (int): Snapshot ID to query
        timestamp (str): Timestamp in format 'YYYY-MM-DD HH:MM:SS'
        
    Returns:
        DataFrame
    """
    try:
        if snapshot_id:
            df = spark.read \
                .option("snapshot-id", snapshot_id) \
                .table(table_name)
            logger.info(f"✅ Time travel query: {table_name} @ snapshot {snapshot_id}")
        elif timestamp:
            df = spark.read \
                .option("as-of-timestamp", timestamp) \
                .table(table_name)
            logger.info(f"✅ Time travel query: {table_name} @ {timestamp}")
        else:
            raise ValueError("Either snapshot_id or timestamp must be provided")
        
        return df
    
    except Exception as e:
        logger.error(f"❌ Time travel query failed: {str(e)}")
        raise


if __name__ == "__main__":
    print("Iceberg utilities module loaded successfully")
