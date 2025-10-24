"""
Utility functions for MERGE/UPSERT operations in Bronze layer

Functions:
- calculate_row_checksum: Generate MD5 hash for business columns
- merge_into_bronze: Execute MERGE INTO statement
- get_merge_stats: Get INSERT/UPDATE/SKIP statistics
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import md5, concat_ws, col, lit
from typing import List, Dict


def calculate_row_checksum(df: DataFrame, business_columns: List[str]) -> DataFrame:
    """
    Calculate MD5 checksum for each row based on business columns only.
    
    Args:
        df: Input DataFrame
        business_columns: List of column names to include in checksum
                         (exclude metadata: ingestion_timestamp, source_file, etc.)
    
    Returns:
        DataFrame with additional 'row_checksum' column
    
    Example:
        df = calculate_row_checksum(df, ["hotel_name", "hotel_url", "province"])
    """
    # Concatenate all business columns with delimiter, then hash
    # coalesce handles NULL values (replace with empty string)
    from pyspark.sql.functions import coalesce
    
    cols_to_hash = [coalesce(col(c).cast("string"), lit("")) for c in business_columns]
    
    return df.withColumn(
        "row_checksum",
        md5(concat_ws("|", *cols_to_hash))
    )


def merge_into_bronze(
    spark: SparkSession,
    new_data_df: DataFrame,
    target_table: str,
    business_key: str,
    business_columns: List[str]
) -> Dict[str, int]:
    """
    MERGE new data into Bronze table using UPSERT logic.
    
    Strategy:
    - WHEN MATCHED AND row_checksum changed → UPDATE (data + metadata)
    - WHEN MATCHED AND row_checksum unchanged → DO NOTHING (SKIP)
    - WHEN NOT MATCHED → INSERT
    
    Args:
        spark: SparkSession
        new_data_df: DataFrame with new data (must have row_checksum column)
        target_table: Full table name (e.g., "lakehouse.bronze.raw_booking_hotels_list")
        business_key: Column name to match records (e.g., "hotel_url")
        business_columns: List of business column names (for UPDATE SET clause)
    
    Returns:
        Dictionary with stats: {"inserted": 10, "updated": 5, "skipped": 2}
    """
    
    # Create temp view for source data
    temp_view_name = "merge_source_temp"
    new_data_df.createOrReplaceTempView(temp_view_name)
    
    # Build UPDATE SET clause
    update_set_clauses = []
    for col_name in business_columns:
        update_set_clauses.append(f"target.{col_name} = source.{col_name}")
    
    # Add metadata columns to UPDATE
    update_set_clauses.extend([
        "target.row_checksum = source.row_checksum",
        "target.ingestion_timestamp = source.ingestion_timestamp",
        "target.source_file = source.source_file",
        "target.source_file_checksum = source.source_file_checksum"
    ])
    
    update_set_clause = ",\n            ".join(update_set_clauses)
    
    # Build INSERT VALUES clause
    all_columns = business_columns + ["row_checksum", "ingestion_timestamp", "source_file", "source_file_checksum"]
    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join([f"source.{col}" for col in all_columns])
    
    # MERGE SQL statement
    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING {temp_view_name} AS source
    ON target.{business_key} = source.{business_key}
    
    WHEN MATCHED AND target.row_checksum != source.row_checksum THEN
        UPDATE SET
            {update_set_clause}
    
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    
    print(f"📝 Executing MERGE statement...")
    print(f"   Business Key: {business_key}")
    print(f"   Target Table: {target_table}")
    
    # Get counts before merge
    try:
        before_count = spark.table(target_table).count()
        target_exists = True
    except:
        before_count = 0
        target_exists = False
    
    source_count = new_data_df.count()
    
    # Calculate statistics BEFORE merge by comparing checksums
    inserted = 0
    updated = 0
    skipped = 0
    
    if target_exists:
        # Join source with target to compare checksums
        target_df = spark.table(target_table).select(business_key, "row_checksum")
        
        # Find records that will be INSERTED (not in target)
        new_records = new_data_df.join(
            target_df, 
            new_data_df[business_key] == target_df[business_key], 
            "left_anti"
        )
        inserted = new_records.count()
        
        # Find records that will be UPDATED (checksum changed)
        changed_records = new_data_df.alias("src").join(
            target_df.alias("tgt"),
            (new_data_df[business_key] == target_df[business_key]) & 
            (new_data_df["row_checksum"] != target_df["row_checksum"]),
            "inner"
        )
        updated = changed_records.count()
        
        # SKIPPED = existing records with SAME checksum
        skipped = source_count - inserted - updated
    else:
        # Table doesn't exist - all records will be inserted
        inserted = source_count
        updated = 0
        skipped = 0
    
    # Execute MERGE
    spark.sql(merge_sql)
    
    stats = {
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "total_processed": source_count
    }
    
    return stats


def print_merge_stats(stats: Dict[str, int]):
    """Pretty print MERGE statistics"""
    print(f"\n📊 MERGE Statistics:")
    print(f"   Total records processed: {stats['total_processed']}")
    print(f"   - New records INSERTED:  {stats['inserted']}")
    print(f"   - Existing records UPDATED: {stats['updated']}")
    print(f"   - Records SKIPPED (unchanged): {stats['skipped']}")
