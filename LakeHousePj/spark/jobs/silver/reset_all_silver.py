"""
RESET ALL SILVER LAYER - Complete Cleanup
- Delete ALL Silver Iceberg tables data
- Clear ALL PostgreSQL tracking logs for Silver layer
- Clean ALL scratch bucket tmp folders
- Drop and recreate ALL Silver tables
WARNING: This will delete ALL data in Silver layer!
"""

import sys
import psycopg2

sys.path.append('/opt/spark/jobs')

from utils.spark_session import get_spark_session

# PostgreSQL connection
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}

SILVER_DATABASE = "silver"

# All Silver tables to reset
SILVER_TABLES = [
    "hotels_list",
    "hotels_detail",
    "hotels_reviews",
    "tiktok_videos",
    "tiktok_post_metadata",
    "tiktok_post_comments"
]

def clear_all_postgres_tracking():
    """Clear ALL PostgreSQL tracking logs for Silver layer"""
    print(f"\n{'=' * 80}")
    print(f"1. CLEARING POSTGRESQL TRACKING LOGS")
    print(f"{'=' * 80}")
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()

        # Count all Silver tracking records
        cursor.execute("""
            SELECT COUNT(*) FROM file_ingestion_log 
            WHERE layer = 'silver'
        """)
        count_before = cursor.fetchone()[0]
        
        print(f"Found {count_before} tracking records in Silver layer")
        
        if count_before > 0:
            # Show breakdown by table
            cursor.execute("""
                SELECT table_name, COUNT(*) as count 
                FROM file_ingestion_log 
                WHERE layer = 'silver'
                GROUP BY table_name
                ORDER BY count DESC
            """)
            
            print(f"Breakdown by table:")
            for row in cursor.fetchall():
                table_name, count = row
                print(f"      • {table_name}: {count} records")
            
            # Delete ALL Silver tracking records
            print(f"Deleting all Silver tracking records...")
            cursor.execute("""
                DELETE FROM file_ingestion_log 
                WHERE layer = 'silver'
            """)
            conn.commit()
            
            print(f"Deleted {count_before} tracking records")
        else:
            print(f"No tracking records to delete")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Warning: Could not clear tracking: {e}")
        print(f"(This is OK if table doesn't exist yet)")

def delete_all_silver_tables_data(spark):
    """Delete all data from ALL Silver Iceberg tables"""
    print(f"\n{'=' * 80}")
    print(f"2. DELETING ALL SILVER TABLES DATA")
    print(f"{'=' * 80}")
    
    try:
        # Create Silver database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS silver.{SILVER_DATABASE}")
        
        # Get all tables in Silver database
        tables = spark.sql(f"SHOW TABLES IN silver.{SILVER_DATABASE}").collect()
        existing_tables = [t.tableName for t in tables]
        
        print(f"Found {len(existing_tables)} tables in Silver database:")
        for table in existing_tables:
            print(f" • {table}")
        
        if not existing_tables:
            print(f"No tables found in Silver database")
            return
        
        print(f"Deleting data from all tables...")
        
        total_deleted = 0
        for table_name in existing_tables:
            full_table = f"silver.{SILVER_DATABASE}.{table_name}"  # Use 'silver' catalog
            
            try:
                # Count records
                count_before = spark.sql(f"SELECT COUNT(*) as count FROM {full_table}").collect()[0]['count']
                
                if count_before > 0:
                    print(f"\n{table_name}:")
                    print(f"Records before: {count_before:,}")
                    
                    # Delete all records
                    spark.sql(f"DELETE FROM {full_table} WHERE 1=1")
                    
                    # Verify
                    count_after = spark.sql(f"SELECT COUNT(*) as count FROM {full_table}").collect()[0]['count']
                    print(f"Records after: {count_after:,}")
                    print(f"Deleted: {count_before:,} records")
                    
                    total_deleted += count_before
                else:
                    print(f"{table_name}: Already empty ✓")
                    
            except Exception as e:
                print(f"{table_name}:  Error - {e}")
        
        print(f"\n{'=' * 70}")
        print(f"TOTAL DELETED: {total_deleted:,} records from {len(existing_tables)} tables")
        print(f"{'=' * 70}")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

def drop_all_silver_tables(spark):
    """Drop ALL Silver tables"""
    print(f"\n{'=' * 80}")
    print(f"3. DROPPING ALL SILVER TABLES")
    print(f"{'=' * 80}")
    
    try:
        # Create Silver database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS silver.{SILVER_DATABASE}")
        
        # Get all tables in Silver database
        tables = spark.sql(f"SHOW TABLES IN silver.{SILVER_DATABASE}").collect()
        existing_tables = [t.tableName for t in tables]
        
        if not existing_tables:
            print(f"No tables to drop")
            return
        
        print(f"Dropping {len(existing_tables)} tables...")
        
        dropped_count = 0
        for table_name in existing_tables:
            full_table = f"silver.{SILVER_DATABASE}.{table_name}"  # Use 'silver' catalog
            
            try:
                # Use PURGE to force delete data files
                spark.sql(f"DROP TABLE IF EXISTS {full_table} PURGE")
                print(f"Dropped: {table_name} (with PURGE)")
                dropped_count += 1
            except Exception as e:
                print(f"Could not drop {table_name}: {e}")
        
        print(f"Dropped {dropped_count} tables")
        
        # Verify Silver database is empty
        remaining = spark.sql(f"SHOW TABLES IN {SILVER_DATABASE}").collect()
        if remaining:
            print(f"Warning: {len(remaining)} tables still remain:")
            for t in remaining:
                print(f" • {t.tableName}")
        else:
            print(f"Silver database is now EMPTY")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

def cleanup_silver_minio_bucket(spark):
    """FORCE DELETE all files in Silver MinIO bucket"""
    print(f"\n{'=' * 80}")
    print(f"4. FORCE CLEANING SILVER MINIO BUCKET")
    print(f"{'=' * 80}")
    print(f"\nManually deleting ALL files in s3a://silver/lakehouse/...")
    
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI("s3a://silver"),
            hadoop_conf
        )
        
        # Path to Silver lakehouse folder (using new catalog configuration)
        base_path = "s3a://silver/lakehouse"
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
        
        if fs.exists(hadoop_path):
            # List all table folders
            status_list = fs.listStatus(hadoop_path)
            folders = []
            
            for status in status_list:
                path_str = str(status.getPath())
                folder_name = path_str.split("/")[-1]
                folders.append(folder_name)
            
            print(f"Found {len(folders)} table folders to delete:")
            for folder in folders:
                print(f"  • {folder}")
            
            if len(folders) > 0:
                # Delete entire lakehouse folder (all tables)
                print(f"Deleting ALL table folders and data files...")
                fs.delete(hadoop_path, True)  # True = recursive
                print(f"FORCE DELETED: {base_path}")
                print(f"Removed {len(folders)} table folders with ALL Parquet/metadata files")
            else:
                print(f"No folders to delete")
        else:
            print(f"Path does not exist: {base_path}")
            
    except Exception as e:
        print(f"Warning: Could not cleanup MinIO: {e}")
        print(f"Try manual cleanup via MinIO Console: http://localhost:9001")

def cleanup_all_scratch_bucket(spark):
    """Clean up ALL scratch bucket tmp folders"""
    print(f"\n{'=' * 80}")
    print(f"5. CLEANING SCRATCH BUCKET")
    print(f"{'=' * 80}")
    
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI("s3a://scratch"),
            hadoop_conf
        )
        
        # Path to Silver pipeline folders
        base_path = "s3a://scratch/pipeline/silver"
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
        
        if fs.exists(hadoop_path):
            # List all folders
            status_list = fs.listStatus(hadoop_path)
            folders = [str(status.getPath()).split("/")[-1] for status in status_list]
            
            print(f"Found {len(folders)} table folders:")
            for folder in folders:
                print(f"      • {folder}")
            
            # Delete entire Silver pipeline folder
            print(f"Deleting all tmp folders...")
            fs.delete(hadoop_path, True)  # True = recursive
            print(f"Deleted: {base_path}")
            print(f"Removed {len(folders)} table folders with all run histories")
        else:
            print(f"No tmp folders found")
            
    except Exception as e:
        print(f"Warning: Could not cleanup scratch: {e}")

def cleanup_gold_scratch_bucket(spark):
    """Clean up Gold scratch bucket as well (if needed)"""
    print(f"\n{'=' * 80}")
    print(f"6. CLEANING GOLD SCRATCH BUCKET (Optional)")
    print(f"{'=' * 80}")
    
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI("s3a://scratch"),
            hadoop_conf
        )
        
        # Path to Gold pipeline folders
        base_path = "s3a://scratch/pipeline/gold"
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
        
        if fs.exists(hadoop_path):
            # List all folders
            status_list = fs.listStatus(hadoop_path)
            folders = [str(status.getPath()).split("/")[-1] for status in status_list]
            
            print(f"Found {len(folders)} Gold table folders:")
            for folder in folders:
                print(f"    • {folder}")
            
            # Delete entire Gold pipeline folder
            print(f"Deleting all Gold tmp folders...")
            fs.delete(hadoop_path, True)  # True = recursive
            print(f"Deleted: {base_path}")
        else:
            print(f"No Gold tmp folders found")
            
    except Exception as e:
        print(f"Info: {e}")

def main():
    print("\n" + "=" * 80)
    print("RESET ALL SILVER LAYER")
    print("=" * 80)
    print("\nTHIS WILL DELETE EVERYTHING IN SILVER LAYER! \n")
    print("Actions:")
    print("   1. Clear ALL PostgreSQL tracking logs (layer='silver')")
    print("   2. Delete ALL data from Silver Iceberg tables")
    print("   3. Drop ALL Silver tables")
    print("   4. Clean ALL scratch bucket tmp folders")
    print("   5. Clean Gold scratch bucket (optional)")
    print("\n" + "=" * 80)
    print("THIS IS A DESTRUCTIVE OPERATION - NO UNDO!")
    print("=" * 80 + "\n")
    
    spark = None
    
    try:
        # Step 1: Clear PostgreSQL tracking (no Spark needed)
        clear_all_postgres_tracking()
        
        # Step 2-5: Spark operations
        print(f"\n Starting Spark session...")
        spark = get_spark_session(app_name="Reset_All_Silver_Layer")
        
        delete_all_silver_tables_data(spark)
        drop_all_silver_tables(spark)
        cleanup_silver_minio_bucket(spark)  # NEW: Force delete MinIO files
        cleanup_all_scratch_bucket(spark)
        cleanup_gold_scratch_bucket(spark)
        
        # Final summary
        print(f"\n" + "=" * 80)
        print(f"COMPLETE RESET SUCCESSFUL")
        print(f"=" * 80)
        print(f"")
        print(f"Summary:")
        print(f"   • PostgreSQL tracking: ALL Silver records cleared")
        print(f"   • Silver tables: ALL dropped (with PURGE)")
        print(f"   • MinIO Silver bucket: ALL data files FORCE DELETED")
        print(f"   • Scratch bucket: ALL tmp folders cleaned")
        print(f"   • Gold scratch: Cleaned (if existed)")
        print(f"")
        print(f"Silver layer is now completely clean!")
        print(f"")
        print(f"Next steps:")
        print(f"   1. Re-run Bronze ingestion (if needed)")
        print(f"   2. Run Silver pipelines for each table:")
        print(f"      • hotels_list")
        print(f"      • hotels_detail")
        print(f"      • hotels_reviews")
        print(f"      • tiktok_videos")
        print(f"      • tiktok_post_metadata")
        print(f"      • tiktok_post_comments")
        print(f"")
        print(f"=" * 80)
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print(f"RESET FAILED")
        print(f"{'=' * 80}")
        print(f"Error: {e}")
        print("")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()
            
if __name__ == "__main__":
    main()
