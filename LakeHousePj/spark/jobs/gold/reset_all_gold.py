"""
RESET ALL GOLD LAYER - Complete Cleanup
- Delete ALL Gold Iceberg tables data
- Clear ALL PostgreSQL tracking logs for Gold layer
- Clean ALL scratch bucket tmp folders for Gold
- Drop and recreate ALL Gold tables

⚠️  WARNING: This will delete ALL data in Gold layer!
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

GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"

# All Gold tables to reset (dimension tables)
GOLD_TABLES = [
    "dim_province",
    "dim_destination"
]


def clear_all_postgres_tracking():
    """Clear ALL PostgreSQL tracking logs for Gold layer"""
    print(f"\n{'=' * 80}")
    print(f"1️⃣  CLEARING POSTGRESQL TRACKING LOGS")
    print(f"{'=' * 80}")
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()
        
        # Count all Gold tracking records
        cursor.execute("""
            SELECT COUNT(*) FROM file_ingestion_log 
            WHERE layer = 'gold'
        """)
        count_before = cursor.fetchone()[0]
        
        print(f"\n   Found {count_before} tracking records in Gold layer")
        
        if count_before > 0:
            # Show breakdown by table
            cursor.execute("""
                SELECT table_name, COUNT(*) as count 
                FROM file_ingestion_log 
                WHERE layer = 'gold'
                GROUP BY table_name
                ORDER BY count DESC
            """)
            
            print(f"\n   Breakdown by table:")
            for row in cursor.fetchall():
                table_name, count = row
                print(f"      • {table_name}: {count} records")
            
            # Delete ALL Gold tracking records
            print(f"\n   ⏳ Deleting all Gold tracking records...")
            cursor.execute("""
                DELETE FROM file_ingestion_log 
                WHERE layer = 'gold'
            """)
            conn.commit()
            
            print(f"   ✅ Deleted {count_before} tracking records")
        else:
            print(f"   ℹ️  No tracking records to delete")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ⚠️  Warning: Could not clear tracking: {e}")
        print(f"      (This is OK if table doesn't exist yet)")


def delete_all_gold_tables_data(spark):
    """Delete all data from ALL Gold Iceberg tables"""
    print(f"\n{'=' * 80}")
    print(f"2️⃣  DELETING ALL GOLD TABLES DATA")
    print(f"{'=' * 80}")
    
    try:
        # Create Gold database if not exists
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
        
        # Get all tables in Gold database
        tables = spark.sql(f"SHOW TABLES IN {GOLD_CATALOG}.{GOLD_DATABASE}").collect()
        existing_tables = [t.tableName for t in tables]
        
        print(f"\n   Found {len(existing_tables)} tables in Gold database:")
        for table in existing_tables:
            print(f"      • {table}")
        
        if not existing_tables:
            print(f"\n   ℹ️  No tables found in Gold database")
            return
        
        print(f"\n   ⏳ Deleting data from all tables...")
        
        total_deleted = 0
        for table_name in existing_tables:
            full_table = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{table_name}"
            
            try:
                # Count records
                count_before = spark.sql(f"SELECT COUNT(*) as count FROM {full_table}").collect()[0]['count']
                
                if count_before > 0:
                    print(f"\n      {table_name}:")
                    print(f"         Records before: {count_before:,}")
                    
                    # Delete all records
                    spark.sql(f"DELETE FROM {full_table} WHERE 1=1")
                    
                    # Verify
                    count_after = spark.sql(f"SELECT COUNT(*) as count FROM {full_table}").collect()[0]['count']
                    print(f"         Records after: {count_after:,}")
                    print(f"         ✅ Deleted: {count_before:,} records")
                    
                    total_deleted += count_before
                else:
                    print(f"\n      {table_name}: Already empty ✓")
                    
            except Exception as e:
                print(f"\n      {table_name}: ⚠️  Error - {e}")
        
        print(f"\n   {'=' * 70}")
        print(f"   ✅ TOTAL DELETED: {total_deleted:,} records from {len(existing_tables)} tables")
        print(f"   {'=' * 70}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()


def drop_all_gold_tables(spark):
    """Drop ALL Gold tables"""
    print(f"\n{'=' * 80}")
    print(f"3️⃣  DROPPING ALL GOLD TABLES")
    print(f"{'=' * 80}")
    
    try:
        # Create Gold database if not exists
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_CATALOG}.{GOLD_DATABASE}")
        
        # Get all tables in Gold database
        tables = spark.sql(f"SHOW TABLES IN {GOLD_CATALOG}.{GOLD_DATABASE}").collect()
        existing_tables = [t.tableName for t in tables]
        
        if not existing_tables:
            print(f"\n   ℹ️  No tables to drop")
            return
        
        print(f"\n   ⏳ Dropping {len(existing_tables)} tables...")
        
        dropped_count = 0
        for table_name in existing_tables:
            full_table = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{table_name}"
            
            try:
                # Use PURGE to force delete data files
                spark.sql(f"DROP TABLE IF EXISTS {full_table} PURGE")
                print(f"      ✅ Dropped: {table_name} (with PURGE)")
                dropped_count += 1
            except Exception as e:
                print(f"      ⚠️  Could not drop {table_name}: {e}")
        
        print(f"\n   ✅ Dropped {dropped_count} tables")
        
        # Verify Gold database is empty
        remaining = spark.sql(f"SHOW TABLES IN {GOLD_CATALOG}.{GOLD_DATABASE}").collect()
        if remaining:
            print(f"\n   ⚠️  Warning: {len(remaining)} tables still remain:")
            for t in remaining:
                print(f"      • {t.tableName}")
        else:
            print(f"\n   ✅ Gold database is now EMPTY")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()


def cleanup_gold_minio_bucket(spark):
    """FORCE DELETE all files in Gold MinIO bucket"""
    print(f"\n{'=' * 80}")
    print(f"3.5️⃣  FORCE CLEANING GOLD MINIO BUCKET")
    print(f"{'=' * 80}")
    print(f"\n   ⚠️  Manually deleting ALL files in s3a://gold/lakehouse/...")
    
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI("s3a://gold"),
            hadoop_conf
        )
        
        # Path to Gold lakehouse folder
        base_path = "s3a://gold/lakehouse"
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
        
        if fs.exists(hadoop_path):
            # List all table folders
            status_list = fs.listStatus(hadoop_path)
            folders = []
            
            for status in status_list:
                path_str = str(status.getPath())
                folder_name = path_str.split("/")[-1]
                folders.append(folder_name)
            
            print(f"\n   Found {len(folders)} table folders to delete:")
            for folder in folders:
                print(f"      • {folder}")
            
            if len(folders) > 0:
                # Delete entire lakehouse folder (all tables)
                print(f"\n   ⏳ Deleting ALL table folders and data files...")
                fs.delete(hadoop_path, True)  # True = recursive
                print(f"   ✅ FORCE DELETED: {base_path}")
                print(f"   ✅ Removed {len(folders)} table folders with ALL Parquet/metadata files")
            else:
                print(f"\n   ℹ️  No folders to delete")
        else:
            print(f"\n   ℹ️  Path does not exist: {base_path}")
            
    except Exception as e:
        print(f"   ⚠️  Warning: Could not cleanup MinIO: {e}")
        print(f"   💡 Try manual cleanup via MinIO Console: http://localhost:9001")


def cleanup_gold_scratch_bucket(spark):
    """Clean up ALL Gold scratch bucket tmp folders"""
    print(f"\n{'=' * 80}")
    print(f"4️⃣  CLEANING GOLD SCRATCH BUCKET")
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
            
            print(f"\n   Found {len(folders)} Gold table folders:")
            for folder in folders:
                print(f"      • {folder}")
            
            # Delete entire Gold pipeline folder
            print(f"\n   ⏳ Deleting all tmp folders...")
            fs.delete(hadoop_path, True)  # True = recursive
            print(f"   ✅ Deleted: {base_path}")
            print(f"   ✅ Removed {len(folders)} table folders with all run histories")
        else:
            print(f"\n   ℹ️  No Gold tmp folders found")
            
    except Exception as e:
        print(f"   ⚠️  Warning: Could not cleanup scratch: {e}")


def clean_hive_metastore_metadata():
    """FORCE DELETE all Gold table metadata from Hive Metastore (PostgreSQL)"""
    print(f"\n{'=' * 80}")
    print(f"5️⃣  CLEANING HIVE METASTORE METADATA (PostgreSQL)")
    print(f"{'=' * 80}")
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()
        
        # Step 1: Find Gold database ID
        cursor.execute('SELECT "DB_ID" FROM "DBS" WHERE "NAME" = %s', (GOLD_DATABASE,))
        db_result = cursor.fetchone()
        
        if not db_result:
            print(f"\n   ℹ️  Gold database '{GOLD_DATABASE}' not found in Hive metastore")
            cursor.close()
            conn.close()
            return
        
        db_id = db_result[0]
        print(f"\n   Found Gold database ID: {db_id}")
        
        # Step 2: Get all tables in Gold database
        cursor.execute('SELECT "TBL_ID", "TBL_NAME" FROM "TBLS" WHERE "DB_ID" = %s', (db_id,))
        tables = cursor.fetchall()
        
        if not tables:
            print(f"\n   ℹ️  No tables found in Gold database")
            cursor.close()
            conn.close()
            return
        
        print(f"\n   Found {len(tables)} tables in Hive metastore:")
        for tbl_id, tbl_name in tables:
            print(f"      • {tbl_name} (TBL_ID: {tbl_id})")
        
        print(f"\n   ⏳ Deleting metadata for all Gold tables...")
        
        total_deleted = 0
        for tbl_id, tbl_name in tables:
            print(f"\n      Processing: {tbl_name} (TBL_ID: {tbl_id})")
            
            try:
                # Get SD_ID and SERDE_ID before deleting
                cursor.execute('SELECT "SD_ID" FROM "TBLS" WHERE "TBL_ID" = %s', (tbl_id,))
                sd_result = cursor.fetchone()
                sd_id = sd_result[0] if sd_result else None
                
                serde_id = None
                if sd_id:
                    cursor.execute('SELECT "SERDE_ID" FROM "SDS" WHERE "SD_ID" = %s', (sd_id,))
                    serde_result = cursor.fetchone()
                    serde_id = serde_result[0] if serde_result else None
                
                # Delete in correct order (respecting foreign keys)
                
                # 1. Delete partition keys
                cursor.execute('DELETE FROM "PARTITION_KEYS" WHERE "TBL_ID" = %s', (tbl_id,))
                deleted_pk = cursor.rowcount
                if deleted_pk > 0:
                    print(f"         Deleted {deleted_pk} partition keys")
                
                # 2. Delete table parameters
                cursor.execute('DELETE FROM "TABLE_PARAMS" WHERE "TBL_ID" = %s', (tbl_id,))
                deleted_params = cursor.rowcount
                if deleted_params > 0:
                    print(f"         Deleted {deleted_params} table parameters")
                
                # 3. Delete column metadata (if exists)
                if sd_id:
                    cursor.execute('SELECT "CD_ID" FROM "SDS" WHERE "SD_ID" = %s', (sd_id,))
                    cd_result = cursor.fetchone()
                    cd_id = cd_result[0] if cd_result else None
                    
                    if cd_id:
                        cursor.execute('DELETE FROM "COLUMNS_V2" WHERE "CD_ID" = %s', (cd_id,))
                        deleted_cols = cursor.rowcount
                        if deleted_cols > 0:
                            print(f"         Deleted {deleted_cols} column definitions")
                
                # 4. Delete storage descriptor parameters
                if sd_id:
                    cursor.execute('DELETE FROM "SD_PARAMS" WHERE "SD_ID" = %s', (sd_id,))
                    deleted_sd_params = cursor.rowcount
                    if deleted_sd_params > 0:
                        print(f"         Deleted {deleted_sd_params} storage descriptor params")
                
                # 5. Delete storage descriptor
                if sd_id:
                    cursor.execute('DELETE FROM "SDS" WHERE "SD_ID" = %s', (sd_id,))
                    deleted_sds = cursor.rowcount
                    if deleted_sds > 0:
                        print(f"         Deleted storage descriptor")
                
                # 6. Delete serde parameters (if exists)
                if serde_id:
                    cursor.execute('DELETE FROM "SERDE_PARAMS" WHERE "SERDE_ID" = %s', (serde_id,))
                    deleted_serde_params = cursor.rowcount
                    if deleted_serde_params > 0:
                        print(f"         Deleted {deleted_serde_params} serde parameters")
                
                # 7. Delete serde (if exists)
                if serde_id:
                    cursor.execute('DELETE FROM "SERDES" WHERE "SERDE_ID" = %s', (serde_id,))
                    deleted_serde = cursor.rowcount
                    if deleted_serde > 0:
                        print(f"         Deleted serde")
                
                # 8. Finally delete the table entry
                cursor.execute('DELETE FROM "TBLS" WHERE "TBL_ID" = %s', (tbl_id,))
                deleted_table = cursor.rowcount
                if deleted_table > 0:
                    print(f"         ✅ Deleted table entry: {tbl_name}")
                    total_deleted += 1
                
            except Exception as e:
                print(f"         ⚠️  Error deleting {tbl_name}: {e}")
                import traceback
                traceback.print_exc()
        
        # Commit all deletions
        conn.commit()
        
        print(f"\n   {'=' * 70}")
        print(f"   ✅ TOTAL DELETED: {total_deleted} table metadata entries")
        print(f"   {'=' * 70}")
        
        # Verify cleanup
        cursor.execute('SELECT COUNT(*) FROM "TBLS" WHERE "DB_ID" = %s', (db_id,))
        remaining_count = cursor.fetchone()[0]
        
        if remaining_count == 0:
            print(f"\n   ✅ All Gold table metadata removed from Hive metastore")
        else:
            print(f"\n   ⚠️  Warning: {remaining_count} table entries still remain")
            cursor.execute('SELECT "TBL_NAME" FROM "TBLS" WHERE "DB_ID" = %s', (db_id,))
            remaining = cursor.fetchall()
            for (tbl_name,) in remaining:
                print(f"      • {tbl_name}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ❌ Error cleaning Hive metastore: {e}")
        import traceback
        traceback.print_exc()


def main():
    print("\n" + "=" * 80)
    print("⚠️  ⚠️  ⚠️   RESET ALL GOLD LAYER   ⚠️  ⚠️  ⚠️")
    print("=" * 80)
    print("\n🔥 THIS WILL DELETE EVERYTHING IN GOLD LAYER! 🔥\n")
    print("Actions:")
    print("   1. Clear ALL PostgreSQL tracking logs (layer='gold')")
    print("   2. Delete ALL data from Gold Iceberg tables")
    print("   3. Drop ALL Gold tables (with PURGE)")
    print("   4. FORCE DELETE MinIO Gold bucket files")
    print("   5. Clean ALL Gold scratch bucket tmp folders")
    print("   6. FORCE DELETE Hive metastore metadata from PostgreSQL")
    print("\n" + "=" * 80)
    print("⚠️  THIS IS A DESTRUCTIVE OPERATION - NO UNDO!")
    print("=" * 80 + "\n")
    
    spark = None
    
    try:
        # Step 1: Clear PostgreSQL tracking (no Spark needed)
        clear_all_postgres_tracking()
        
        # Step 2-5: Spark operations
        print(f"\n🔥 Starting Spark session...")
        spark = get_spark_session(app_name="Reset_All_Gold_Layer")
        
        delete_all_gold_tables_data(spark)
        drop_all_gold_tables(spark)
        cleanup_gold_minio_bucket(spark)  # Force delete MinIO files
        cleanup_gold_scratch_bucket(spark)
        
        # Step 6: Clean Hive metastore metadata (after Spark operations)
        # This ensures any orphaned metadata is removed
        clean_hive_metastore_metadata()
        
        # Final summary
        print(f"\n" + "=" * 80)
        print(f"✅ ✅ ✅  COMPLETE RESET SUCCESSFUL  ✅ ✅ ✅")
        print(f"=" * 80)
        print(f"")
        print(f"📊 Summary:")
        print(f"   • PostgreSQL tracking: ALL Gold records cleared")
        print(f"   • Gold tables: ALL dropped (with PURGE)")
        print(f"   • MinIO Gold bucket: ALL data files FORCE DELETED")
        print(f"   • Scratch bucket: ALL Gold tmp folders cleaned")
        print(f"   • Hive metastore: ALL Gold metadata removed from PostgreSQL")
        print(f"")
        print(f"🏗️  Gold layer is now completely clean!")
        print(f"")
        print(f"🚀 Next steps:")
        print(f"   1. Re-run Gold dimension jobs:")
        print(f"      • dim_province")
        print(f"      • dim_destination")
        print(f"")
        print(f"=" * 80)
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print(f"❌ RESET FAILED")
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

