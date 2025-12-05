"""
RESET fact_province_content_engagement Table
- Delete all data from fact table
- Clear PostgreSQL tracking logs
- Clean scratch bucket tmp folders
- Drop and recreate table (schema change support)
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
TABLE_NAME = "fact_province_content_engagement"
FULL_TABLE = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{TABLE_NAME}"


def clear_postgres_tracking():
    """Clear PostgreSQL tracking logs for this table"""
    print(f"\n{'=' * 80}")
    print(f"1️⃣  CLEARING POSTGRESQL TRACKING LOGS")
    print(f"{'=' * 80}")
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) FROM file_ingestion_log 
            WHERE table_name = %s AND layer = 'gold'
        """, (FULL_TABLE,))
        count_before = cursor.fetchone()[0]
        
        print(f"\n   Found {count_before} tracking records for {TABLE_NAME}")
        
        if count_before > 0:
            cursor.execute("""
                DELETE FROM file_ingestion_log 
                WHERE table_name = %s AND layer = 'gold'
            """, (FULL_TABLE,))
            conn.commit()
            print(f"   ✅ Deleted {count_before} tracking records")
        else:
            print(f"   ℹ️  No tracking records to delete")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ⚠️  Warning: Could not clear tracking: {e}")


def delete_table_data(spark):
    """Delete all data from fact table"""
    print(f"\n{'=' * 80}")
    print(f"2️⃣  DELETING TABLE DATA")
    print(f"{'=' * 80}")
    
    try:
        # Check if table exists
        tables = spark.sql(f"SHOW TABLES IN {GOLD_CATALOG}.{GOLD_DATABASE}").collect()
        existing_tables = [t.tableName for t in tables]
        
        if TABLE_NAME not in existing_tables:
            print(f"\n   ℹ️  Table {TABLE_NAME} does not exist")
            return
        
        # Count records
        count_before = spark.sql(f"SELECT COUNT(*) as count FROM {FULL_TABLE}").collect()[0]['count']
        
        print(f"\n   Records before: {count_before:,}")
        
        if count_before > 0:
            # Delete all records
            spark.sql(f"DELETE FROM {FULL_TABLE} WHERE 1=1")
            
            # Verify
            count_after = spark.sql(f"SELECT COUNT(*) as count FROM {FULL_TABLE}").collect()[0]['count']
            print(f"   Records after: {count_after:,}")
            print(f"   ✅ Deleted: {count_before:,} records")
        else:
            print(f"   ℹ️  Table is already empty")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")


def drop_table(spark):
    """Drop table (with PURGE to delete data files)"""
    print(f"\n{'=' * 80}")
    print(f"3️⃣  DROPPING TABLE")
    print(f"{'=' * 80}")
    
    try:
        # Check if table exists
        tables = spark.sql(f"SHOW TABLES IN {GOLD_CATALOG}.{GOLD_DATABASE}").collect()
        existing_tables = [t.tableName for t in tables]
        
        if TABLE_NAME not in existing_tables:
            print(f"\n   ℹ️  Table {TABLE_NAME} does not exist")
            return
        
        # Drop with PURGE
        spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE} PURGE")
        print(f"   ✅ Dropped table: {TABLE_NAME} (with PURGE)")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")


def cleanup_minio_bucket(spark):
    """Clean up MinIO bucket data for this table"""
    print(f"\n{'=' * 80}")
    print(f"4️⃣  CLEANING MINIO BUCKET")
    print(f"{'=' * 80}")
    
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI("s3a://gold"),
            hadoop_conf
        )
        
        # Path to table data
        table_path = f"s3a://gold/lakehouse/{TABLE_NAME}"
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(table_path)
        
        if fs.exists(hadoop_path):
            print(f"\n   Found table data at: {table_path}")
            print(f"   ⏳ Deleting...")
            fs.delete(hadoop_path, True)  # True = recursive
            print(f"   ✅ Deleted: {table_path}")
        else:
            print(f"\n   ℹ️  No data files found at: {table_path}")
            
    except Exception as e:
        print(f"   ⚠️  Warning: Could not cleanup MinIO: {e}")


def cleanup_scratch_bucket(spark):
    """Clean up scratch bucket tmp folders for this table"""
    print(f"\n{'=' * 80}")
    print(f"5️⃣  CLEANING SCRATCH BUCKET")
    print(f"{'=' * 80}")
    
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI("s3a://scratch"),
            hadoop_conf
        )
        
        # Path to table tmp folder
        scratch_path = f"s3a://scratch/pipeline/gold/{TABLE_NAME}"
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(scratch_path)
        
        if fs.exists(hadoop_path):
            print(f"\n   Found tmp folder at: {scratch_path}")
            print(f"   ⏳ Deleting...")
            fs.delete(hadoop_path, True)  # True = recursive
            print(f"   ✅ Deleted: {scratch_path}")
        else:
            print(f"\n   ℹ️  No tmp folder found at: {scratch_path}")
            
    except Exception as e:
        print(f"   ⚠️  Warning: Could not cleanup scratch: {e}")


def clean_hive_metastore():
    """Remove table metadata from Hive metastore"""
    print(f"\n{'=' * 80}")
    print(f"6️⃣  CLEANING HIVE METASTORE METADATA")
    print(f"{'=' * 80}")
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONN)
        cursor = conn.cursor()
        
        # Find Gold database ID
        cursor.execute('SELECT "DB_ID" FROM "DBS" WHERE "NAME" = %s', (GOLD_DATABASE,))
        db_result = cursor.fetchone()
        
        if not db_result:
            print(f"\n   ℹ️  Gold database not found in Hive metastore")
            cursor.close()
            conn.close()
            return
        
        db_id = db_result[0]
        
        # Find table
        cursor.execute('SELECT "TBL_ID", "TBL_NAME" FROM "TBLS" WHERE "DB_ID" = %s AND "TBL_NAME" = %s', 
                      (db_id, TABLE_NAME))
        table_result = cursor.fetchone()
        
        if not table_result:
            print(f"\n   ℹ️  Table {TABLE_NAME} not found in Hive metastore")
            cursor.close()
            conn.close()
            return
        
        tbl_id, tbl_name = table_result
        print(f"\n   Found table: {tbl_name} (TBL_ID: {tbl_id})")
        print(f"   ⏳ Deleting metadata...")
        
        # Get SD_ID and SERDE_ID
        cursor.execute('SELECT "SD_ID" FROM "TBLS" WHERE "TBL_ID" = %s', (tbl_id,))
        sd_result = cursor.fetchone()
        sd_id = sd_result[0] if sd_result else None
        
        serde_id = None
        if sd_id:
            cursor.execute('SELECT "SERDE_ID" FROM "SDS" WHERE "SD_ID" = %s', (sd_id,))
            serde_result = cursor.fetchone()
            serde_id = serde_result[0] if serde_result else None
        
        # Delete in correct order
        cursor.execute('DELETE FROM "PARTITION_KEYS" WHERE "TBL_ID" = %s', (tbl_id,))
        cursor.execute('DELETE FROM "TABLE_PARAMS" WHERE "TBL_ID" = %s', (tbl_id,))
        
        if sd_id:
            cursor.execute('SELECT "CD_ID" FROM "SDS" WHERE "SD_ID" = %s', (sd_id,))
            cd_result = cursor.fetchone()
            if cd_result:
                cursor.execute('DELETE FROM "COLUMNS_V2" WHERE "CD_ID" = %s', (cd_result[0],))
            
            cursor.execute('DELETE FROM "SD_PARAMS" WHERE "SD_ID" = %s', (sd_id,))
            cursor.execute('DELETE FROM "SDS" WHERE "SD_ID" = %s', (sd_id,))
        
        if serde_id:
            cursor.execute('DELETE FROM "SERDE_PARAMS" WHERE "SERDE_ID" = %s', (serde_id,))
            cursor.execute('DELETE FROM "SERDES" WHERE "SERDE_ID" = %s', (serde_id,))
        
        cursor.execute('DELETE FROM "TBLS" WHERE "TBL_ID" = %s', (tbl_id,))
        
        conn.commit()
        print(f"   ✅ Deleted metadata for: {tbl_name}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"   ⚠️  Warning: {e}")


def main():
    print("\n" + "=" * 80)
    print(f"🔥 RESET {TABLE_NAME}")
    print("=" * 80)
    print("\nThis will:")
    print(f"   1. Clear PostgreSQL tracking logs")
    print(f"   2. Delete all data from table")
    print(f"   3. Drop table (with PURGE)")
    print(f"   4. Clean MinIO bucket data")
    print(f"   5. Clean scratch bucket tmp folders")
    print(f"   6. Remove Hive metastore metadata")
    print("\n" + "=" * 80 + "\n")
    
    spark = None
    
    try:
        clear_postgres_tracking()
        
        print(f"\n🔥 Starting Spark session...")
        spark = get_spark_session(app_name=f"Reset_{TABLE_NAME}")
        
        delete_table_data(spark)
        drop_table(spark)
        cleanup_minio_bucket(spark)
        cleanup_scratch_bucket(spark)
        clean_hive_metastore()
        
        print(f"\n" + "=" * 80)
        print(f"✅ RESET COMPLETE: {TABLE_NAME}")
        print(f"=" * 80)
        print(f"\n🚀 Ready to rebuild table with:")
        print(f"   python3 fact_province_content_engagement_job.py")
        print(f"\n" + "=" * 80 + "\n")
        
    except Exception as e:
        print(f"\n{'=' * 80}")
        print(f"❌ RESET FAILED")
        print(f"{'=' * 80}")
        print(f"Error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
