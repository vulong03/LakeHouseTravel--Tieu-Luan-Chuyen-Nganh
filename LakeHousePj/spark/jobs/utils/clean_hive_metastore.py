"""
Clean corrupted table metadata from Hive Metastore (PostgreSQL)
This script directly deletes table entries from PostgreSQL when Spark cannot drop them
"""

import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL connection details (from docker-compose.yml and .env)
PG_CONFIG = {
    "host": "postgres",  # Docker service name
    "port": 5432,
    "database": "metastore_db",
    "user": "lakehouse_user",
    "password": "lakehouse_pass"  # From .env file
}

TABLE_NAMES = [
    "raw_booking_hotels_list",
    "raw_booking_hotels_detail",
    "raw_booking_hotels_reviews",
    "raw_tiktok_video_links",
    "raw_tiktok_post_metadata",
    "raw_tiktok_post_comments"
]

def clean_corrupted_tables():
    """Delete corrupted table metadata from Hive Metastore"""
    
    print("\n" + "="*80)
    print("🧹 CLEANING CORRUPTED HIVE METASTORE METADATA")
    print("="*80)
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = False  # Use transactions
        cursor = conn.cursor()
        
        logger.info(f"✅ Connected to PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}")
        
        # Get database ID for 'bronze'
        cursor.execute("""
            SELECT "DB_ID", "NAME" 
            FROM "DBS" 
            WHERE "NAME" = 'bronze'
        """)
        db_result = cursor.fetchone()
        
        if not db_result:
            logger.warning("⚠️  Database 'bronze' not found in Hive Metastore")
            return
        
        db_id = db_result[0]
        logger.info(f"✅ Found database 'bronze' with DB_ID = {db_id}")
        
        # Delete each table
        for table_name in TABLE_NAMES:
            print(f"\n🗑️  Processing table: {table_name}")
            
            # Find table ID
            cursor.execute("""
                SELECT "TBL_ID", "TBL_NAME" 
                FROM "TBLS" 
                WHERE "DB_ID" = %s AND "TBL_NAME" = %s
            """, (db_id, table_name))
            
            table_result = cursor.fetchone()
            
            if not table_result:
                logger.info(f"   ⏭️  Table '{table_name}' not found in metastore (already clean)")
                continue
            
            table_id = table_result[0]
            logger.info(f"   Found TBL_ID = {table_id}")
            
            # Delete in reverse order of foreign key dependencies
            
            # 1. Delete partition keys
            cursor.execute('DELETE FROM "PARTITION_KEYS" WHERE "TBL_ID" = %s', (table_id,))
            deleted_partition_keys = cursor.rowcount
            logger.info(f"   Deleted {deleted_partition_keys} partition keys")
            
            # 2. Delete table parameters
            cursor.execute('DELETE FROM "TABLE_PARAMS" WHERE "TBL_ID" = %s', (table_id,))
            deleted_params = cursor.rowcount
            logger.info(f"   Deleted {deleted_params} table parameters")
            
            # 3. Get SD_ID and SERDE_ID before deleting table
            cursor.execute('SELECT "SD_ID" FROM "TBLS" WHERE "TBL_ID" = %s', (table_id,))
            sd_result = cursor.fetchone()
            sd_id = sd_result[0] if sd_result else None
            
            serde_id = None
            if sd_id:
                cursor.execute('SELECT "SERDE_ID" FROM "SDS" WHERE "SD_ID" = %s', (sd_id,))
                serde_result = cursor.fetchone()
                serde_id = serde_result[0] if serde_result else None
                logger.info(f"   Found SD_ID = {sd_id}, SERDE_ID = {serde_id}")
            
            # 4. Delete the table FIRST (it references SD_ID)
            cursor.execute('DELETE FROM "TBLS" WHERE "TBL_ID" = %s', (table_id,))
            deleted_tables = cursor.rowcount
            logger.info(f"   Deleted table entry from TBLS")
            
            # 5. Now delete storage descriptor components
            if sd_id:
                # Delete column descriptors
                cursor.execute('DELETE FROM "COLUMNS_V2" WHERE "CD_ID" IN (SELECT "CD_ID" FROM "SDS" WHERE "SD_ID" = %s)', (sd_id,))
                deleted_columns = cursor.rowcount
                logger.info(f"   Deleted {deleted_columns} column descriptors")
                
                # Delete storage descriptor parameters
                cursor.execute('DELETE FROM "SD_PARAMS" WHERE "SD_ID" = %s', (sd_id,))
                deleted_sd_params = cursor.rowcount
                logger.info(f"   Deleted {deleted_sd_params} SD parameters")
                
                # Delete storage descriptor (it references SERDE_ID)
                cursor.execute('DELETE FROM "SDS" WHERE "SD_ID" = %s', (sd_id,))
                deleted_sds = cursor.rowcount
                logger.info(f"   Deleted {deleted_sds} storage descriptors")
            
            # 6. Finally delete SERDE info
            if serde_id:
                cursor.execute('DELETE FROM "SERDE_PARAMS" WHERE "SERDE_ID" = %s', (serde_id,))
                deleted_serde_params = cursor.rowcount
                logger.info(f"   Deleted {deleted_serde_params} SERDE parameters")
                
                cursor.execute('DELETE FROM "SERDES" WHERE "SERDE_ID" = %s', (serde_id,))
                deleted_serdes = cursor.rowcount
                logger.info(f"   Deleted {deleted_serdes} SERDE entries")
            
            logger.info(f"   ✅ Deleted table '{table_name}' and all related metadata")
        
        # Commit transaction
        conn.commit()
        logger.info("\n✅ All corrupted tables cleaned successfully!")
        logger.info("   You can now run the ingestion scripts to create fresh tables")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"\n❌ PostgreSQL error: {e}")
        if conn:
            conn.rollback()
            logger.info("   Transaction rolled back")
        raise
    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logger.info("   PostgreSQL connection closed")


if __name__ == "__main__":
    try:
        clean_corrupted_tables()
        print("\n" + "="*80)
        print("✅ SUCCESS - Hive Metastore cleaned!")
        print("="*80)
    except Exception as e:
        print("\n" + "="*80)
        print(f"❌ FAILED - {str(e)}")
        print("="*80)
        exit(1)
