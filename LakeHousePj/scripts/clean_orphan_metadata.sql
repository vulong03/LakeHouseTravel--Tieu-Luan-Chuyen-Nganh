-- Clean orphaned table metadata from Hive Metastore
-- Delete in correct order to handle foreign key constraints

BEGIN;

-- Step 1: Break the circular reference by setting SD_ID to NULL first
UPDATE "TBLS" SET "SD_ID" = NULL 
WHERE "TBL_NAME" IN ('hotels_list', 'tiktok_videos') 
  AND "DB_ID" = (SELECT "DB_ID" FROM "DBS" WHERE "NAME" = 'silver');

-- Step 2: Now we can delete from child tables
WITH orphan_tables AS (
    SELECT "TBL_ID", "SD_ID"
    FROM "TBLS" 
    WHERE "TBL_NAME" IN ('hotels_list', 'tiktok_videos') 
      AND "DB_ID" = (SELECT "DB_ID" FROM "DBS" WHERE "NAME" = 'silver')
),
orphan_sds AS (
    SELECT DISTINCT "SD_ID" FROM orphan_tables WHERE "SD_ID" IS NOT NULL
)
DELETE FROM "TABLE_PARAMS" WHERE "TBL_ID" IN (SELECT "TBL_ID" FROM orphan_tables);

WITH orphan_tables AS (
    SELECT "TBL_ID", "SD_ID"
    FROM "TBLS" 
    WHERE "TBL_NAME" IN ('hotels_list', 'tiktok_videos') 
      AND "DB_ID" = (SELECT "DB_ID" FROM "DBS" WHERE "NAME" = 'silver')
),
orphan_sds AS (
    SELECT DISTINCT "SD_ID" FROM orphan_tables WHERE "SD_ID" IS NOT NULL
)
DELETE FROM "SD_PARAMS" WHERE "SD_ID" IN (SELECT "SD_ID" FROM orphan_sds);

WITH orphan_tables AS (
    SELECT "TBL_ID", "SD_ID"
    FROM "TBLS" 
    WHERE "TBL_NAME" IN ('hotels_list', 'tiktok_videos') 
      AND "DB_ID" = (SELECT "DB_ID" FROM "DBS" WHERE "NAME" = 'silver')
),
orphan_sds AS (
    SELECT DISTINCT "SD_ID" FROM orphan_tables WHERE "SD_ID" IS NOT NULL
)
DELETE FROM "COLUMNS_V2" WHERE "CD_ID" IN (SELECT "CD_ID" FROM "SDS" WHERE "SD_ID" IN (SELECT "SD_ID" FROM orphan_sds));

WITH orphan_tables AS (
    SELECT "TBL_ID", "SD_ID"
    FROM "TBLS" 
    WHERE "TBL_NAME" IN ('hotels_list', 'tiktok_videos') 
      AND "DB_ID" = (SELECT "DB_ID" FROM "DBS" WHERE "NAME" = 'silver')
),
orphan_sds AS (
    SELECT DISTINCT "SD_ID" FROM orphan_tables WHERE "SD_ID" IS NOT NULL
)
DELETE FROM "SDS" WHERE "SD_ID" IN (SELECT "SD_ID" FROM orphan_sds);

-- Step 3: Finally delete from main table
DELETE FROM "TBLS" 
WHERE "TBL_NAME" IN ('hotels_list', 'tiktok_videos') 
  AND "DB_ID" = (SELECT "DB_ID" FROM "DBS" WHERE "NAME" = 'silver');

COMMIT;
