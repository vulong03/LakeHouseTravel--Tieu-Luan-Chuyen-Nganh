#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Clean TikTok Comments Silver Tables (posts + comments)
    
.DESCRIPTION
    Xóa hoàn toàn 2 bảng Silver TikTok:
    - silver.tiktok_post_metadata
    - silver.tiktok_post_comments
    
    Bao gồm:
    - Hive metastore metadata
    - MinIO data files
    - PostgreSQL ingestion logs
    - Scratch data (optional)
#>

Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 79) -ForegroundColor Cyan
Write-Host "[CLEANUP] TIKTOK COMMENTS SILVER TABLES" -ForegroundColor Yellow
Write-Host "=" -NoNewline -ForegroundColor Cyan
Write-Host ("=" * 79) -ForegroundColor Cyan

# ============================================================================
# STEP 1: Get Table IDs from Hive Metastore
# ============================================================================
Write-Host "`nSTEP 1: Getting table IDs from Hive metastore..." -ForegroundColor Cyan

$query1 = "SELECT `"TBL_ID`", `"TBL_NAME`" FROM `"TBLS`" WHERE `"TBL_NAME`" IN ('tiktok_post_metadata', 'tiktok_post_comments');"
Write-Host "   Query: $query1" -ForegroundColor Gray

$result = echo $query1 | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
Write-Host $result

# ============================================================================
# STEP 2: Delete tiktok_post_metadata
# ============================================================================
Write-Host "`nSTEP 2: Deleting tiktok_post_metadata..." -ForegroundColor Yellow

Write-Host "`n   2.1 Getting TBL_ID for tiktok_post_metadata..." -ForegroundColor Cyan
$queryGetId1 = "SELECT `"TBL_ID`" FROM `"TBLS`" WHERE `"TBL_NAME`" = 'tiktok_post_metadata';"
$tblId1Result = echo $queryGetId1 | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db -t -A

if ($tblId1Result -match '^\d+$') {
    $tblId1 = $tblId1Result.Trim()
    Write-Host "   [OK] Found TBL_ID: $tblId1" -ForegroundColor Green
    
    Write-Host "`n   2.2 Deleting Hive metadata for tiktok_post_metadata (TBL_ID=$tblId1)..." -ForegroundColor Cyan
    $deleteQuery1 = "DELETE FROM `"TABLE_PARAMS`" WHERE `"TBL_ID`" = $tblId1; DELETE FROM `"PARTITION_KEYS`" WHERE `"TBL_ID`" = $tblId1; DELETE FROM `"TBLS`" WHERE `"TBL_ID`" = $tblId1;"
    echo $deleteQuery1 | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
    Write-Host "   [OK] Hive metadata deleted" -ForegroundColor Green
    
    Write-Host "`n   2.3 Deleting MinIO data for tiktok_post_metadata..." -ForegroundColor Cyan
    docker exec lakehouse_minio mc rm --recursive --force local/silver/lakehouse/silver.db/tiktok_post_metadata/
    docker exec lakehouse_minio mc rm --recursive --force local/silver/lakehouse/silver.db/tiktok_post_metadata/metadata/
    Write-Host "   [OK] MinIO data deleted" -ForegroundColor Green
    
    Write-Host "`n   2.4 Deleting ingestion logs for tiktok_post_metadata..." -ForegroundColor Cyan
    $deleteLog1 = "DELETE FROM file_ingestion_log WHERE table_name LIKE '%tiktok_post_metadata%';"
    echo $deleteLog1 | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
    Write-Host "   [OK] Ingestion logs deleted" -ForegroundColor Green
    
} else {
    Write-Host "   [WARNING] Table tiktok_post_metadata not found in metastore" -ForegroundColor Yellow
}

# ============================================================================
# STEP 3: Delete tiktok_post_comments
# ============================================================================
Write-Host "`nSTEP 3: Deleting tiktok_post_comments..." -ForegroundColor Yellow

Write-Host "`n   3.1 Getting TBL_ID for tiktok_post_comments..." -ForegroundColor Cyan
$queryGetId2 = "SELECT `"TBL_ID`" FROM `"TBLS`" WHERE `"TBL_NAME`" = 'tiktok_post_comments';"
$tblId2Result = echo $queryGetId2 | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db -t -A

if ($tblId2Result -match '^\d+$') {
    $tblId2 = $tblId2Result.Trim()
    Write-Host "   [OK] Found TBL_ID: $tblId2" -ForegroundColor Green
    
    Write-Host "`n   3.2 Deleting Hive metadata for tiktok_post_comments (TBL_ID=$tblId2)..." -ForegroundColor Cyan
    $deleteQuery2 = "DELETE FROM `"TABLE_PARAMS`" WHERE `"TBL_ID`" = $tblId2; DELETE FROM `"PARTITION_KEYS`" WHERE `"TBL_ID`" = $tblId2; DELETE FROM `"TBLS`" WHERE `"TBL_ID`" = $tblId2;"
    echo $deleteQuery2 | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
    Write-Host "   [OK] Hive metadata deleted" -ForegroundColor Green
    
    Write-Host "`n   3.3 Deleting MinIO data for tiktok_post_comments..." -ForegroundColor Cyan
    docker exec lakehouse_minio mc rm --recursive --force local/silver/lakehouse/silver.db/tiktok_post_comments/
    docker exec lakehouse_minio mc rm --recursive --force local/silver/lakehouse/silver.db/tiktok_post_comments/metadata/
    Write-Host "   [OK] MinIO data deleted" -ForegroundColor Green
    
    Write-Host "`n   3.4 Deleting ingestion logs for tiktok_post_comments..." -ForegroundColor Cyan
    $deleteLog2 = "DELETE FROM file_ingestion_log WHERE table_name LIKE '%tiktok_post_comments%';"
    echo $deleteLog2 | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
    Write-Host "   [OK] Ingestion logs deleted" -ForegroundColor Green
    
} else {
    Write-Host "   [WARNING] Table tiktok_post_comments not found in metastore" -ForegroundColor Yellow
}

# ============================================================================
# STEP 4: Clean Scratch data (optional)
# ============================================================================
Write-Host "`nSTEP 4: Cleaning Scratch data..." -ForegroundColor Yellow

Write-Host "`n   Would you like to delete Scratch data as well? (y/N): " -ForegroundColor Cyan -NoNewline
$cleanScratch = Read-Host

if ($cleanScratch -eq 'y' -or $cleanScratch -eq 'Y') {
    Write-Host "`n   4.1 Deleting Scratch posts data..." -ForegroundColor Cyan
    docker exec lakehouse_minio mc rm --recursive --force local/scratch/pipeline/silver/tiktok_post_metadata/
    Write-Host "   [OK] Scratch posts deleted" -ForegroundColor Green
    
    Write-Host "`n   4.2 Deleting Scratch comments data..." -ForegroundColor Cyan
    docker exec lakehouse_minio mc rm --recursive --force local/scratch/pipeline/silver/tiktok_post_comments/
    Write-Host "   [OK] Scratch comments deleted" -ForegroundColor Green
} else {
    Write-Host "   [SKIP] Skipped Scratch cleanup" -ForegroundColor Gray
}

# ============================================================================
# STEP 5: Verify cleanup
# ============================================================================
Write-Host "`nSTEP 5: Verifying cleanup..." -ForegroundColor Cyan

Write-Host "`n   5.1 Checking remaining tables in Hive metastore..." -ForegroundColor Cyan
$verifyQuery = "SELECT `"TBL_NAME`" FROM `"TBLS`" WHERE `"TBL_NAME`" LIKE '%tiktok%';"
$remainingTables = echo $verifyQuery | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
Write-Host $remainingTables

Write-Host "`n   5.2 Checking MinIO buckets..." -ForegroundColor Cyan
Write-Host "   Silver bucket:" -ForegroundColor Gray
docker exec lakehouse_minio mc ls local/silver/lakehouse/silver.db/ | Select-String "tiktok"
Write-Host "   Scratch bucket:" -ForegroundColor Gray
docker exec lakehouse_minio mc ls local/scratch/pipeline/silver/ | Select-String "tiktok"

Write-Host "`n" -NoNewline
Write-Host "=" -NoNewline -ForegroundColor Green
Write-Host ("=" * 79) -ForegroundColor Green
Write-Host "[SUCCESS] CLEANUP COMPLETED!" -ForegroundColor Green
Write-Host "=" -NoNewline -ForegroundColor Green
Write-Host ("=" * 79) -ForegroundColor Green
Write-Host "`nSummary:" -ForegroundColor Cyan
Write-Host "   - Hive metastore: Cleaned" -ForegroundColor Gray
Write-Host "   - MinIO data: Cleaned" -ForegroundColor Gray
Write-Host "   - Ingestion logs: Cleaned" -ForegroundColor Gray
if ($cleanScratch -eq 'y' -or $cleanScratch -eq 'Y') {
    Write-Host "   - Scratch data: Cleaned" -ForegroundColor Gray
} else {
    Write-Host "   - Scratch data: Kept" -ForegroundColor Gray
}
Write-Host "`nYou can now re-run the pipeline from Step 1" -ForegroundColor Yellow
