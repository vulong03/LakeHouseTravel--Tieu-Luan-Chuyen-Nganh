#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Reset fact_comment_nlp_engagement table để chạy lại với sentiment fix

.DESCRIPTION
    Script này sẽ:
    1. Lấy TBL_ID của fact_comment_nlp_engagement từ Hive metastore
    2. Xóa metadata trong PostgreSQL (TABLE_PARAMS, PARTITION_KEYS, TBLS)
    3. Xóa data files trong MinIO
    4. Xóa log trong gold_job_log

.NOTES
    Chạy từ thư mục scripts/
#>

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Reset fact_comment_nlp_engagement Table" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$tableName = "fact_comment_nlp_engagement"
$catalog = "gold"
$database = "gold"

# Step 1: Lấy TBL_ID từ Hive metastore
Write-Host "`n[1/4] Getting table ID from Hive metastore..." -ForegroundColor Yellow

$query = "SELECT `"TBL_ID`", `"TBL_NAME`" FROM `"TBLS`" WHERE `"TBL_NAME`" = '$tableName';"
$result = echo $query | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db -t

if ([string]::IsNullOrWhiteSpace($result)) {
    Write-Host "   ⚠️  Table '$tableName' not found in metastore" -ForegroundColor Yellow
    Write-Host "   Table may not exist yet or already deleted" -ForegroundColor Gray
    $tblId = $null
} else {
    $tblId = ($result.Trim() -split '\s+')[0]
    Write-Host "   ✅ Found table ID: $tblId" -ForegroundColor Green
}

# Step 2: Xóa metadata trong PostgreSQL
if ($tblId) {
    Write-Host "`n[2/4] Deleting Hive metadata from PostgreSQL..." -ForegroundColor Yellow
    
    $deleteQuery = @"
DELETE FROM "TABLE_PARAMS" WHERE "TBL_ID" = $tblId;
DELETE FROM "PARTITION_KEYS" WHERE "TBL_ID" = $tblId;
DELETE FROM "TBLS" WHERE "TBL_ID" = $tblId;
"@
    
    echo $deleteQuery | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   ✅ Metadata deleted successfully" -ForegroundColor Green
    } else {
        Write-Host "   ❌ Failed to delete metadata" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "`n[2/4] Skipping metadata deletion (table not found)" -ForegroundColor Gray
}

# Step 3: Xóa data files trong MinIO
Write-Host "`n[3/4] Deleting data files from MinIO..." -ForegroundColor Yellow

$minioPath = "$catalog/lakehouse/$database.db/$tableName/"
Write-Host "   Path: $minioPath" -ForegroundColor Gray

docker exec lakehouse_minio mc rm --recursive --force local/$minioPath 2>$null

if ($LASTEXITCODE -eq 0 -or $LASTEXITCODE -eq 1) {
    # Exit code 1 means path not found, which is OK
    Write-Host "   ✅ Data files deleted (or path not found)" -ForegroundColor Green
} else {
    Write-Host "   ⚠️  Warning: mc rm returned code $LASTEXITCODE" -ForegroundColor Yellow
}

# Step 4: Xóa execution logs
Write-Host "`n[4/4] Deleting execution logs from gold_job_log..." -ForegroundColor Yellow

$logDeleteQuery = "DELETE FROM gold_job_log WHERE table_name = '$tableName';"
echo $logDeleteQuery | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db

if ($LASTEXITCODE -eq 0) {
    Write-Host "   ✅ Logs deleted successfully" -ForegroundColor Green
} else {
    Write-Host "   ⚠️  Warning: Failed to delete logs (table may not exist)" -ForegroundColor Yellow
}

# Summary
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "✅ Reset Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Table '$tableName' has been reset." -ForegroundColor White
Write-Host "You can now run: .\run-fact-comment-nlp.ps1" -ForegroundColor White
Write-Host ""
