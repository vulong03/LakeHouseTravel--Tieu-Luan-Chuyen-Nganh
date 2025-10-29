# ============================================
# Reset Bronze Layer - Complete Clean Start
# ============================================
# Purpose: Delete all Bronze data and metadata to start fresh
# Use Case: 
#   - Schema migration (e.g., adding row_checksum)
#   - Fix corrupted data
#   - Testing fresh ingestion
#
# WARNING: This will DELETE ALL DATA in Bronze layer!
# ============================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   BRONZE LAYER RESET" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "WARNING: This will DELETE ALL Bronze data!" -ForegroundColor Red
Write-Host "   - Hive Metastore metadata" -ForegroundColor Yellow
Write-Host "   - File tracking logs" -ForegroundColor Yellow
Write-Host "   - MinIO data files (bronze/lakehouse/)" -ForegroundColor Yellow
Write-Host ""

$confirmation = Read-Host "Are you sure? Type 'YES' to confirm"
if ($confirmation -ne "YES") {
    Write-Host ""
    Write-Host "Operation cancelled." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Starting Bronze layer reset..." -ForegroundColor Green
Write-Host ""

# ============================================
# STEP 1: Clean Hive Metastore Metadata
# ============================================
Write-Host "=== STEP 1: Clean Hive Metastore Metadata ===" -ForegroundColor Yellow
Write-Host "Dropping all Iceberg tables in Bronze database..."
Write-Host ""

docker exec lakehouse_spark_master python3 /opt/spark/jobs/utils/clean_hive_metastore.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "SUCCESS: Hive metadata cleaned" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "WARNING: Could not clean Hive metadata (may not exist)" -ForegroundColor Yellow
    Write-Host ""
}

# ============================================
# STEP 2: Delete File Tracking Logs
# ============================================
Write-Host "=== STEP 2: Delete File Tracking Logs ===" -ForegroundColor Yellow
Write-Host "Truncating file_ingestion_log table..."
Write-Host ""

docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "TRUNCATE TABLE file_ingestion_log;"

if ($LASTEXITCODE -eq 0) {
    Write-Host "SUCCESS: File tracking logs deleted" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "FAILED to delete tracking logs" -ForegroundColor Red
    Write-Host ""
    exit 1
}

# ============================================
# STEP 3: Verify Logs Deleted
# ============================================
Write-Host "=== STEP 3: Verify Logs Deleted ===" -ForegroundColor Yellow

$result = docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -t -c "SELECT COUNT(*) FROM file_ingestion_log;"
$count = $result.Trim()

Write-Host "Remaining logs: $count"
Write-Host ""

if ($count -eq "0") {
    Write-Host "SUCCESS: All tracking logs deleted" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "WARNING: Still have $count logs remaining" -ForegroundColor Yellow
    Write-Host ""
}

# ============================================
# STEP 4: Delete MinIO Data Files
# ============================================
Write-Host "=== STEP 4: Delete MinIO Data Files ===" -ForegroundColor Yellow
Write-Host "Deleting bronze/lakehouse/ folder..."
Write-Host ""

# Option 1: Using MinIO Client (mc)
Write-Host "Attempting to delete via MinIO Client..."
Write-Host ""
docker exec lakehouse_minio_init mc rm --recursive --force myminio/bronze/lakehouse/

if ($LASTEXITCODE -eq 0) {
    Write-Host "SUCCESS: MinIO data files deleted" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "MinIO Client method failed. Please delete manually via MinIO Console:" -ForegroundColor Yellow
    Write-Host "   1. Open: http://localhost:9001" -ForegroundColor Cyan
    Write-Host "   2. Login: minioadmin / minioadmin123" -ForegroundColor Cyan
    Write-Host "   3. Navigate to: bronze bucket" -ForegroundColor Cyan
    Write-Host "   4. Delete: lakehouse/ folder" -ForegroundColor Cyan
    Write-Host ""
    
    $manualConfirm = Read-Host "Have you deleted it manually? Type 'YES' to continue"
    if ($manualConfirm -ne "YES") {
        Write-Host ""
        Write-Host "Operation cancelled. Please delete MinIO data before proceeding." -ForegroundColor Red
        exit 1
    }
}

# ============================================
# STEP 5: Summary
# ============================================
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   RESET COMPLETE" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "SUCCESS: Hive Metastore - CLEANED" -ForegroundColor Green
Write-Host "SUCCESS: Tracking Logs - DELETED" -ForegroundColor Green
Write-Host "SUCCESS: MinIO Data - DELETED" -ForegroundColor Green
Write-Host ""

Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "   1. Run Bronze ingestion DAG in Airflow" -ForegroundColor Cyan
Write-Host "   2. Tables will be created with NEW schema (including row_checksum)" -ForegroundColor Cyan
Write-Host "   3. All data will be fresh from source CSVs" -ForegroundColor Cyan
Write-Host ""

Write-Host "To run ingestion:" -ForegroundColor Yellow
Write-Host "   - Open Airflow: http://localhost:8080" -ForegroundColor Cyan
Write-Host "   - Trigger DAG: bronze_layer_ingestion" -ForegroundColor Cyan
Write-Host ""
