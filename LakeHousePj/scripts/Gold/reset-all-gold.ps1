# RESET ALL GOLD LAYER - Complete Nuclear Option
# ⚠️  WARNING: This will DELETE EVERYTHING in Gold layer!

Write-Host "`n========================================" -ForegroundColor Red
Write-Host "RESET ALL GOLD LAYER" -ForegroundColor Red
Write-Host "========================================" -ForegroundColor Red

Write-Host "`n[!!!] WARNING: DESTRUCTIVE OPERATION [!!!]" -ForegroundColor Red
Write-Host "`nThis will:" -ForegroundColor Yellow
Write-Host "   1. Clear ALL PostgreSQL tracking logs (Gold)" -ForegroundColor Yellow
Write-Host "   2. Delete ALL data from Gold Iceberg tables" -ForegroundColor Yellow
Write-Host "   3. Drop ALL Gold tables (with PURGE)" -ForegroundColor Yellow
Write-Host "   4. FORCE DELETE MinIO Gold bucket files" -ForegroundColor Red
Write-Host "   5. Clean ALL Gold scratch bucket folders" -ForegroundColor Yellow
Write-Host "   6. FORCE DELETE Hive metastore metadata (PostgreSQL)" -ForegroundColor Red

Write-Host "`n[!!!] NO UNDO - ALL DATA WILL BE LOST [!!!]" -ForegroundColor Red

Write-Host "`nTables that will be affected:" -ForegroundColor Cyan
Write-Host "   - dim_province" -ForegroundColor White
Write-Host "   - dim_destination" -ForegroundColor White

$confirmation = Read-Host "`nType 'DELETE_ALL_GOLD' to continue (case sensitive)"
if ($confirmation -ne 'DELETE_ALL_GOLD') {
    Write-Host "`n[X] Reset cancelled - good choice!" -ForegroundColor Green
    exit 0
}

Write-Host "`n[!!!] Last chance to cancel [!!!]" -ForegroundColor Red
$finalConfirm = Read-Host "Type 'YES' to proceed"
if ($finalConfirm -ne 'YES') {
    Write-Host "`n[X] Reset cancelled" -ForegroundColor Yellow
    exit 0
}

Write-Host "`n[*] Starting complete Gold reset..." -ForegroundColor Cyan
Write-Host "[*] This may take a few minutes..." -ForegroundColor Cyan

# Run reset script
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --conf spark.app.name="Reset_All_Gold_Layer" `
    --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.gold.type=hive `
    --conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    /opt/spark/jobs/gold/reset_all_gold.py

$exitCode = $LASTEXITCODE

Write-Host "`n========================================" -ForegroundColor Yellow

if ($exitCode -eq 0) {
    Write-Host "[OK] COMPLETE RESET SUCCESSFUL" -ForegroundColor Green
    Write-Host "`nGold layer is now completely clean!" -ForegroundColor Cyan
    Write-Host "`n[>] Next steps:" -ForegroundColor Cyan
    Write-Host "   1. Re-run Gold dimension jobs:" -ForegroundColor White
    Write-Host "      • .\scripts\run-gold-dim-province.ps1" -ForegroundColor White
    Write-Host "      • .\scripts\run-gold-dim-destination.ps1" -ForegroundColor White
} else {
    Write-Host "[X] RESET FAILED (Exit Code: $exitCode)" -ForegroundColor Red
    Write-Host "Check logs above for details" -ForegroundColor Yellow
    Write-Host "Some data may have been deleted - verify manually!" -ForegroundColor Red
}

exit $exitCode

