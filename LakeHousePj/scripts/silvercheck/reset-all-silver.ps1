# RESET ALL SILVER LAYER - Complete Nuclear Option
# ⚠️  WARNING: This will DELETE EVERYTHING in Silver layer!

Write-Host "`n========================================" -ForegroundColor Red
Write-Host "RESET ALL SILVER LAYER" -ForegroundColor Red
Write-Host "========================================" -ForegroundColor Red

Write-Host "`n[!!!] WARNING: DESTRUCTIVE OPERATION [!!!]" -ForegroundColor Red
Write-Host "`nThis will:" -ForegroundColor Yellow
Write-Host "   1. Clear ALL PostgreSQL tracking logs (Silver)" -ForegroundColor Yellow
Write-Host "   2. Delete ALL data from Silver tables" -ForegroundColor Yellow
Write-Host "   3. Drop ALL Silver tables (with PURGE)" -ForegroundColor Yellow
Write-Host "   4. FORCE DELETE MinIO Silver bucket files" -ForegroundColor Red
Write-Host "   5. Clean ALL scratch bucket folders" -ForegroundColor Yellow
Write-Host "   6. Clean Gold scratch bucket" -ForegroundColor Yellow

Write-Host "`n[!!!] NO UNDO - ALL DATA WILL BE LOST [!!!]" -ForegroundColor Red

Write-Host "`nTables that will be affected:" -ForegroundColor Cyan
Write-Host "   - hotels_list" -ForegroundColor White
Write-Host "   - hotels_detail" -ForegroundColor White
Write-Host "   - hotels_reviews" -ForegroundColor White
Write-Host "   - tiktok_videos" -ForegroundColor White
Write-Host "   - tiktok_post_metadata" -ForegroundColor White
Write-Host "   - tiktok_post_comments" -ForegroundColor White

$confirmation = Read-Host "`nType 'DELETE_ALL_SILVER' to continue (case sensitive)"
if ($confirmation -ne 'DELETE_ALL_SILVER') {
    Write-Host "`n[X] Reset cancelled - good choice!" -ForegroundColor Green
    exit 0
}

Write-Host "`n[!!!] Last chance to cancel [!!!]" -ForegroundColor Red
$finalConfirm = Read-Host "Type 'YES' to proceed"
if ($finalConfirm -ne 'YES') {
    Write-Host "`n[X] Reset cancelled" -ForegroundColor Yellow
    exit 0
}

Write-Host "`n[*] Starting complete Silver reset..." -ForegroundColor Cyan
Write-Host "[*] This may take a few minutes..." -ForegroundColor Cyan

# Run reset script
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.silver.type=hive `
    --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    /opt/spark/jobs/silver/reset_all_silver.py

$exitCode = $LASTEXITCODE

Write-Host "`n========================================" -ForegroundColor Yellow

if ($exitCode -eq 0) {
    Write-Host "[OK] COMPLETE RESET SUCCESSFUL" -ForegroundColor Green
    Write-Host "`nSilver layer is now completely clean!" -ForegroundColor Cyan
    Write-Host "`n[>] Next steps:" -ForegroundColor Cyan
    Write-Host "   1. Re-ingest Bronze data (if needed)" -ForegroundColor White
    Write-Host "   2. Run Silver pipelines for each table" -ForegroundColor White
} else {
    Write-Host "[X] RESET FAILED (Exit Code: $exitCode)" -ForegroundColor Red
    Write-Host "Check logs above for details" -ForegroundColor Yellow
    Write-Host "Some data may have been deleted - verify manually!" -ForegroundColor Red
}

exit $exitCode
