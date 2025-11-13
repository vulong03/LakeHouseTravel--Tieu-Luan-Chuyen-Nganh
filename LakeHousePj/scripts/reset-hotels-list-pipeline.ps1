# Reset Hotels List Pipeline - Complete Cleanup
# Clears PostgreSQL tracking, Silver table data, and scratch bucket

Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "RESET HOTELS LIST PIPELINE" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow

Write-Host "`n[WARNING] This will:" -ForegroundColor Red
Write-Host "   1. Clear PostgreSQL tracking logs" -ForegroundColor Red
Write-Host "   2. Delete Silver table data" -ForegroundColor Red
Write-Host "   3. Drop and recreate table" -ForegroundColor Red
Write-Host "   4. Clean scratch bucket" -ForegroundColor Red

$confirmation = Read-Host "`nType 'YES' to continue"
if ($confirmation -ne 'YES') {
    Write-Host "`n[X] Reset cancelled" -ForegroundColor Yellow
    exit 0
}

Write-Host "`n[*] Starting reset..." -ForegroundColor Cyan

# Run reset script
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/lakehouse `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    /opt/spark/jobs/silver/hotels_list/reset_hotels_list.py

$exitCode = $LASTEXITCODE

Write-Host "`n========================================" -ForegroundColor Yellow

if ($exitCode -eq 0) {
    Write-Host "[OK] RESET COMPLETED SUCCESSFULLY" -ForegroundColor Green
    Write-Host "`n[>] Next step: Run pipeline test" -ForegroundColor Cyan
    Write-Host "   .\scripts\test-hotels-list-pipeline.ps1" -ForegroundColor White
} else {
    Write-Host "[X] RESET FAILED (Exit Code: $exitCode)" -ForegroundColor Red
    Write-Host "Check logs above for details" -ForegroundColor Yellow
}

exit $exitCode
