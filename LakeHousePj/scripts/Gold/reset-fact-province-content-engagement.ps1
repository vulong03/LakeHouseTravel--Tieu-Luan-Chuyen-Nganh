# ============================================
# Reset fact_province_content_engagement Table
# ============================================
# Purpose: Clean up fact table for rebuild (supports schema changes)
# Actions:
#   1. Clear PostgreSQL tracking logs
#   2. Delete all table data
#   3. Drop table (with PURGE)
#   4. Clean MinIO bucket data
#   5. Clean scratch bucket tmp
#   6. Remove Hive metastore metadata
# ============================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Red
Write-Host "   Reset fact_province_content_engagement" -ForegroundColor Red
Write-Host "============================================" -ForegroundColor Red
Write-Host ""

Write-Host "[!] WARNING: This will delete ALL data from fact_province_content_engagement!" -ForegroundColor Yellow
Write-Host ""

$SPARK_JOB_PATH = "/opt/spark/jobs/gold/fact_province_content_engagement/reset_fact_province_content_engagement.py"

Write-Host "[*] Starting reset job..." -ForegroundColor Cyan
Write-Host "   Script: $SPARK_JOB_PATH" -ForegroundColor White
Write-Host ""

# Run with spark-submit (not python3)
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master local[*] `
    --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.gold.type=hive `
    --conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    $SPARK_JOB_PATH

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "   Reset Complete!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "[*] Next step: Rebuild table" -ForegroundColor Cyan
    Write-Host "   .\run-gold-fact-province-content-engagement.ps1" -ForegroundColor White
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Red
    Write-Host "   Reset Failed!" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "[!] Check logs above for errors" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}
