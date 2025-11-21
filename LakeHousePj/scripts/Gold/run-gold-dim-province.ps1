# ============================================
# Run Gold Layer - dim_province Job
# ============================================
# Purpose: Build dimension table for provinces
# Source: /data/VietNam_Province/list_of_provinces_of_vietnam-154j.csv
# Target: gold.dim_province (Iceberg table)
# ============================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   Gold Layer - dim_province Job" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[*] Job Details:" -ForegroundColor Yellow
Write-Host "   Source: /data/VietNam_Province/list_of_provinces_of_vietnam-154j.csv" -ForegroundColor White
Write-Host "   Target: gold.dim_province" -ForegroundColor White
Write-Host "   Mode: Full Refresh (Overwrite)" -ForegroundColor White
Write-Host ""

Write-Host "[*] Submitting Spark job..." -ForegroundColor Green
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.app.name="Gold_Dim_Province" `
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog `
  --conf spark.sql.catalog.gold.type=hive `
  --conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 `
  --conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse `
  --conf spark.sql.defaultCatalog=gold `
  --driver-memory 1g `
  --executor-memory 768m `
  --executor-cores 2 `
  --num-executors 1 `
  /opt/spark/jobs/gold/dim_province/dim_province_job.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "   [SUCCESS] Job completed successfully!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next Steps:" -ForegroundColor Yellow
    Write-Host "   1. Query table: spark.table('gold.dim_province').show()" -ForegroundColor Cyan
    Write-Host "   2. Check MinIO: http://localhost:9001 (gold bucket)" -ForegroundColor Cyan
    Write-Host "   3. Check Hive Metastore: Iceberg metadata stored" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Red
    Write-Host "   [FAILED] Job failed!" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "   1. Check Spark logs: docker logs lakehouse_spark_master" -ForegroundColor Cyan
    Write-Host "   2. Check CSV exists: docker exec lakehouse_spark_master ls -la /data/VietNam_Province/" -ForegroundColor Cyan
    Write-Host "   3. Check Hive Metastore: docker logs lakehouse_hive_metastore" -ForegroundColor Cyan
    Write-Host ""
    exit 1
}
