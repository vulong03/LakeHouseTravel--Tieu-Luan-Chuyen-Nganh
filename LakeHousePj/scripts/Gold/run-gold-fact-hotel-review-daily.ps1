# ============================================
# Run Gold Layer - fact_hotel_review_daily Job
# ============================================
# Purpose: Build fact_hotel_review_daily from silver.hotels_reviews
# Source : silver.hotels_reviews
# Target : gold.fact_hotel_review_daily
# Mode   : Append
# ============================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   Gold Layer - fact_hotel_review_daily Job" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[*] Job Details:" -ForegroundColor Yellow
Write-Host "   Source: silver.hotels_reviews" -ForegroundColor White
Write-Host "   Target: gold.fact_hotel_review_daily" -ForegroundColor White
Write-Host "   Mode: Append" -ForegroundColor White
Write-Host ""

Write-Host "[*] Submitting Spark job..." -ForegroundColor Green
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.app.name="Gold_Fact_Hotel_Review_Daily" `
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog `
  --conf spark.sql.catalog.gold.type=hive `
  --conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 `
  --conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse `
  --conf spark.sql.defaultCatalog=gold `
  --driver-memory 1g `
  --executor-memory 768m `
  --executor-cores 2 `
  --num-executors 1 `
  /opt/spark/jobs/gold/fact_hotel_review_daily/fact_hotel_review_daily_job.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "   [SUCCESS] Job completed successfully!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Red
    Write-Host "   [FAILED] Job failed!" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Red
    Write-Host ""
    exit 1
}
