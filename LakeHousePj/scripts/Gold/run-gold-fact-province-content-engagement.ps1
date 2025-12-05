# ============================================
# Run Gold Layer - fact_province_content_engagement Job
# ============================================
# Purpose: Load post-level engagement metrics (1 row per post)
# Grain: 1 row = 1 post (post_sk is unique key)
# Source: gold.gold.dim_post + silver.silver.tiktok_post_metadata
# Target: gold.fact_province_content_engagement (Iceberg table)
# ============================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   Gold Layer - fact_province_content_engagement Job" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[*] Job Details:" -ForegroundColor Yellow
Write-Host "   Source 1: gold.gold.dim_post" -ForegroundColor White
Write-Host "   Source 2: silver.silver.tiktok_post_metadata" -ForegroundColor White
Write-Host "   Target: gold.fact_province_content_engagement" -ForegroundColor White
Write-Host "   Mode: Full Refresh (Overwrite)" -ForegroundColor White
Write-Host ""

Write-Host "[*] Submitting Spark job..." -ForegroundColor Green
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --driver-memory 2G `
  --executor-memory 2G `
  --conf spark.executor.memoryOverhead=512m `
  --conf spark.sql.shuffle.partitions=48 `
  --conf spark.app.name="Gold_Fact_Province_Content_Engagement" `
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog `
  --conf spark.sql.catalog.gold.type=hive `
  --conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 `
  --conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse `
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
  --conf spark.hadoop.fs.s3a.access.key=minioadmin `
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 `
  --conf spark.hadoop.fs.s3a.path.style.access=true `
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
  /opt/spark/jobs/gold/fact_province_content_engagement/fact_province_content_engagement_job.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "   [SUCCESS] Job completed successfully!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next Steps:" -ForegroundColor Yellow
    Write-Host "   1. spark.table('gold.fact_province_content_engagement').orderBy('date_sk').show()" -ForegroundColor Cyan
    Write-Host "   2. Check MinIO: http://localhost:9001 (gold/lakehouse/fact_province_content_engagement)" -ForegroundColor Cyan
    Write-Host "   3. Refresh metadata in Dremio (hive_metastore.gold.fact_province_content_engagement)" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Red
    Write-Host "   [FAILED] Job failed!" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "   1. docker logs lakehouse_spark_master" -ForegroundColor Cyan
    Write-Host "   2. spark.table('gold.gold.dim_post').count()" -ForegroundColor Cyan
    Write-Host "   3. spark.table('silver.silver.tiktok_post_metadata').count()" -ForegroundColor Cyan
    Write-Host "   4. docker logs lakehouse_hive_metastore" -ForegroundColor Cyan
    Write-Host ""
    exit 1
}


