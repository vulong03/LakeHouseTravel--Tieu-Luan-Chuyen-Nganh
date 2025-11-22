# ============================================
# Run Gold Layer - dim_post Job
# ============================================
# Purpose: Build dimension table for posts from Silver layer
# Source: silver.silver.tiktok_post_metadata + silver.silver.tiktok_videos
# Target: gold.dim_post (Iceberg table)
# ============================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   Gold Layer - dim_post Job" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[*] Job Details:" -ForegroundColor Yellow
Write-Host "   Source 1: silver.silver.tiktok_post_metadata" -ForegroundColor White
Write-Host "   Source 2: silver.silver.tiktok_videos" -ForegroundColor White
Write-Host "   Target: gold.dim_post" -ForegroundColor White
Write-Host "   Mode: Full Refresh (Overwrite)" -ForegroundColor White
Write-Host ""

Write-Host "[*] Submitting Spark job..." -ForegroundColor Green
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.app.name="Gold_Dim_Post" `
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
  /opt/spark/jobs/gold/dim_post/dim_post_job.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "   [SUCCESS] Job completed successfully!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next Steps:" -ForegroundColor Yellow
    Write-Host "   1. Query table: spark.table('gold.dim_post').show()" -ForegroundColor Cyan
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
    Write-Host "   2. Check Silver tables: spark.table('silver.silver.tiktok_post_metadata').count()" -ForegroundColor Cyan
    Write-Host "   3. Check dimension tables: dim_author, dim_province, dim_date must exist" -ForegroundColor Cyan
    Write-Host "   4. Check Hive Metastore: docker logs lakehouse_hive_metastore" -ForegroundColor Cyan
    Write-Host ""
    exit 1
}

