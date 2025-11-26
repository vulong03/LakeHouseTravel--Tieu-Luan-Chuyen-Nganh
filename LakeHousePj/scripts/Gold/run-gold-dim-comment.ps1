# ============================================
# Run Gold Layer - dim_comment Job
# ============================================
# Purpose: Build dimension table for comments from Silver layer
# Source: silver.silver.tiktok_post_comments
# Target: gold.gold.dim_comment (Iceberg table)
# Mode: Incremental (MERGE INTO - SCD Type 2)
# ============================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   Gold Layer - dim_comment Job" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[*] Job Details:" -ForegroundColor Yellow
Write-Host "   Source: silver.silver.tiktok_post_comments" -ForegroundColor White
Write-Host "   Target: gold.gold.dim_comment" -ForegroundColor White
Write-Host "   Mode: Incremental (MERGE INTO)" -ForegroundColor White
Write-Host "   Strategy: SCD Type 2 (insert/update/skip by row_checksum)" -ForegroundColor White
Write-Host ""

Write-Host "[*] Submitting Spark job..." -ForegroundColor Green
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.app.name="Gold_Dim_Comment" `
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
  /opt/spark/jobs/gold/dim_comment/dim_comment_job.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "   [SUCCESS] Job completed successfully!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next Steps:" -ForegroundColor Yellow
    Write-Host "   1. Query table: spark.table('gold.gold.dim_comment').show()" -ForegroundColor Cyan
    Write-Host "   2. Check MinIO: http://localhost:9001 (gold bucket)" -ForegroundColor Cyan
    Write-Host "   3. Check Hive Metastore: Iceberg metadata stored" -ForegroundColor Cyan
    Write-Host "   4. Check job logs: SELECT * FROM gold_job_logs WHERE table_name = 'gold.gold.dim_comment' ORDER BY execution_time DESC LIMIT 1;" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Red
    Write-Host "   [FAILED] Job failed!" -ForegroundColor Red
    Write-Host "============================================" -ForegroundColor Red
    Write-Host ""
    Write-Host "Troubleshooting:" -ForegroundColor Yellow
    Write-Host "   1. Check Spark logs: docker logs lakehouse_spark_master" -ForegroundColor Cyan
    Write-Host "   2. Check Silver table: spark.table('silver.silver.tiktok_post_comments').count()" -ForegroundColor Cyan
    Write-Host "   3. Check dimension tables: dim_post, dim_date must exist" -ForegroundColor Cyan
    Write-Host "   4. Check Hive Metastore: docker logs lakehouse_hive_metastore" -ForegroundColor Cyan
    Write-Host "   5. Check data quality: SELECT level_comment, COUNT(*) FROM silver.silver.tiktok_post_comments GROUP BY level_comment" -ForegroundColor Cyan
    Write-Host ""
    exit 1
}

