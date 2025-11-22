# ============================================
# Run Gold Layer - dim_date Job
# ============================================
# Purpose: Build dimension table for dates from PostgreSQL
# Source: PostgreSQL Date_DB.dim_date
# Target: gold.dim_date (Iceberg table)
# ============================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   Gold Layer - dim_date Job" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[*] Job Details:" -ForegroundColor Yellow
Write-Host "   Source: PostgreSQL Date_DB.dim_date" -ForegroundColor White
Write-Host "   Target: gold.dim_date" -ForegroundColor White
Write-Host "   Mode: Full Refresh (Overwrite)" -ForegroundColor White
Write-Host ""

Write-Host "[*] Submitting Spark job..." -ForegroundColor Green
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --deploy-mode client `
  --conf spark.app.name="Gold_Dim_Date" `
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
  --jars /opt/spark/jars/postgresql-42.7.2.jar `
  --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar `
  /opt/spark/jobs/gold/dim_date/dim_date_job.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "============================================" -ForegroundColor Green
    Write-Host "   [SUCCESS] Job completed successfully!" -ForegroundColor Green
    Write-Host "============================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next Steps:" -ForegroundColor Yellow
    Write-Host "   1. Query table: spark.table('gold.dim_date').show()" -ForegroundColor Cyan
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
    Write-Host "   2. Check PostgreSQL: docker exec lakehouse_postgres psql -U lakehouse_user -d Date_DB -c 'SELECT COUNT(*) FROM dim_date;'" -ForegroundColor Cyan
    Write-Host "   3. Check Hive Metastore: docker logs lakehouse_hive_metastore" -ForegroundColor Cyan
    Write-Host ""
    exit 1
}

