#!/usr/bin/env pwsh
# Rebuild dim_date and dim_post with new configurations

$ErrorActionPreference = "Stop"

Write-Host "================================" -ForegroundColor Cyan
Write-Host "Rebuilding dim_date & dim_post" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

# Step 1: Rebuild dim_date (2000-2025)
Write-Host "`n[1/3] Rebuilding dim_date in PostgreSQL date_db..." -ForegroundColor Yellow

# Drop and recreate dim_date table in PostgreSQL date_db
Get-Content "$PSScriptRoot\..\data\DateDimension\Date_Dimension_2015_2025.sql" | docker exec -i lakehouse_postgres psql -U lakehouse_user -d date_db

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to rebuild dim_date in PostgreSQL" -ForegroundColor Red
    exit 1
}

Write-Host "   ✅ dim_date rebuilt in date_db (2000-2025)" -ForegroundColor Green

# Step 2: Sync dim_date to Gold Iceberg
Write-Host "`n[2/3] Syncing dim_date to Gold Iceberg..." -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --executor-memory 1g `
    --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.gold.type=hive `
    --conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minio_admin `
    --conf spark.hadoop.fs.s3a.secret.key=minio_password `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    /opt/spark/jobs/gold/dim_date/dim_date_job.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to sync dim_date to Gold" -ForegroundColor Red
    exit 1
}

Write-Host "   ✅ dim_date synced to Gold successfully" -ForegroundColor Green

# Step 3: Re-run dim_post job with INNER JOIN
Write-Host "`n[3/3] Re-running dim_post job..." -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --num-executors 1 `
    --executor-cores 2 `
    --executor-memory 2G `
    --driver-memory 2G `
    /opt/spark/jobs/gold/dim_post/dim_post_job.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to rebuild dim_post" -ForegroundColor Red
    exit 1
}

Write-Host "`n================================" -ForegroundColor Green
Write-Host "✅ Rebuild completed!" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next: Re-run fact_comment_nlp_engagement job" -ForegroundColor Cyan
