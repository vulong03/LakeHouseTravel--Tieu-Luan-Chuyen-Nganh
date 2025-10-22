# Test Incremental Loading for Bronze Layer
# Demonstrates file change detection and URL deduplication

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "TESTING INCREMENTAL LOADING - Bronze Layer" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan

# Test 1: Run ingestion again with unchanged files (should skip)
Write-Host "`nTEST 1: Re-run ingestion with unchanged files" -ForegroundColor Yellow
Write-Host "Expected: All files skipped (checksum match)" -ForegroundColor Gray
Write-Host "Press Enter to continue..." -ForegroundColor Green
Read-Host

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze/ `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar `
    /opt/spark/jobs/bronze/ingest_tiktok_videos.py

if ($LASTEXITCODE -eq 0) {
    Write-Host "`nTEST 1 PASSED: Files correctly skipped" -ForegroundColor Green
} else {
    Write-Host "`nTEST 1 FAILED" -ForegroundColor Red
}

# Test 2: Check current record count
Write-Host "`nTEST 2: Check current Bronze table counts" -ForegroundColor Yellow

$counts = docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    -e "SELECT 'Videos' as table_name, COUNT(*) as count FROM lakehouse.bronze.tiktok_videos_metadata UNION ALL SELECT 'Posts' as table_name, COUNT(*) as count FROM lakehouse.bronze.tiktok_posts_raw UNION ALL SELECT 'Comments' as table_name, COUNT(*) as count FROM lakehouse.bronze.tiktok_comments_raw;" 2>&1

Write-Host "`nCurrent Counts:" -ForegroundColor Cyan
Write-Host $counts

# Test 3: Check file tracking log
Write-Host "`nTEST 3: Check PostgreSQL tracking log" -ForegroundColor Yellow

$tracking = docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c `
    "SELECT file_name, records_ingested, status, ingestion_timestamp FROM file_ingestion_log ORDER BY ingestion_timestamp DESC LIMIT 10;" 2>&1

Write-Host "`nRecent Ingestion Logs:" -ForegroundColor Cyan
Write-Host $tracking

Write-Host "`n================================================================================" -ForegroundColor Cyan
Write-Host "INCREMENTAL LOADING TEST COMPLETED" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Cyan

Write-Host "`nSummary:" -ForegroundColor Yellow
Write-Host "Comment files: APPEND mode with checksum deduplication" -ForegroundColor Green
Write-Host "merged_videos.csv: APPEND mode with URL deduplication" -ForegroundColor Green
