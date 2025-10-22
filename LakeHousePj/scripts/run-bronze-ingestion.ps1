# Bronze Layer Ingestion Script
# Purpose: Run Bronze jobs to ingest TikTok data into lakehouse

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  BRONZE LAYER INGESTION - TikTok Data" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker containers are running
Write-Host "Checking Docker containers..." -ForegroundColor Yellow
$containers = docker ps --format "{{.Names}}" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker is not running or not accessible" -ForegroundColor Red
    exit 1
}

$required_containers = @("lakehouse_spark_master", "lakehouse_postgres", "lakehouse_minio")
foreach ($container in $required_containers) {
    if ($containers -notcontains $container) {
        Write-Host "ERROR: Required container '$container' is not running" -ForegroundColor Red
        Write-Host "Please run: docker-compose up -d" -ForegroundColor Yellow
        exit 1
    }
}
Write-Host "All required containers are running" -ForegroundColor Green
Write-Host ""

# Initialize PostgreSQL tracking table
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "1. Initializing File Tracking Table" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$sql_file = "d:\CodeStored\Nam_4\TieuLuan\LakeHousePj\postgres\init\02_create_file_tracking.sql"

if (Test-Path $sql_file) {
    Write-Host "Creating file_ingestion_log table in PostgreSQL..." -ForegroundColor Yellow
    
    Get-Content $sql_file | docker exec -i lakehouse_postgres psql -U hive -d metastore_db 2>&1 | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "File tracking table created successfully" -ForegroundColor Green
    } else {
        Write-Host "Warning: File tracking table might already exist (this is OK)" -ForegroundColor Yellow
    }
} else {
    Write-Host "Warning: SQL file not found: $sql_file" -ForegroundColor Yellow
}
Write-Host ""

# Job 1: Ingest Videos Metadata
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "2. Ingesting Videos Metadata" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Source: data/raw/tiktok/links/merged_videos.csv" -ForegroundColor Gray
Write-Host "Target: bronze.tiktok_videos_metadata" -ForegroundColor Gray
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze/ `
    /opt/spark/jobs/bronze/ingest_tiktok_videos.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "Videos metadata ingestion completed successfully" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "ERROR: Videos metadata ingestion failed" -ForegroundColor Red
    Write-Host "Check logs above for details" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Job 2: Ingest Comment Files
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "3. Ingesting Comment Files" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Source: data/raw/tiktok/comments/*.csv" -ForegroundColor Gray
Write-Host "Target: bronze.tiktok_posts_raw + bronze.tiktok_comments_raw" -ForegroundColor Gray
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze/ `
    /opt/spark/jobs/bronze/ingest_tiktok_comments.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "Comment files ingestion completed successfully" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "ERROR: Comment files ingestion failed" -ForegroundColor Red
    Write-Host "Check logs above for details" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Summary
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  BRONZE INGESTION COMPLETED" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Check MinIO Console: http://localhost:9001" -ForegroundColor White
Write-Host "   - Navigate to 'bronze' bucket to see ingested data" -ForegroundColor Gray
Write-Host ""
Write-Host "2. Check pgAdmin: http://localhost:5050" -ForegroundColor White
Write-Host "   - View file_ingestion_log table for tracking" -ForegroundColor Gray
Write-Host ""
Write-Host "3. Query Bronze tables via Spark SQL:" -ForegroundColor White
Write-Host "   docker exec -it spark-master spark-sql" -ForegroundColor Gray
Write-Host "   > SELECT * FROM lakehouse.bronze.tiktok_videos_metadata LIMIT 5;" -ForegroundColor Gray
Write-Host "   > SELECT * FROM lakehouse.bronze.tiktok_posts_raw LIMIT 5;" -ForegroundColor Gray
Write-Host "   > SELECT * FROM lakehouse.bronze.tiktok_comments_raw LIMIT 10;" -ForegroundColor Gray
Write-Host ""
