# Bronze Layer Ingestion Script
# Purpose: Run Bronze jobs to ingest TikTok + Booking.com data into lakehouse

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  BRONZE LAYER INGESTION - Full Pipeline" -ForegroundColor Cyan
Write-Host "  - TikTok: Videos & Comments" -ForegroundColor Cyan
Write-Host "  - Booking.com: Hotels, Details & Reviews" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker containers are running
Write-Host "Checking Docker containers..." -ForegroundColor Yellow
$containers = docker ps --format "{{.Names}}" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker is not running or not accessible" -ForegroundColor Red
    exit 1
}

$required_containers = @("lakehouse_spark_master", "lakehouse_postgres", "lakehouse_minio", "lakehouse_hive_metastore")
foreach ($container in $required_containers) {
    if ($containers -notcontains $container) {
        Write-Host "ERROR: Required container '$container' is not running" -ForegroundColor Red
        Write-Host "Please run: docker-compose up -d" -ForegroundColor Yellow
        exit 1
    }
}
Write-Host "All required containers are running" -ForegroundColor Green
Write-Host ""

# Define Spark Submit command template (consistent with Airflow DAG)
$SPARK_SUBMIT_CMD = @"
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --jars /opt/spark/jars/postgresql-42.7.2.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.lakehouse.type=hive \
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze/ \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
"@

# Initialize PostgreSQL tracking table
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "1. Initializing File Tracking Table" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$sql_file = "d:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\LakeHousePj\postgres\init\02_create_file_tracking.sql"

if (Test-Path $sql_file) {
    Write-Host "Creating file_ingestion_log table in PostgreSQL..." -ForegroundColor Yellow
    
    Get-Content $sql_file | docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db 2>&1 | Out-Null
    
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
    --jars /opt/spark/jars/postgresql-42.7.2.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar `
    --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar `
    --conf spark.sql.adaptive.enabled=true `
    --conf spark.sql.adaptive.coalescePartitions.enabled=true `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze/ `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
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
Write-Host "3. Ingesting TikTok Comments" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Source: data/raw/tiktok/comments/*.csv" -ForegroundColor Gray
Write-Host "Target: bronze.tiktok_posts_raw + bronze.tiktok_comments_raw" -ForegroundColor Gray
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --jars /opt/spark/jars/postgresql-42.7.2.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar `
    --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar `
    --conf spark.sql.adaptive.enabled=true `
    --conf spark.sql.adaptive.coalescePartitions.enabled=true `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze/ `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    /opt/spark/jobs/bronze/ingest_tiktok_comments.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "TikTok comments ingestion completed successfully" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "ERROR: TikTok comments ingestion failed" -ForegroundColor Red
    Write-Host "Check logs above for details" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# ========================================
# BOOKING.COM DATA INGESTION
# ========================================

Write-Host "========================================" -ForegroundColor Magenta
Write-Host "  BOOKING.COM DATA INGESTION" -ForegroundColor Magenta
Write-Host "========================================" -ForegroundColor Magenta
Write-Host ""

# Job 3: Ingest Booking Hotels List
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "4. Ingesting Booking Hotels List" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Source: data/raw/booking/vietnam_hotels_list.csv" -ForegroundColor Gray
Write-Host "Target: bronze.raw_booking_hotels_list" -ForegroundColor Gray
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --jars /opt/spark/jars/postgresql-42.7.2.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar `
    --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar `
    --conf spark.sql.adaptive.enabled=true `
    --conf spark.sql.adaptive.coalescePartitions.enabled=true `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze/ `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    /opt/spark/jobs/bronze/ingest_booking_hotels_list.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "Booking hotels list ingestion completed successfully" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "ERROR: Booking hotels list ingestion failed" -ForegroundColor Red
    Write-Host "Check logs above for details" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Job 4: Ingest Booking Hotels Detail
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "5. Ingesting Booking Hotels Detail" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Source: data/raw/booking/vietnam_hotels_detail.csv" -ForegroundColor Gray
Write-Host "Target: bronze.raw_booking_hotels_detail" -ForegroundColor Gray
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --jars /opt/spark/jars/postgresql-42.7.2.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar `
    --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar `
    --conf spark.sql.adaptive.enabled=true `
    --conf spark.sql.adaptive.coalescePartitions.enabled=true `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze/ `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    /opt/spark/jobs/bronze/ingest_booking_hotels_detail.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "Booking hotels detail ingestion completed successfully" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "ERROR: Booking hotels detail ingestion failed" -ForegroundColor Red
    Write-Host "Check logs above for details" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Job 5: Ingest Booking Hotels Reviews
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "6. Ingesting Booking Hotels Reviews" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Source: data/raw/booking/vietnam_hotels_reviews.csv" -ForegroundColor Gray
Write-Host "Target: bronze.raw_booking_hotels_reviews" -ForegroundColor Gray
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --jars /opt/spark/jars/postgresql-42.7.2.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar `
    --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar `
    --conf spark.sql.adaptive.enabled=true `
    --conf spark.sql.adaptive.coalescePartitions.enabled=true `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://bronze/ `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    /opt/spark/jobs/bronze/ingest_booking_hotels_reviews.py

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "Booking hotels reviews ingestion completed successfully" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "ERROR: Booking hotels reviews ingestion failed" -ForegroundColor Red
    Write-Host "Check logs above for details" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Summary
Write-Host "========================================" -ForegroundColor Green
Write-Host "  BRONZE INGESTION COMPLETED" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Ingested Tables:" -ForegroundColor Yellow
Write-Host ""
Write-Host "  TikTok Data:" -ForegroundColor Cyan
Write-Host "    - bronze.raw_tiktok_video_links" -ForegroundColor White
Write-Host "    - bronze.raw_tiktok_post_metadata" -ForegroundColor White
Write-Host "    - bronze.raw_tiktok_post_comments" -ForegroundColor White
Write-Host ""
Write-Host "  Booking.com Data:" -ForegroundColor Magenta
Write-Host "    - bronze.raw_booking_hotels_list" -ForegroundColor White
Write-Host "    - bronze.raw_booking_hotels_detail" -ForegroundColor White
Write-Host "    - bronze.raw_booking_hotels_reviews" -ForegroundColor White
Write-Host ""
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  Next Steps" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""
Write-Host "Check MinIO Console at: http://localhost:9001" -ForegroundColor White
Write-Host "Credentials: minioadmin / minioadmin123" -ForegroundColor Gray
Write-Host ""
Write-Host "Query PostgreSQL tracking:" -ForegroundColor White
Write-Host "  docker exec -it lakehouse_postgres psql -U lakehouse_user -d metastore_db" -ForegroundColor Gray
Write-Host ""
Write-Host "Query Bronze tables:" -ForegroundColor White
Write-Host "  docker exec -it lakehouse_spark_master spark-sql" -ForegroundColor Gray
Write-Host ""
Write-Host "Ready for Silver Layer transformation!" -ForegroundColor Green
Write-Host ""
