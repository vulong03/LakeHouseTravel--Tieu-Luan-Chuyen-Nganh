# Test TikTok Videos Pipeline (2-task pattern)
# Step 1: Transform (Bronze → Scratch)
# Step 2: Clean & Load (Scratch → Silver)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "TIKTOK VIDEOS PIPELINE TEST" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$ErrorCount = 0

# Step 1: Transform
Write-Host "`n[1/2] Running Step 1: Transform (Bronze → Scratch)" -ForegroundColor Yellow
Write-Host "------------------------------------------------"

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --jars /opt/spark/jars/postgresql-42.7.2.jar `
    /opt/spark/jobs/silver/tiktok_videos/step_01_transform.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Step 1 failed" -ForegroundColor Red
    $ErrorCount++
} else {
    Write-Host "[OK] Step 1 completed successfully" -ForegroundColor Green
}

# Step 2: Clean & Load
Write-Host "`n[2/2] Running Step 2: Clean & Load (Scratch → Silver)" -ForegroundColor Yellow
Write-Host "------------------------------------------------"

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --jars /opt/spark/jars/postgresql-42.7.2.jar `
    /opt/spark/jobs/silver/tiktok_videos/step_02_clean_load.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Step 2 failed" -ForegroundColor Red
    $ErrorCount++
} else {
    Write-Host "[OK] Step 2 completed successfully" -ForegroundColor Green
}

Write-Host "`n🎉 TikTok Videos pipeline finished successfully!" -ForegroundColor Green

# Verification
if ($ErrorCount -eq 0) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "[OK] PIPELINE COMPLETED SUCCESSFULLY" -ForegroundColor Green
    
    Write-Host "`n[*] Verifying data in Silver table..." -ForegroundColor Yellow
    
    # Count records
    docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog `
        --conf spark.sql.catalog.silver.type=hive `
        --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 `
        --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse `
        -e "SELECT COUNT(*) FROM silver.silver.tiktok_videos;"
    
    # Check table location
    Write-Host "`n[*] Checking table location..." -ForegroundColor Yellow
    docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog `
        --conf spark.sql.catalog.silver.type=hive `
        --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 `
        --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse `
        -e "DESCRIBE EXTENDED silver.silver.tiktok_videos;" | Select-String "Location"
    
    # Sample data
    Write-Host "`n[*] Sample data (5 rows):" -ForegroundColor Yellow
    docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        --conf spark.sql.catalog.silver=org.apache.iceberg.spark.SparkCatalog `
        --conf spark.sql.catalog.silver.type=hive `
        --conf spark.sql.catalog.silver.uri=thrift://hive-metastore:9083 `
        --conf spark.sql.catalog.silver.warehouse=s3a://silver/lakehouse `
        -e "SELECT url, keyword, region, read_status, posted_date FROM silver.silver.tiktok_videos LIMIT 5;"
    
} else {
    Write-Host "`n========================================" -ForegroundColor Red
    Write-Host "[ERROR] PIPELINE FAILED ($ErrorCount errors)" -ForegroundColor Red
    exit 1
}
