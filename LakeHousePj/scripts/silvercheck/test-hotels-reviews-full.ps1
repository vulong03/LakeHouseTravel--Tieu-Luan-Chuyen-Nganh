#!/usr/bin/env pwsh
# Test script: Hotels Reviews - Full processing (1.5M records at once)

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "Hotels Reviews Silver Pipeline - FULL MODE (Optimized Dedup)" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan

$ErrorActionPreference = "Stop"

# Step 1: Bronze -> Scratch (1.5M records)
Write-Host "`n================================================================================" -ForegroundColor Yellow
Write-Host "STEP 1: Transform Bronze -> Scratch" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    /opt/spark/jobs/silver/hotels_reviews/step_01_transform.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "`n[X] Step 1 FAILED" -ForegroundColor Red
    exit 1
}

Write-Host "`n[✓] Step 1 COMPLETED" -ForegroundColor Green

# Step 2: Scratch -> Silver (deduplicate + append all records)
Write-Host "`n================================================================================" -ForegroundColor Yellow
Write-Host "STEP 2: Clean and Load Scratch to Silver (ALL records)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    /opt/spark/jobs/silver/hotels_reviews/step_02_clean_load.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "`n[X] Step 2 FAILED" -ForegroundColor Red
    exit 1
}

Write-Host "`n[✓] Step 2 COMPLETED" -ForegroundColor Green

# Verification: Check final count
Write-Host "`n================================================================================" -ForegroundColor Yellow
Write-Host "VERIFICATION: Final Silver table stats" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

Write-Host "`nTotal records in Silver:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "SELECT COUNT(*) as total_records FROM silver.silver.hotels_reviews;"

Write-Host "`nRecords by year:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "SELECT YEAR(review_date) as year, COUNT(*) as count FROM silver.silver.hotels_reviews GROUP BY YEAR(review_date) ORDER BY year;"

Write-Host "`nTop 10 year-months:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "SELECT YEAR(review_date) as year, MONTH(review_date) as month, COUNT(*) as count FROM silver.silver.hotels_reviews GROUP BY YEAR(review_date), MONTH(review_date) ORDER BY year DESC, month DESC LIMIT 10;"

Write-Host "`n================================================================================" -ForegroundColor Green
Write-Host "✅ PIPELINE COMPLETED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green
#!/usr/bin/env pwsh
# Test script: Hotels Reviews - Full processing (1.5M records at once)

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "Hotels Reviews Silver Pipeline - FULL MODE (Optimized Dedup)" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan

$ErrorActionPreference = "Stop"

# Step 1: Bronze -> Scratch (1.5M records)
Write-Host "`n================================================================================" -ForegroundColor Yellow
Write-Host "STEP 1: Transform Bronze -> Scratch" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    /opt/spark/jobs/silver/hotels_reviews/step_01_transform.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "`n[X] Step 1 FAILED" -ForegroundColor Red
    exit 1
}

Write-Host "`n[✓] Step 1 COMPLETED" -ForegroundColor Green

# Step 2: Scratch -> Silver (deduplicate + append all records)
Write-Host "`n================================================================================" -ForegroundColor Yellow
Write-Host "STEP 2: Clean and Load Scratch to Silver (ALL records)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    /opt/spark/jobs/silver/hotels_reviews/step_02_clean_load.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "`n[X] Step 2 FAILED" -ForegroundColor Red
    exit 1
}

Write-Host "`n[✓] Step 2 COMPLETED" -ForegroundColor Green

# Verification: Check final count
Write-Host "`n================================================================================" -ForegroundColor Yellow
Write-Host "VERIFICATION: Final Silver table stats" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Yellow

Write-Host "`nTotal records in Silver:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "SELECT COUNT(*) as total_records FROM silver.silver.hotels_reviews;"

Write-Host "`nRecords by year:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "SELECT YEAR(review_date) as year, COUNT(*) as count FROM silver.silver.hotels_reviews GROUP BY YEAR(review_date) ORDER BY year;"

Write-Host "`nTop 10 year-months:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "SELECT YEAR(review_date) as year, MONTH(review_date) as month, COUNT(*) as count FROM silver.silver.hotels_reviews GROUP BY YEAR(review_date), MONTH(review_date) ORDER BY year DESC, month DESC LIMIT 10;"

Write-Host "`n================================================================================" -ForegroundColor Green
Write-Host "✅ PIPELINE COMPLETED SUCCESSFULLY!" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green
