#!/usr/bin/env pwsh
# Data Quality Analysis Script for Silver Hotels Reviews

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "SILVER HOTELS REVIEWS - DATA QUALITY ANALYSIS" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan

$ErrorActionPreference = "Continue"

# 1. Basic Statistics
Write-Host "`n[1] BASIC STATISTICS" -ForegroundColor Yellow
Write-Host "--------------------" -ForegroundColor Yellow

Write-Host "`nTotal records:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "SELECT COUNT(*) as total_records FROM silver.silver.hotels_reviews;"

Write-Host "`nSnapshot count:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "SELECT COUNT(*) as snapshots FROM silver.silver.hotels_reviews.snapshots;"

# 2. Data Quality - NULL values
Write-Host "`n[2] DATA QUALITY - NULL VALUES" -ForegroundColor Yellow
Write-Host "-------------------------------" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN hotel_name IS NULL THEN 1 ELSE 0 END) as null_hotel_name,
    SUM(CASE WHEN reviewer_name IS NULL THEN 1 ELSE 0 END) as null_reviewer_name,
    SUM(CASE WHEN review_date IS NULL THEN 1 ELSE 0 END) as null_review_date,
    SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END) as null_review_score,
    SUM(CASE WHEN traveler_type IS NULL THEN 1 ELSE 0 END) as null_traveler_type,
    SUM(CASE WHEN review_title IS NULL THEN 1 ELSE 0 END) as null_review_title,
    SUM(CASE WHEN review_positive IS NULL THEN 1 ELSE 0 END) as null_review_positive,
    SUM(CASE WHEN review_negative IS NULL THEN 1 ELSE 0 END) as null_review_negative
FROM silver.silver.hotels_reviews;
"

# 3. Duplicate Detection
Write-Host "`n[3] DUPLICATE DETECTION (by row_checksum)" -ForegroundColor Yellow
Write-Host "------------------------------------------" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    COUNT(*) as total_checksums,
    COUNT(DISTINCT row_checksum) as unique_checksums,
    COUNT(*) - COUNT(DISTINCT row_checksum) as duplicates
FROM silver.silver.hotels_reviews;
"

Write-Host "`nDuplicate checksums (if any):" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT row_checksum, COUNT(*) as count 
FROM silver.silver.hotels_reviews 
GROUP BY row_checksum 
HAVING COUNT(*) > 1 
LIMIT 10;
"

# 4. Date Distribution
Write-Host "`n[4] DATE DISTRIBUTION" -ForegroundColor Yellow
Write-Host "---------------------" -ForegroundColor Yellow

Write-Host "`nRecords by year:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    YEAR(review_date) as year, 
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM silver.silver.hotels_reviews 
GROUP BY YEAR(review_date) 
ORDER BY year;
"

Write-Host "`nRecords by year-month (last 12 months):" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    YEAR(review_date) as year,
    MONTH(review_date) as month,
    COUNT(*) as count
FROM silver.silver.hotels_reviews 
WHERE review_date IS NOT NULL
GROUP BY YEAR(review_date), MONTH(review_date)
ORDER BY year DESC, month DESC
LIMIT 12;
"

# 5. Review Score Distribution
Write-Host "`n[5] REVIEW SCORE DISTRIBUTION" -ForegroundColor Yellow
Write-Host "-----------------------------" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    review_score,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM silver.silver.hotels_reviews 
WHERE review_score IS NOT NULL
GROUP BY review_score 
ORDER BY review_score DESC;
"

# 6. Traveler Type Distribution
Write-Host "`n[6] TRAVELER TYPE DISTRIBUTION" -ForegroundColor Yellow
Write-Host "-------------------------------" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    traveler_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM silver.silver.hotels_reviews 
GROUP BY traveler_type 
ORDER BY count DESC;
"

# 7. Top Hotels by Review Count
Write-Host "`n[7] TOP 20 HOTELS BY REVIEW COUNT" -ForegroundColor Yellow
Write-Host "----------------------------------" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    hotel_name,
    COUNT(*) as review_count,
    ROUND(AVG(review_score), 2) as avg_score,
    MIN(review_date) as first_review,
    MAX(review_date) as last_review
FROM silver.silver.hotels_reviews 
WHERE hotel_name IS NOT NULL
GROUP BY hotel_name 
ORDER BY review_count DESC 
LIMIT 20;
"

# 8. Data Completeness
Write-Host "`n[8] DATA COMPLETENESS RATE" -ForegroundColor Yellow
Write-Host "--------------------------" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    ROUND((COUNT(*) - SUM(CASE WHEN hotel_name IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) as hotel_name_completeness,
    ROUND((COUNT(*) - SUM(CASE WHEN review_date IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) as review_date_completeness,
    ROUND((COUNT(*) - SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) as review_score_completeness,
    ROUND((COUNT(*) - SUM(CASE WHEN traveler_type IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) as traveler_type_completeness,
    ROUND((COUNT(*) - SUM(CASE WHEN review_positive IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) as review_positive_completeness,
    ROUND((COUNT(*) - SUM(CASE WHEN review_negative IS NULL THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) as review_negative_completeness
FROM silver.silver.hotels_reviews;
"

# 9. Sample Records
Write-Host "`n[9] SAMPLE RECORDS (Random 10)" -ForegroundColor Yellow
Write-Host "-------------------------------" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    hotel_name,
    reviewer_name,
    review_date,
    review_score,
    traveler_type,
    SUBSTRING(review_positive, 1, 50) as review_preview
FROM silver.silver.hotels_reviews 
ORDER BY RAND() 
LIMIT 10;
"

# 10. Iceberg Metadata
Write-Host "`n[10] ICEBERG METADATA" -ForegroundColor Yellow
Write-Host "---------------------" -ForegroundColor Yellow

Write-Host "`nSnapshots:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary
FROM silver.silver.hotels_reviews.snapshots 
ORDER BY committed_at DESC;
"

Write-Host "`nData files:" -ForegroundColor Cyan
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    -e "
SELECT 
    COUNT(*) as file_count,
    ROUND(SUM(file_size_in_bytes) / 1024 / 1024, 2) as total_size_mb,
    ROUND(AVG(file_size_in_bytes) / 1024 / 1024, 2) as avg_file_size_mb,
    SUM(record_count) as total_records
FROM silver.silver.hotels_reviews.files;
"

Write-Host "`n================================================================================" -ForegroundColor Green
Write-Host "✅ ANALYSIS COMPLETED!" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green
