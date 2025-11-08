#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Comprehensive Silver Layer Verification Script

.DESCRIPTION
    Kiểm tra toàn diện dữ liệu trong Silver layer:
    - Danh sách bảng
    - Số lượng records
    - Kiểm tra duplicate
    - Kiểm tra NULL values
    - Thống kê chất lượng dữ liệu
#>

$SPARK_SQL = "docker exec lakehouse_spark_master /opt/spark/bin/spark-sql"
$SPARK_CONF = "--master spark://spark-master:7077 --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.lakehouse.type=hive --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "🔍 SILVER LAYER VERIFICATION REPORT" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# 1. List all tables
Write-Host "📋 1. DANH SÁCH BẢNG TRONG SILVER" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray
& docker exec lakehouse_spark_master /opt/spark/bin/spark-sql $SPARK_CONF.Split() -e "USE lakehouse.silver; SHOW TABLES;" 2>$null | Select-String -Pattern "^[a-z_]" | ForEach-Object { Write-Host "   - $_" -ForegroundColor White }
Write-Host ""

# 2. Hotels Reviews
Write-Host "📊 2. HOTELS REVIEWS" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray
$query = "USE silver; SELECT COUNT(*) as total_reviews, COUNT(DISTINCT hotel_url) as unique_hotels, COUNT(DISTINCT source_file) as unique_files FROM hotels_reviews;"
Write-Host "   Running query..." -ForegroundColor DarkGray
& docker exec lakehouse_spark_master /opt/spark/bin/spark-sql $SPARK_CONF.Split() -e $query 2>$null | Select-String -Pattern "^\d" | ForEach-Object {
    $parts = $_ -split '\s+'
    Write-Host "   Total reviews:    $($parts[0])" -ForegroundColor Green
    Write-Host "   Unique hotels:    $($parts[1])" -ForegroundColor Green
    Write-Host "   Unique files:     $($parts[2])" -ForegroundColor Green
}
Write-Host ""

# 3. Hotels Detail
Write-Host "📊 3. HOTELS DETAIL" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray
$query = "USE silver; SELECT COUNT(*) as total_hotels, COUNT(DISTINCT hotel_url) as unique_urls, COUNT(DISTINCT source_file) as unique_files FROM hotels_detail;"
Write-Host "   Running query..." -ForegroundColor DarkGray
& docker exec lakehouse_spark_master /opt/spark/bin/spark-sql $SPARK_CONF.Split() -e $query 2>$null | Select-String -Pattern "^\d" | ForEach-Object {
    $parts = $_ -split '\s+'
    Write-Host "   Total hotels:     $($parts[0])" -ForegroundColor Green
    Write-Host "   Unique URLs:      $($parts[1])" -ForegroundColor Green
    Write-Host "   Unique files:     $($parts[2])" -ForegroundColor Green
}
Write-Host ""

# 4. TikTok Post Metadata
Write-Host "📊 4. TIKTOK POST METADATA" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray
$query = "USE silver; SELECT COUNT(*) as total_posts, COUNT(DISTINCT post_url) as unique_posts, COUNT(DISTINCT source_file) as unique_files, COUNT(DISTINCT source_file_checksum) as unique_checksums FROM tiktok_post_metadata;"
Write-Host "   Running query..." -ForegroundColor DarkGray
& docker exec lakehouse_spark_master /opt/spark/bin/spark-sql $SPARK_CONF.Split() -e $query 2>$null | Select-String -Pattern "^\d" | ForEach-Object {
    $parts = $_ -split '\s+'
    Write-Host "   Total posts:      $($parts[0])" -ForegroundColor Green
    Write-Host "   Unique URLs:      $($parts[1])" -ForegroundColor Green
    Write-Host "   Unique files:     $($parts[2])" -ForegroundColor Green
    Write-Host "   Unique checksums: $($parts[3])" -ForegroundColor Green
    
    if ($parts[0] -eq $parts[2]) {
        Write-Host "   ✅ No file duplicates" -ForegroundColor Green
    } else {
        Write-Host "   ⚠️  File duplicates detected!" -ForegroundColor Red
    }
}
Write-Host ""

# 5. TikTok Post Comments
Write-Host "📊 5. TIKTOK POST COMMENTS" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray
$query = "USE silver; SELECT COUNT(*) as total_comments, COUNT(DISTINCT post_url) as unique_posts, COUNT(DISTINCT source_file) as unique_files FROM tiktok_post_comments;"
Write-Host "   Running query..." -ForegroundColor DarkGray
& docker exec lakehouse_spark_master /opt/spark/bin/spark-sql $SPARK_CONF.Split() -e $query 2>$null | Select-String -Pattern "^\d" | ForEach-Object {
    $parts = $_ -split '\s+'
    Write-Host "   Total comments:   $($parts[0])" -ForegroundColor Green
    Write-Host "   Unique posts:     $($parts[1])" -ForegroundColor Green
    Write-Host "   Unique files:     $($parts[2])" -ForegroundColor Green
}
Write-Host ""

# 6. Check NULL values in TikTok metadata
Write-Host "🔎 6. NULL VALUES CHECK (TikTok Metadata)" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray
$query = "USE silver; SELECT SUM(CASE WHEN post_url IS NULL THEN 1 ELSE 0 END) as null_post_url, SUM(CASE WHEN author IS NULL THEN 1 ELSE 0 END) as null_author, SUM(CASE WHEN likes IS NULL THEN 1 ELSE 0 END) as null_likes, SUM(CASE WHEN comments_count IS NULL THEN 1 ELSE 0 END) as null_comments FROM tiktok_post_metadata;"
Write-Host "   Running query..." -ForegroundColor DarkGray
& docker exec lakehouse_spark_master /opt/spark/bin/spark-sql $SPARK_CONF.Split() -e $query 2>$null | Select-String -Pattern "^\d" | ForEach-Object {
    $parts = $_ -split '\s+'
    $hasNulls = $false
    if ($parts[0] -ne "0") { Write-Host "   NULL post_url:    $($parts[0])" -ForegroundColor Red; $hasNulls = $true }
    if ($parts[1] -ne "0") { Write-Host "   NULL author:      $($parts[1])" -ForegroundColor Red; $hasNulls = $true }
    if ($parts[2] -ne "0") { Write-Host "   NULL likes:       $($parts[2])" -ForegroundColor Red; $hasNulls = $true }
    if ($parts[3] -ne "0") { Write-Host "   NULL comments:    $($parts[3])" -ForegroundColor Red; $hasNulls = $true }
    
    if (-not $hasNulls) {
        Write-Host "   ✅ No NULL values in key fields" -ForegroundColor Green
    }
}
Write-Host ""

# 7. Date range check
Write-Host "📅 7. DATE RANGE (TikTok Posts)" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray
$query = "USE silver; SELECT MIN(crawl_time) as earliest_crawl, MAX(crawl_time) as latest_crawl FROM tiktok_post_metadata;"
Write-Host "   Running query..." -ForegroundColor DarkGray
& docker exec lakehouse_spark_master /opt/spark/bin/spark-sql $SPARK_CONF.Split() -e $query 2>$null | Select-String -Pattern "GMT" | ForEach-Object {
    Write-Host "   $_" -ForegroundColor Cyan
}
Write-Host ""

# 8. Top authors
Write-Host "👥 8. TOP 10 AUTHORS (Most Posts)" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray
$query = "USE silver; SELECT author, COUNT(*) as post_count FROM tiktok_post_metadata GROUP BY author ORDER BY post_count DESC LIMIT 10;"
Write-Host "   Running query..." -ForegroundColor DarkGray
& docker exec lakehouse_spark_master /opt/spark/bin/spark-sql $SPARK_CONF.Split() -e $query 2>$null | Select-String -Pattern "^@|^\w" | ForEach-Object {
    Write-Host "   $_" -ForegroundColor White
}
Write-Host ""

# 9. Tracking log check
Write-Host "📝 9. TRACKING LOG STATUS" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray
Write-Host "   Querying PostgreSQL..." -ForegroundColor DarkGray
& docker exec lakehouse_postgres sh -c 'PGPASSWORD=lakehouse_pass psql -U lakehouse_user -d metastore_db -t -c "SELECT layer, COUNT(*) FROM file_ingestion_log GROUP BY layer ORDER BY layer;"' 2>$null | ForEach-Object {
    if ($_ -match '\S') {
        Write-Host "   $_" -ForegroundColor White
    }
}
Write-Host ""

# Summary
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "✅ VERIFICATION COMPLETED" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "💡 Quick Commands:" -ForegroundColor Yellow
Write-Host "   Count all records:       .\scripts\count-silver-records.ps1" -ForegroundColor Gray
Write-Host "   Check duplicates:        .\scripts\check-silver-duplicates.ps1" -ForegroundColor Gray
Write-Host "   View sample data:        .\scripts\sample-silver-data.ps1" -ForegroundColor Gray
Write-Host ""
