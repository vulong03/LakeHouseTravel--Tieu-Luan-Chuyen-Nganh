#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Check for duplicate records in Silver layer
#>

Write-Host "🔍 DUPLICATE CHECK - SILVER LAYER" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Gray

# TikTok Posts
Write-Host "`n📊 TIKTOK POST METADATA" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray

$query = @"
USE silver; 
SELECT 
    COUNT(*) as total_posts, 
    COUNT(DISTINCT post_url) as unique_posts,
    COUNT(DISTINCT source_file) as unique_files,
    COUNT(*) - COUNT(DISTINCT post_url) as duplicate_posts,
    COUNT(*) - COUNT(DISTINCT source_file) as duplicate_files
FROM tiktok_post_metadata;
"@

Write-Host "   Analyzing..." -ForegroundColor DarkGray
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse `
    -e $query 2>$null | Select-String -Pattern "^\d" | ForEach-Object {
        $parts = $_ -split '\s+'
        Write-Host "   Total posts:        $($parts[0])" -ForegroundColor White
        Write-Host "   Unique post URLs:   $($parts[1])" -ForegroundColor White
        Write-Host "   Unique files:       $($parts[2])" -ForegroundColor White
        
        if ($parts[3] -eq "0") {
            Write-Host "   ✅ No duplicate posts" -ForegroundColor Green
        } else {
            Write-Host "   ⚠️  Duplicate posts:   $($parts[3])" -ForegroundColor Yellow
            Write-Host "      (Note: Same video crawled multiple times is normal)" -ForegroundColor DarkGray
        }
        
        if ($parts[4] -eq "0") {
            Write-Host "   ✅ No duplicate files" -ForegroundColor Green
        } else {
            Write-Host "   ❌ Duplicate files:    $($parts[4])" -ForegroundColor Red
        }
    }

# TikTok Comments
Write-Host "`n📊 TIKTOK POST COMMENTS" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray

$query = @"
USE silver;
SELECT 
    COUNT(*) as total_comments,
    COUNT(DISTINCT CONCAT(source_file_checksum, '|', post_url, '|', CAST(stt AS STRING))) as unique_comments
FROM tiktok_post_comments;
"@

Write-Host "   Analyzing..." -ForegroundColor DarkGray
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse `
    -e $query 2>$null | Select-String -Pattern "^\d" | ForEach-Object {
        $parts = $_ -split '\s+'
        $total = [int]$parts[0]
        $unique = [int]$parts[1]
        $dups = $total - $unique
        
        Write-Host "   Total comments:     $total" -ForegroundColor White
        Write-Host "   Unique comments:    $unique" -ForegroundColor White
        
        if ($dups -eq 0) {
            Write-Host "   ✅ No duplicate comments" -ForegroundColor Green
        } else {
            Write-Host "   ⚠️  Duplicate comments: $dups" -ForegroundColor Yellow
            $dupPercent = [math]::Round(($dups / $total) * 100, 2)
            Write-Host "      ($dupPercent% - likely from raw data duplicates)" -ForegroundColor DarkGray
        }
    }

# Hotels
Write-Host "`n📊 HOTELS DETAIL" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------" -ForegroundColor Gray

$query = @"
USE silver;
SELECT 
    COUNT(*) as total_hotels,
    COUNT(DISTINCT hotel_url) as unique_hotels,
    COUNT(DISTINCT source_file) as unique_files
FROM hotels_detail;
"@

Write-Host "   Analyzing..." -ForegroundColor DarkGray
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse `
    -e $query 2>$null | Select-String -Pattern "^\d" | ForEach-Object {
        $parts = $_ -split '\s+'
        Write-Host "   Total hotels:       $($parts[0])" -ForegroundColor White
        Write-Host "   Unique URLs:        $($parts[1])" -ForegroundColor White
        Write-Host "   Unique files:       $($parts[2])" -ForegroundColor White
        
        if ($parts[0] -eq $parts[1] -and $parts[0] -eq $parts[2]) {
            Write-Host "   ✅ No duplicates" -ForegroundColor Green
        } else {
            Write-Host "   ⚠️  Duplicates detected" -ForegroundColor Yellow
        }
    }

Write-Host "`n============================================================" -ForegroundColor Gray
Write-Host "✅ Duplicate check completed" -ForegroundColor Green
