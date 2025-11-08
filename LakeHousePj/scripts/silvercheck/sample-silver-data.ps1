#!/usr/bin/env pwsh
<#
.SYNOPSIS
    View sample data from Silver layer tables
#>

param(
    [Parameter()]
    [ValidateSet("tiktok_posts", "tiktok_comments", "hotels_detail", "hotels_reviews", "all")]
    [string]$Table = "all",
    
    [Parameter()]
    [int]$Limit = 5
)

Write-Host "📋 SILVER LAYER SAMPLE DATA" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Gray

function Show-Sample {
    param(
        [string]$TableName,
        [string]$Query,
        [int]$Rows = 5
    )
    
    Write-Host "`n🔹 $TableName (Top $Rows)" -ForegroundColor Yellow
    Write-Host "------------------------------------------------------------" -ForegroundColor Gray
    
    docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
        --conf spark.sql.catalog.lakehouse.type=hive `
        --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
        --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse `
        -e "USE silver; $Query LIMIT $Rows;" 2>$null | Select-Object -Skip 2 | Where-Object { $_ -match '\S' }
}

if ($Table -eq "all" -or $Table -eq "tiktok_posts") {
    $query = "SELECT post_url, author, likes, comments_count, crawl_time FROM tiktok_post_metadata ORDER BY crawl_time DESC"
    Show-Sample -TableName "TIKTOK POST METADATA" -Query $query -Rows $Limit
}

if ($Table -eq "all" -or $Table -eq "tiktok_comments") {
    $query = "SELECT post_url, ten, comment, time, likes FROM tiktok_post_comments ORDER BY time DESC"
    Show-Sample -TableName "TIKTOK POST COMMENTS" -Query $query -Rows $Limit
}

if ($Table -eq "all" -or $Table -eq "hotels_detail") {
    $query = "SELECT hotel_name, hotel_url, province, rating_score, review_count_text FROM hotels_detail ORDER BY rating_score DESC"
    Show-Sample -TableName "HOTELS DETAIL" -Query $query -Rows $Limit
}

if ($Table -eq "all" -or $Table -eq "hotels_reviews") {
    $query = "SELECT hotel_name, reviewer_name, review_score, review_date, review_title FROM hotels_reviews ORDER BY review_date DESC"
    Show-Sample -TableName "HOTELS REVIEWS" -Query $query -Rows $Limit
}

Write-Host "`n============================================================" -ForegroundColor Gray
Write-Host "💡 Usage: .\sample-silver-data.ps1 -Table <table_name> -Limit <number>" -ForegroundColor DarkGray
Write-Host "   Tables: tiktok_posts, tiktok_comments, hotels_detail, hotels_reviews, all" -ForegroundColor DarkGray
