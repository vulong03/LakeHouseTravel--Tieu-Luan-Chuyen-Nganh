#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Quick record count for all Silver tables
#>

Write-Host "📊 SILVER LAYER RECORD COUNT" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Gray

$tables = @(
    "hotels_reviews",
    "hotels_detail", 
    "tiktok_post_metadata",
    "tiktok_post_comments"
)

foreach ($table in $tables) {
    Write-Host "`n🔹 $table" -ForegroundColor Yellow
    $query = "USE silver; SELECT COUNT(*) FROM $table;"
    $count = docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
        --conf spark.sql.catalog.lakehouse.type=hive `
        --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
        --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse `
        -e $query 2>$null | Select-String -Pattern "^\d+"
    
    if ($count) {
        Write-Host "   Records: $count" -ForegroundColor Green
    } else {
        Write-Host "   ⚠️ Error querying table" -ForegroundColor Red
    }
}

Write-Host "`n============================================================" -ForegroundColor Gray
