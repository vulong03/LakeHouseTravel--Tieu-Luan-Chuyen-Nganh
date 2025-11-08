# Generate schema documentation for all Silver tables

$tables = @("hotels_list", "hotels_detail", "hotels_reviews", "tiktok_videos", "tiktok_post_metadata", "tiktok_post_comments")
$outputFile = ".\docs\SILVER_TABLES_SCHEMA.md"

"# SILVER LAYER - TABLE SCHEMAS`n" | Out-File -FilePath $outputFile -Encoding UTF8
"Generated: $(Get-Date -Format 'dd/MM/yyyy HH:mm')`n" | Out-File -FilePath $outputFile -Append -Encoding UTF8
"---`n" | Out-File -FilePath $outputFile -Append -Encoding UTF8

foreach ($table in $tables) {
    Write-Host "Processing: $table" -ForegroundColor Yellow
    
    "## Table: $table`n" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "| Column | Type | Description |" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    "|--------|------|-------------|" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    
    $schema = docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
        --conf spark.sql.catalog.lakehouse.type=hive `
        --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
        --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse `
        -e "USE silver; DESCRIBE $table;" 2>&1 | 
        Where-Object { $_ -match "^\w+\s+\w+" } |
        Where-Object { $_ -notmatch "WARN|INFO|Setting|Spark|Time|Application|col_name|Partition|Metadata" }
    
    foreach ($line in $schema) {
        if ($line -match "^(\w+)\s+(\w+)") {
            $col = $matches[1]
            $type = $matches[2]
            "| $col | $type |  |" | Out-File -FilePath $outputFile -Append -Encoding UTF8
        }
    }
    
    "`n---`n" | Out-File -FilePath $outputFile -Append -Encoding UTF8
}

Write-Host "`nDone! File saved to: $outputFile" -ForegroundColor Green
