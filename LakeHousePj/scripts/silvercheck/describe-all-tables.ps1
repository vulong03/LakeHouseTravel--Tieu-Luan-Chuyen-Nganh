# ==============================================================================
# Script: describe-all-tables.ps1
# Description: Generate detailed schema documentation for all Silver tables
# ==============================================================================

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  SILVER LAYER SCHEMA DOCUMENTATION   " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$tables = @(
    "hotels_list",
    "hotels_detail",
    "hotels_reviews",
    "tiktok_videos",
    "tiktok_post_metadata",
    "tiktok_post_comments"
)

# Function to run Spark SQL query
function Get-TableSchema {
    param (
        [string]$TableName
    )
    
    $result = docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
        --conf spark.sql.catalog.lakehouse.type=hive `
        --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
        --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse `
        -e "USE silver; DESCRIBE $TableName;" 2>&1 | 
        Where-Object { $_ -match "^\w+\s+\w+" -and $_ -notmatch "WARN|INFO|Setting|Spark|Time taken|Application" }
    
    return $result
}

# Output file
$outputFile = "D:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\LakeHousePj\docs\SILVER_TABLES_SCHEMA.md"

# Start markdown file
@"
# SILVER LAYER - TABLE SCHEMAS

**Generated:** $(Get-Date -Format "dd/MM/yyyy HH:mm:ss")  
**Total Tables:** 6  
**Database:** silver  
**Format:** Apache Iceberg + Parquet

---

"@ | Out-File -FilePath $outputFile -Encoding UTF8

foreach ($table in $tables) {
    Write-Host "Processing table: $table..." -ForegroundColor Yellow
    
    $schema = Get-TableSchema -TableName $table
    
    # Add table header to markdown
    @"
## Table: ``$table``

| Column | Type | Description |
|--------|------|-------------|
"@ | Out-File -FilePath $outputFile -Append -Encoding UTF8
    
    # Parse schema and add to markdown
    $lineCount = 0
    foreach ($line in $schema) {
        if ($line -match "^(\w+)\s+(\w+)") {
            $columnName = $matches[1]
            $dataType = $matches[2]
            
            # Skip partition info and metadata columns
            if ($columnName -eq "col_name" -or $columnName -match "^_" -or $line -match "^#") {
                continue
            }
            
            # Determine if nullable (all columns in Silver are nullable except keys)
            $nullable = "Yes"
            if ($columnName -match "url|province|region" -and $columnName -notmatch "hotel_url|post_url") {
                $nullable = "No (Partition key)"
            }
            
            # Add description based on column name
            $description = switch -Regex ($columnName) {
                "^stt$" { "So thu tu" }
                "hotel_name" { "Ten khach san" }
                "hotel_url" { "URL khach san (Primary Key)" }
                "province" { "Tinh/thanh pho (Partition Key)" }
                "description" { "Mo ta chi tiet" }
                "top_amenities" { "Tien nghi noi bat" }
                "rating_score" { "Diem danh gia" }
                "review_count_text" { "So luong review (text)" }
                "rating_breakdown" { "Chi tiet rating theo tieu chi" }
                "activities" { "Hoat dong giai tri" }
                "reviewer_name" { "Ten nguoi danh gia" }
                "reviewer_country" { "Quoc gia nguoi danh gia" }
                "room_type" { "Loai phong" }
                "stay_date" { "Ngay luu tru" }
                "traveler_type" { "Loai du khach" }
                "review_date" { "Ngay viet review" }
                "review_title" { "Tieu de review" }
                "review_score" { "Diem danh gia (0-10)" }
                "review_positive" { "Phan danh gia tich cuc" }
                "review_negative" { "Phan danh gia tieu cuc" }
                "^url$" { "URL video TikTok (Primary Key)" }
                "posted_date" { "Ngay dang video" }
                "read_status" { "Trang thai doc" }
                "keyword" { "Tu khoa tim kiem" }
                "ques_id" { "ID cau hoi" }
                "target_type" { "Loai target" }
                "region" { "Vung mien (Partition Key)" }
                "has_sub" { "Co phu de khong" }
                "vi_sub" { "Phu de tieng Viet (100% NULL)" }
                "post_url" { "URL bai dang (Key)" }
                "author" { "Ten tac gia" }
                "author_tag" { "Tag/username tac gia" }
                "author_url" { "URL profile tac gia" }
                "post_date" { "Ngay dang bai" }
                "post_description" { "Mo ta bai dang" }
                "^likes$" { "So luot thich" }
                "comments_count" { "So luong comments" }
                "saves" { "So luot luu" }
                "shares" { "So luot share" }
                "comments_level1" { "So comments cap 1" }
                "comments_level2" { "So comments cap 2 (replies)" }
                "comments_loaded" { "So comments da load" }
                "comments_displayed_tiktok" { "So comments TikTok hien thi" }
                "comments_difference" { "Chenh lech so comments" }
                "crawl_time" { "Thoi gian crawl du lieu" }
                "ten" { "Ten nguoi comment" }
                "tag_ten" { "Tag username" }
                "comment" { "Noi dung comment" }
                "time" { "Thoi gian comment" }
                "level_comment" { "Cap do comment (No=L1, Yes=L2)" }
                "replied_to_tag_name" { "Tag nguoi duoc reply" }
                "number_of_replies" { "So luong replies" }
                "row_checksum" { "MD5 checksum for deduplication" }
                "ingestion_timestamp" { "Timestamp ingest to Silver" }
                "source_file" { "Ten file nguon tu Bronze" }
                "source_file_checksum" { "Checksum cua source file" }
                default { "" }
            }
            
            "| ``$columnName`` | $dataType | $description |" | Out-File -FilePath $outputFile -Append -Encoding UTF8
            $lineCount++
        }
    }
    
    # Add separator
    "`n---`n" | Out-File -FilePath $outputFile -Append -Encoding UTF8
    
    Write-Host "  ✓ Processed $lineCount columns" -ForegroundColor Green
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Schema documentation saved to:" -ForegroundColor Green
Write-Host $outputFile -ForegroundColor White
Write-Host "========================================" -ForegroundColor Cyan
