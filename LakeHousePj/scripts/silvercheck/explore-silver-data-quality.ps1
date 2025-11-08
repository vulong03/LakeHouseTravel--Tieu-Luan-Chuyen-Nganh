# ==============================================================================
# Script: explore-silver-data-quality.ps1
# Description: Comprehensive data quality analysis for Silver layer
# - DESCRIBE EXTENDED for all tables
# - NULL value analysis for each column
# - Duplicate detection
# - Data profiling
# ==============================================================================

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  SILVER LAYER DATA QUALITY EXPLORER  " -ForegroundColor Cyan
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
function Run-SparkSQL {
    param (
        [string]$Query
    )
    
    $result = docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
        --conf spark.sql.catalog.lakehouse.type=hive `
        --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
        --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse `
        -e "USE silver; $Query" 2>&1 | Where-Object { $_ -notmatch "WARN|INFO|Setting default" }
    
    return $result
}

# ==============================================================================
# PART 1: TABLE SCHEMAS
# ==============================================================================
Write-Host "PART 1: TABLE SCHEMAS AND METADATA" -ForegroundColor Yellow
Write-Host "====================================" -ForegroundColor Yellow
Write-Host ""

foreach ($table in $tables) {
    Write-Host "Table: $table" -ForegroundColor Green
    Write-Host ("-" * 80) -ForegroundColor Gray
    
    $schema = Run-SparkSQL "DESCRIBE EXTENDED $table;"
    
    # Extract columns only (before Partition Information)
    $inColumns = $true
    $columns = @()
    
    foreach ($line in $schema) {
        if ($line -match "^# Partition Information" -or $line -match "^# Metadata Columns") {
            $inColumns = $false
        }
        if ($inColumns -and $line -match "^\w+\s+\w+") {
            $columns += $line
            Write-Host "  $line" -ForegroundColor White
        }
    }
    
    Write-Host ""
}

# ==============================================================================
# PART 2: RECORD COUNTS
# ==============================================================================
Write-Host ""
Write-Host "PART 2: RECORD COUNTS" -ForegroundColor Yellow
Write-Host "=====================" -ForegroundColor Yellow
Write-Host ""

$countQuery = @"
SELECT 'hotels_list' as table_name, COUNT(*) as record_count FROM hotels_list
UNION ALL
SELECT 'hotels_detail', COUNT(*) FROM hotels_detail
UNION ALL
SELECT 'hotels_reviews', COUNT(*) FROM hotels_reviews
UNION ALL
SELECT 'tiktok_videos', COUNT(*) FROM tiktok_videos
UNION ALL
SELECT 'tiktok_post_metadata', COUNT(*) FROM tiktok_post_metadata
UNION ALL
SELECT 'tiktok_post_comments', COUNT(*) FROM tiktok_post_comments
ORDER BY record_count DESC;
"@

$counts = Run-SparkSQL $countQuery
$counts | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor Cyan
    }
}

# ==============================================================================
# PART 3: NULL VALUE ANALYSIS
# ==============================================================================
Write-Host ""
Write-Host "PART 3: NULL VALUE ANALYSIS" -ForegroundColor Yellow
Write-Host "============================" -ForegroundColor Yellow
Write-Host ""

# hotels_list NULL analysis
Write-Host "Table: hotels_list" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$nullQueryList = @"
SELECT 
    'stt' as column_name,
    COUNT(*) - COUNT(stt) as null_count,
    ROUND((COUNT(*) - COUNT(stt)) * 100.0 / COUNT(*), 2) as null_percentage
FROM hotels_list
UNION ALL
SELECT 'hotel_name', COUNT(*) - COUNT(hotel_name), ROUND((COUNT(*) - COUNT(hotel_name)) * 100.0 / COUNT(*), 2) FROM hotels_list
UNION ALL
SELECT 'hotel_url', COUNT(*) - COUNT(hotel_url), ROUND((COUNT(*) - COUNT(hotel_url)) * 100.0 / COUNT(*), 2) FROM hotels_list
UNION ALL
SELECT 'province', COUNT(*) - COUNT(province), ROUND((COUNT(*) - COUNT(province)) * 100.0 / COUNT(*), 2) FROM hotels_list
ORDER BY null_count DESC;
"@

$nullResultsList = Run-SparkSQL $nullQueryList
$nullResultsList | ForEach-Object {
    if ($_ -match "\S" -and $_ -notmatch "^column_name") {
        if ($_ -match "\s+0\s+0\.0") {
            Write-Host "  $_" -ForegroundColor Green
        } elseif ($_ -match "\s+\d+\s+([0-9.]+)" -and [double]$matches[1] -lt 5.0) {
            Write-Host "  $_" -ForegroundColor Yellow
        } else {
            Write-Host "  $_" -ForegroundColor Red
        }
    }
}

Write-Host ""

# hotels_detail NULL analysis
Write-Host "Table: hotels_detail" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$nullQuery = @"
SELECT 
    'hotel_name' as column_name,
    COUNT(*) - COUNT(hotel_name) as null_count,
    ROUND((COUNT(*) - COUNT(hotel_name)) * 100.0 / COUNT(*), 2) as null_percentage
FROM hotels_detail
UNION ALL
SELECT 'hotel_url', COUNT(*) - COUNT(hotel_url), ROUND((COUNT(*) - COUNT(hotel_url)) * 100.0 / COUNT(*), 2) FROM hotels_detail
UNION ALL
SELECT 'province', COUNT(*) - COUNT(province), ROUND((COUNT(*) - COUNT(province)) * 100.0 / COUNT(*), 2) FROM hotels_detail
UNION ALL
SELECT 'description', COUNT(*) - COUNT(description), ROUND((COUNT(*) - COUNT(description)) * 100.0 / COUNT(*), 2) FROM hotels_detail
UNION ALL
SELECT 'top_amenities', COUNT(*) - COUNT(top_amenities), ROUND((COUNT(*) - COUNT(top_amenities)) * 100.0 / COUNT(*), 2) FROM hotels_detail
UNION ALL
SELECT 'rating_score', COUNT(*) - COUNT(rating_score), ROUND((COUNT(*) - COUNT(rating_score)) * 100.0 / COUNT(*), 2) FROM hotels_detail
UNION ALL
SELECT 'review_count_text', COUNT(*) - COUNT(review_count_text), ROUND((COUNT(*) - COUNT(review_count_text)) * 100.0 / COUNT(*), 2) FROM hotels_detail
UNION ALL
SELECT 'rating_breakdown', COUNT(*) - COUNT(rating_breakdown), ROUND((COUNT(*) - COUNT(rating_breakdown)) * 100.0 / COUNT(*), 2) FROM hotels_detail
UNION ALL
SELECT 'activities', COUNT(*) - COUNT(activities), ROUND((COUNT(*) - COUNT(activities)) * 100.0 / COUNT(*), 2) FROM hotels_detail
ORDER BY null_count DESC;
"@

$nullResults = Run-SparkSQL $nullQuery
$nullResults | ForEach-Object {
    if ($_ -match "\S" -and $_ -notmatch "^column_name") {
        if ($_ -match "\s+0\s+0\.0") {
            Write-Host "  $_" -ForegroundColor Green
        } elseif ($_ -match "\s+\d+\s+([0-9.]+)" -and [double]$matches[1] -lt 5.0) {
            Write-Host "  $_" -ForegroundColor Yellow
        } else {
            Write-Host "  $_" -ForegroundColor Red
        }
    }
}

Write-Host ""

# hotels_reviews NULL analysis
Write-Host "Table: hotels_reviews" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$nullQuery2 = @"
SELECT 
    'hotel_name' as column_name,
    COUNT(*) - COUNT(hotel_name) as null_count,
    ROUND((COUNT(*) - COUNT(hotel_name)) * 100.0 / COUNT(*), 2) as null_percentage
FROM hotels_reviews
UNION ALL
SELECT 'reviewer_name', COUNT(*) - COUNT(reviewer_name), ROUND((COUNT(*) - COUNT(reviewer_name)) * 100.0 / COUNT(*), 2) FROM hotels_reviews
UNION ALL
SELECT 'reviewer_country', COUNT(*) - COUNT(reviewer_country), ROUND((COUNT(*) - COUNT(reviewer_country)) * 100.0 / COUNT(*), 2) FROM hotels_reviews
UNION ALL
SELECT 'room_type', COUNT(*) - COUNT(room_type), ROUND((COUNT(*) - COUNT(room_type)) * 100.0 / COUNT(*), 2) FROM hotels_reviews
UNION ALL
SELECT 'stay_date', COUNT(*) - COUNT(stay_date), ROUND((COUNT(*) - COUNT(stay_date)) * 100.0 / COUNT(*), 2) FROM hotels_reviews
UNION ALL
SELECT 'review_score', COUNT(*) - COUNT(review_score), ROUND((COUNT(*) - COUNT(review_score)) * 100.0 / COUNT(*), 2) FROM hotels_reviews
UNION ALL
SELECT 'review_positive', COUNT(*) - COUNT(review_positive), ROUND((COUNT(*) - COUNT(review_positive)) * 100.0 / COUNT(*), 2) FROM hotels_reviews
UNION ALL
SELECT 'review_negative', COUNT(*) - COUNT(review_negative), ROUND((COUNT(*) - COUNT(review_negative)) * 100.0 / COUNT(*), 2) FROM hotels_reviews
ORDER BY null_count DESC;
"@

$nullResults2 = Run-SparkSQL $nullQuery2
$nullResults2 | ForEach-Object {
    if ($_ -match "\S" -and $_ -notmatch "^column_name") {
        if ($_ -match "\s+0\s+0\.0") {
            Write-Host "  $_" -ForegroundColor Green
        } elseif ($_ -match "\s+\d+\s+([0-9.]+)" -and [double]$matches[1] -lt 5.0) {
            Write-Host "  $_" -ForegroundColor Yellow
        } else {
            Write-Host "  $_" -ForegroundColor Red
        }
    }
}

Write-Host ""

# tiktok_videos NULL analysis
Write-Host "Table: tiktok_videos" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$nullQueryVideos = @"
SELECT 
    'url' as column_name,
    COUNT(*) - COUNT(url) as null_count,
    ROUND((COUNT(*) - COUNT(url)) * 100.0 / COUNT(*), 2) as null_percentage
FROM tiktok_videos
UNION ALL
SELECT 'posted_date', COUNT(*) - COUNT(posted_date), ROUND((COUNT(*) - COUNT(posted_date)) * 100.0 / COUNT(*), 2) FROM tiktok_videos
UNION ALL
SELECT 'keyword', COUNT(*) - COUNT(keyword), ROUND((COUNT(*) - COUNT(keyword)) * 100.0 / COUNT(*), 2) FROM tiktok_videos
UNION ALL
SELECT 'region', COUNT(*) - COUNT(region), ROUND((COUNT(*) - COUNT(region)) * 100.0 / COUNT(*), 2) FROM tiktok_videos
UNION ALL
SELECT 'has_sub', COUNT(*) - COUNT(has_sub), ROUND((COUNT(*) - COUNT(has_sub)) * 100.0 / COUNT(*), 2) FROM tiktok_videos
UNION ALL
SELECT 'vi_sub', COUNT(*) - COUNT(vi_sub), ROUND((COUNT(*) - COUNT(vi_sub)) * 100.0 / COUNT(*), 2) FROM tiktok_videos
ORDER BY null_count DESC;
"@

$nullResultsVideos = Run-SparkSQL $nullQueryVideos
$nullResultsVideos | ForEach-Object {
    if ($_ -match "\S" -and $_ -notmatch "^column_name") {
        if ($_ -match "\s+0\s+0\.0") {
            Write-Host "  $_" -ForegroundColor Green
        } elseif ($_ -match "\s+\d+\s+([0-9.]+)" -and [double]$matches[1] -lt 5.0) {
            Write-Host "  $_" -ForegroundColor Yellow
        } elseif ($_ -match "\s+\d+\s+100\.0") {
            Write-Host "  $_ (CRITICAL - 100% NULL!)" -ForegroundColor Magenta
        } else {
            Write-Host "  $_" -ForegroundColor Red
        }
    }
}

Write-Host ""

# tiktok_post_metadata NULL analysis
Write-Host "Table: tiktok_post_metadata" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$nullQuery3 = @"
SELECT 
    'post_url' as column_name,
    COUNT(*) - COUNT(post_url) as null_count,
    ROUND((COUNT(*) - COUNT(post_url)) * 100.0 / COUNT(*), 2) as null_percentage
FROM tiktok_post_metadata
UNION ALL
SELECT 'author', COUNT(*) - COUNT(author), ROUND((COUNT(*) - COUNT(author)) * 100.0 / COUNT(*), 2) FROM tiktok_post_metadata
UNION ALL
SELECT 'post_date', COUNT(*) - COUNT(post_date), ROUND((COUNT(*) - COUNT(post_date)) * 100.0 / COUNT(*), 2) FROM tiktok_post_metadata
UNION ALL
SELECT 'likes', COUNT(*) - COUNT(likes), ROUND((COUNT(*) - COUNT(likes)) * 100.0 / COUNT(*), 2) FROM tiktok_post_metadata
UNION ALL
SELECT 'comments_count', COUNT(*) - COUNT(comments_count), ROUND((COUNT(*) - COUNT(comments_count)) * 100.0 / COUNT(*), 2) FROM tiktok_post_metadata
UNION ALL
SELECT 'shares', COUNT(*) - COUNT(shares), ROUND((COUNT(*) - COUNT(shares)) * 100.0 / COUNT(*), 2) FROM tiktok_post_metadata
ORDER BY null_count DESC;
"@

$nullResults3 = Run-SparkSQL $nullQuery3
$nullResults3 | ForEach-Object {
    if ($_ -match "\S" -and $_ -notmatch "^column_name") {
        if ($_ -match "\s+0\s+0\.0") {
            Write-Host "  $_" -ForegroundColor Green
        } else {
            Write-Host "  $_" -ForegroundColor Red
        }
    }
}

Write-Host ""

# tiktok_post_comments NULL analysis
Write-Host "Table: tiktok_post_comments" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$nullQuery4 = @"
SELECT 
    'post_url' as column_name,
    COUNT(*) - COUNT(post_url) as null_count,
    ROUND((COUNT(*) - COUNT(post_url)) * 100.0 / COUNT(*), 2) as null_percentage
FROM tiktok_post_comments
UNION ALL
SELECT 'ten', COUNT(*) - COUNT(ten), ROUND((COUNT(*) - COUNT(ten)) * 100.0 / COUNT(*), 2) FROM tiktok_post_comments
UNION ALL
SELECT 'comment', COUNT(*) - COUNT(comment), ROUND((COUNT(*) - COUNT(comment)) * 100.0 / COUNT(*), 2) FROM tiktok_post_comments
UNION ALL
SELECT 'time', COUNT(*) - COUNT(time), ROUND((COUNT(*) - COUNT(time)) * 100.0 / COUNT(*), 2) FROM tiktok_post_comments
UNION ALL
SELECT 'likes', COUNT(*) - COUNT(likes), ROUND((COUNT(*) - COUNT(likes)) * 100.0 / COUNT(*), 2) FROM tiktok_post_comments
ORDER BY null_count DESC;
"@

$nullResults4 = Run-SparkSQL $nullQuery4
$nullResults4 | ForEach-Object {
    if ($_ -match "\S" -and $_ -notmatch "^column_name") {
        if ($_ -match "\s+0\s+0\.0") {
            Write-Host "  $_" -ForegroundColor Green
        } else {
            Write-Host "  $_" -ForegroundColor Red
        }
    }
}

# ==============================================================================
# PART 4: DUPLICATE ANALYSIS
# ==============================================================================
Write-Host ""
Write-Host "PART 4: DUPLICATE ANALYSIS" -ForegroundColor Yellow
Write-Host "===========================" -ForegroundColor Yellow
Write-Host ""

# hotels_list duplicates (by hotel_url)
Write-Host "Table: hotels_list - Duplicates by hotel_url" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$dupQueryList = @"
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT hotel_url) as unique_hotels,
    COUNT(*) - COUNT(DISTINCT hotel_url) as duplicates
FROM hotels_list;
"@

$dupResultsList = Run-SparkSQL $dupQueryList
$dupResultsList | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor Cyan
    }
}

Write-Host ""

# hotels_detail duplicates (by hotel_url)
Write-Host "Table: hotels_detail - Duplicates by hotel_url" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$dupQuery1 = @"
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT hotel_url) as unique_hotels,
    COUNT(*) - COUNT(DISTINCT hotel_url) as duplicates
FROM hotels_detail;
"@

$dupResults1 = Run-SparkSQL $dupQuery1
$dupResults1 | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor Cyan
    }
}

Write-Host ""

# hotels_reviews duplicates (by row_checksum)
Write-Host "Table: hotels_reviews - Duplicates by row_checksum" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$dupQuery2 = @"
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT row_checksum) as unique_reviews,
    COUNT(*) - COUNT(DISTINCT row_checksum) as duplicates
FROM hotels_reviews;
"@

$dupResults2 = Run-SparkSQL $dupQuery2
$dupResults2 | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor Cyan
    }
}

Write-Host ""

# tiktok_videos duplicates
Write-Host "Table: tiktok_videos - Duplicates by URL" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$dupQueryVideos = @"
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT url) as unique_urls,
    COUNT(*) - COUNT(DISTINCT url) as duplicates
FROM tiktok_videos;
"@

$dupResultsVideos = Run-SparkSQL $dupQueryVideos
$dupResultsVideos | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor Cyan
    }
}

Write-Host ""

# tiktok_post_metadata duplicates
Write-Host "Table: tiktok_post_metadata - Duplicates by post_url vs files" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$dupQuery3 = @"
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT post_url) as unique_urls,
    COUNT(*) - COUNT(DISTINCT post_url) as url_duplicates,
    COUNT(DISTINCT source_file) as unique_files,
    COUNT(*) - COUNT(DISTINCT source_file) as file_duplicates
FROM tiktok_post_metadata;
"@

$dupResults3 = Run-SparkSQL $dupQuery3
$dupResults3 | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor Cyan
    }
}

Write-Host ""

# tiktok_post_comments duplicates
Write-Host "Table: tiktok_post_comments - Duplicates analysis" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$dupQuery4 = @"
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT CONCAT(source_file_checksum, '|', post_url, '|', stt)) as unique_comments,
    COUNT(*) - COUNT(DISTINCT CONCAT(source_file_checksum, '|', post_url, '|', stt)) as duplicates,
    ROUND((COUNT(*) - COUNT(DISTINCT CONCAT(source_file_checksum, '|', post_url, '|', stt))) * 100.0 / COUNT(*), 2) as duplicate_percentage
FROM tiktok_post_comments;
"@

$dupResults4 = Run-SparkSQL $dupQuery4
$dupResults4 | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor Cyan
    }
}

# ==============================================================================
# PART 5: DATA PROFILING - SAMPLE VALUES
# ==============================================================================
Write-Host ""
Write-Host "PART 5: DATA PROFILING - SAMPLE VALUES" -ForegroundColor Yellow
Write-Host "=======================================" -ForegroundColor Yellow
Write-Host ""

# Top provinces
Write-Host "Top 10 provinces (hotels_detail)" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$profileQuery1 = @"
SELECT province, COUNT(*) as hotel_count
FROM hotels_detail
WHERE province IS NOT NULL
GROUP BY province
ORDER BY hotel_count DESC
LIMIT 10;
"@

$profile1 = Run-SparkSQL $profileQuery1
$profile1 | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor White
    }
}

Write-Host ""

# Top reviewers countries
Write-Host "Top 10 reviewer countries (hotels_reviews)" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$profileQuery2 = @"
SELECT reviewer_country, COUNT(*) as review_count
FROM hotels_reviews
WHERE reviewer_country IS NOT NULL
GROUP BY reviewer_country
ORDER BY review_count DESC
LIMIT 10;
"@

$profile2 = Run-SparkSQL $profileQuery2
$profile2 | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor White
    }
}

Write-Host ""

# Top TikTok authors
Write-Host "Top 10 TikTok authors by posts (tiktok_post_metadata)" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$profileQuery3 = @"
SELECT author, COUNT(*) as post_count
FROM tiktok_post_metadata
WHERE author IS NOT NULL
GROUP BY author
ORDER BY post_count DESC
LIMIT 10;
"@

$profile3 = Run-SparkSQL $profileQuery3
$profile3 | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor White
    }
}

Write-Host ""

# Top regions for TikTok videos
Write-Host "Top regions (tiktok_videos)" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$profileQueryVideos = @"
SELECT region, COUNT(*) as video_count
FROM tiktok_videos
WHERE region IS NOT NULL
GROUP BY region
ORDER BY video_count DESC
LIMIT 10;
"@

$profileVideos = Run-SparkSQL $profileQueryVideos
$profileVideos | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor White
    }
}

Write-Host ""

# Comment level distribution
Write-Host "Comment level distribution (tiktok_post_comments)" -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$profileQuery4 = @"
SELECT level_comment, COUNT(*) as comment_count
FROM tiktok_post_comments
WHERE level_comment IS NOT NULL
GROUP BY level_comment
ORDER BY level_comment;
"@

$profile4 = Run-SparkSQL $profileQuery4
$profile4 | ForEach-Object {
    if ($_ -match "\S") {
        Write-Host "  $_" -ForegroundColor White
    }
}

# ==============================================================================
# PART 6: EMPTY STRING ANALYSIS
# ==============================================================================
Write-Host ""
Write-Host "PART 6: EMPTY STRING ANALYSIS" -ForegroundColor Yellow
Write-Host "==============================" -ForegroundColor Yellow
Write-Host ""

Write-Host "Checking for empty strings in critical columns..." -ForegroundColor Green
Write-Host ("-" * 80) -ForegroundColor Gray

$emptyQuery = @"
SELECT 
    'hotels_list.hotel_name' as column_name,
    COUNT(*) as empty_count
FROM hotels_list
WHERE hotel_name = ''
UNION ALL
SELECT 'hotels_detail.hotel_name', COUNT(*) FROM hotels_detail WHERE hotel_name = ''
UNION ALL
SELECT 'hotels_reviews.review_positive', COUNT(*) FROM hotels_reviews WHERE review_positive = ''
UNION ALL
SELECT 'tiktok_videos.url', COUNT(*) FROM tiktok_videos WHERE url = ''
UNION ALL
SELECT 'tiktok_post_metadata.post_description', COUNT(*) FROM tiktok_post_metadata WHERE post_description = ''
UNION ALL
SELECT 'tiktok_post_comments.comment', COUNT(*) FROM tiktok_post_comments WHERE comment = ''
ORDER BY empty_count DESC;
"@

$emptyResults = Run-SparkSQL $emptyQuery
$emptyResults | ForEach-Object {
    if ($_ -match "\S") {
        if ($_ -match "\s+0$") {
            Write-Host "  $_ (OK)" -ForegroundColor Green
        } else {
            Write-Host "  $_ (WARNING!)" -ForegroundColor Red
        }
    }
}

# ==============================================================================
# SUMMARY
# ==============================================================================
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  ANALYSIS COMPLETE!                  " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Legend:" -ForegroundColor Yellow
Write-Host "  Green  = Good quality (0% NULL or no duplicates)" -ForegroundColor Green
Write-Host "  Yellow = Acceptable (<5% NULL)" -ForegroundColor Yellow
Write-Host "  Red    = Needs attention (>5% NULL or has issues)" -ForegroundColor Red
Write-Host ""
