# Check Bronze Ingestion Status with Checksum Tracking
# Purpose: Display file tracking status and detect changes

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  BRONZE INGESTION STATUS CHECK" -ForegroundColor Cyan
Write-Host "  Using Checksum-based Tracking" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is running
docker ps 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker is not running" -ForegroundColor Red
    exit 1
}

# Check if postgres container is running
$postgresStatus = docker inspect -f '{{.State.Running}}' lakehouse_postgres 2>$null
if ($postgresStatus -ne "True") {
    Write-Host "ERROR: lakehouse_postgres container is not running" -ForegroundColor Red
    exit 1
}

Write-Host "Docker and PostgreSQL are running" -ForegroundColor Green
Write-Host ""

# ============================================
# Query ingestion log
# ============================================

Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  FILE INGESTION LOG" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

$query = @"
SELECT 
    file_name,
    table_name,
    records_ingested,
    status,
    TO_CHAR(ingestion_timestamp, 'YYYY-MM-DD HH24:MI:SS') as ingestion_time,
    SUBSTRING(file_checksum, 1, 12) as checksum_short,
    ROUND(file_size_bytes::numeric / 1024, 2) as size_kb
FROM file_ingestion_log
ORDER BY ingestion_timestamp DESC
LIMIT 20;
"@

Write-Host "Recent ingestions:" -ForegroundColor Cyan
docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "$query"

Write-Host ""

# ============================================
# Summary statistics
# ============================================

Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  INGESTION STATISTICS" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

$summaryQuery = @"
SELECT 
    table_name,
    COUNT(*) as file_count,
    SUM(records_ingested) as total_records,
    ROUND(SUM(file_size_bytes)::numeric / 1024 / 1024, 2) as total_size_mb,
    MAX(ingestion_timestamp) as last_ingestion
FROM file_ingestion_log
WHERE status = 'success'
GROUP BY table_name
ORDER BY table_name;
"@

Write-Host "Summary by table:" -ForegroundColor Cyan
docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "$summaryQuery"

Write-Host ""

# ============================================
# Check for duplicate checksums (should not happen)
# ============================================

Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  DUPLICATE CHECK" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

$duplicateQuery = @"
SELECT 
    file_checksum,
    COUNT(*) as occurrence_count,
    STRING_AGG(file_name, ', ') as file_names
FROM file_ingestion_log
GROUP BY file_checksum
HAVING COUNT(*) > 1;
"@

Write-Host "Checking for duplicate checksums (files ingested multiple times):" -ForegroundColor Cyan
$duplicates = docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -t -c "$duplicateQuery"

if ([string]::IsNullOrWhiteSpace($duplicates)) {
    Write-Host "No duplicate checksums found" -ForegroundColor Green
} else {
    Write-Host "WARNING: Found duplicate checksums:" -ForegroundColor Yellow
    docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "$duplicateQuery"
}

Write-Host ""

# ============================================
# Check file changes (compare disk vs database)
# ============================================

Write-Host "========================================" -ForegroundColor Yellow
Write-Host "  FILE CHANGE DETECTION" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host ""

Write-Host "Checking if files on disk have changed since last ingestion..." -ForegroundColor Cyan
Write-Host ""

# TikTok Videos
$videosFile = "d:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\LakeHousePj\data\raw\tiktok\links\merged_videos.csv"
if (Test-Path $videosFile) {
    $currentChecksum = (Get-FileHash -Path $videosFile -Algorithm MD5).Hash.ToLower()
    
    $dbQuery = @"
SELECT file_checksum 
FROM file_ingestion_log 
WHERE file_name = 'merged_videos.csv' 
AND status = 'success'
ORDER BY ingestion_timestamp DESC 
LIMIT 1;
"@
    
    $dbChecksum = docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -t -c "$dbQuery"
    $dbChecksum = $dbChecksum.Trim()
    
    Write-Host "File: merged_videos.csv" -ForegroundColor White
    Write-Host "   Current checksum: $currentChecksum" -ForegroundColor Gray
    Write-Host "   DB checksum:      $dbChecksum" -ForegroundColor Gray
    
    if ($currentChecksum -eq $dbChecksum) {
        Write-Host "   Status: No changes detected" -ForegroundColor Green
    } else {
        Write-Host "   Status: File has CHANGED since last ingestion" -ForegroundColor Yellow
    }
    Write-Host ""
}

# TikTok Comments
$commentsDir = "d:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\LakeHousePj\data\raw\tiktok\comments"
if (Test-Path $commentsDir) {
    $commentFiles = Get-ChildItem -Path $commentsDir -Filter "*.csv"
    $changedFiles = 0
    $unchangedFiles = 0
    $newFiles = 0
    
    Write-Host "TikTok Comments folder ($($commentFiles.Count) files):" -ForegroundColor White
    
    foreach ($file in $commentFiles) {
        $currentChecksum = (Get-FileHash -Path $file.FullName -Algorithm MD5).Hash.ToLower()
        
        $dbQuery = @"
SELECT file_checksum 
FROM file_ingestion_log 
WHERE file_name = '$($file.Name)' 
AND status = 'success'
ORDER BY ingestion_timestamp DESC 
LIMIT 1;
"@
        
        $dbChecksum = docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -t -c "$dbQuery"
        $dbChecksum = $dbChecksum.Trim()
        
        if ([string]::IsNullOrWhiteSpace($dbChecksum)) {
            $newFiles++
        } elseif ($currentChecksum -eq $dbChecksum) {
            $unchangedFiles++
        } else {
            $changedFiles++
        }
    }
    
    Write-Host "   Unchanged: $unchangedFiles files" -ForegroundColor Green
    if ($changedFiles -gt 0) {
        Write-Host "   Changed:   $changedFiles files" -ForegroundColor Yellow
    }
    if ($newFiles -gt 0) {
        Write-Host "   New:       $newFiles files" -ForegroundColor Cyan
    }
    Write-Host ""
}

# ============================================
# Recommendations
# ============================================

Write-Host "========================================" -ForegroundColor Green
Write-Host "  RECOMMENDATIONS" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

$totalChanged = $changedFiles + $newFiles

if ($totalChanged -gt 0) {
    Write-Host "Note: $totalChanged file(s) have changes or are new" -ForegroundColor Yellow
    Write-Host "   Run ingestion to update Bronze layer:" -ForegroundColor White
    Write-Host "   .\scripts\run-bronze-ingestion.ps1" -ForegroundColor Gray
} else {
    Write-Host "All files are up-to-date in Bronze layer" -ForegroundColor Green
    Write-Host "   No ingestion needed" -ForegroundColor White
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Check Complete!" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
