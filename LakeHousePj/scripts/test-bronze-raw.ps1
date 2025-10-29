# ============================================================================
# Test Bronze RAW Layer Ingestion
# Purpose: Test all 5 Bronze RAW jobs to copy CSV files to MinIO
# ============================================================================

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "  TESTING BRONZE RAW LAYER INGESTION" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host ""

# Check Docker containers
Write-Host "Checking Docker containers..." -ForegroundColor Yellow
$containers = docker ps --format "{{.Names}}" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker is not running" -ForegroundColor Red
    exit 1
}

$required = @("lakehouse_spark_master", "lakehouse_postgres", "lakehouse_minio")
foreach ($container in $required) {
    if ($containers -notcontains $container) {
        Write-Host "ERROR: Container '$container' not running" -ForegroundColor Red
        exit 1
    }
}
Write-Host "[OK] All containers running" -ForegroundColor Green
Write-Host ""

# Test results array
$results = @()

# Function to test Bronze RAW job
function Test-BronzeRawJob {
    param(
        [string]$Name,
        [string]$Script,
        [string]$Source,
        [string]$Bucket,
        [string]$Type
    )
    
    Write-Host "Testing: $Name" -ForegroundColor Yellow
    Write-Host "  Script: $Script" -ForegroundColor Gray
    Write-Host "  Source: $Source" -ForegroundColor Gray
    Write-Host "  Target: s3a://$Bucket/lakehouse/$Type/raw/" -ForegroundColor Gray
    Write-Host "  Status: " -NoNewline
    
    $start = Get-Date
    
    try {
        $output = docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
            --master local[*] `
            --deploy-mode client `
            --jars /opt/spark/jars/postgresql-42.7.2.jar `
            --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar `
            --conf spark.driver.memory=2g `
            --conf spark.executor.memory=2g `
            /opt/spark/jobs/bronze/$Script `
            $Source `
            $Bucket `
            $Type 2>&1
        
        $duration = ((Get-Date) - $start).TotalSeconds
        
        # Check output
        $records = "N/A"
        $checksum = "N/A"
        
        # Check for success patterns
        if ($output -like "*BRONZE INGESTION COMPLETED*" -or 
            $output -like "*COMPLETED:*records ingested*" -or
            $output -like "*Logged ingestion (bronze)*") {
            Write-Host "[SUCCESS]" -ForegroundColor Green
            $status = "SUCCESS"
            
            # Extract info
            foreach ($line in $output) {
                if ($line -like "*Records:*" -or $line -like "*records ingested*") {
                    if ($line -match '\d+') {
                        $records = $matches[0]
                    }
                }
                if ($line -like "*Checksum:*") {
                    if ($line -match '[a-f0-9]{8,}') {
                        $checksum = $matches[0].Substring(0, 8)
                    }
                }
            }
        }
        elseif ($output -like "*already exists*" -or $output -like "*SKIPPED*") {
            Write-Host "[SKIPPED]" -ForegroundColor Yellow
            $status = "SKIPPED"
        }
        else {
            Write-Host "[FAILED]" -ForegroundColor Red
            $status = "FAILED"
            Write-Host "  Error:" -ForegroundColor Red
            $output | Select-Object -Last 5 | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
        }
        
        return @{
            Name = $Name
            Status = $status
            Records = $records
            Checksum = $checksum
            Duration = [math]::Round($duration, 2)
        }
    }
    catch {
        Write-Host "[ERROR]" -ForegroundColor Red
        Write-Host "  $_" -ForegroundColor Red
        return @{
            Name = $Name
            Status = "ERROR"
            Records = "0"
            Checksum = "N/A"
            Duration = 0
        }
    }
}

# ============================================================================
# Test 1: Booking Hotels List
# ============================================================================
Write-Host ""
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "[1/5] Booking Hotels List" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
$results += Test-BronzeRawJob `
    -Name "Booking Hotels List" `
    -Script "raw_ingest_booking_hotels_list.py" `
    -Source "/data/raw/booking/vietnam_hotels_list.csv" `
    -Bucket "bronze" `
    -Type "booking_hotels_list"

Start-Sleep -Seconds 1

# ============================================================================
# Test 2: Booking Hotels Detail
# ============================================================================
Write-Host ""
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "[2/5] Booking Hotels Detail" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
$results += Test-BronzeRawJob `
    -Name "Booking Hotels Detail" `
    -Script "raw_ingest_booking_hotels_detail.py" `
    -Source "/data/raw/booking/vietnam_hotels_detail.csv" `
    -Bucket "bronze" `
    -Type "booking_hotels_detail"

Start-Sleep -Seconds 1

# ============================================================================
# Test 3: Booking Hotels Reviews
# ============================================================================
Write-Host ""
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "[3/5] Booking Hotels Reviews" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
$results += Test-BronzeRawJob `
    -Name "Booking Hotels Reviews" `
    -Script "raw_ingest_booking_hotels_reviews.py" `
    -Source "/data/raw/booking/vietnam_hotels_reviews.csv" `
    -Bucket "bronze" `
    -Type "booking_hotels_reviews"

Start-Sleep -Seconds 1

# ============================================================================
# Test 4: TikTok Videos
# ============================================================================
Write-Host ""
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "[4/5] TikTok Videos" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
$results += Test-BronzeRawJob `
    -Name "TikTok Videos" `
    -Script "raw_ingest_tiktok_videos.py" `
    -Source "/data/raw/tiktok/links/merged_videos.csv" `
    -Bucket "bronze" `
    -Type "tiktok_videos"

Start-Sleep -Seconds 1

# ============================================================================
# Test 5: TikTok Comments (BATCH - All files)
# ============================================================================
Write-Host ""
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "[5/5] TikTok Comments (Batch Mode)" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan

# Count comment files
$commentCount = docker exec lakehouse_spark_master bash -c "ls /data/raw/tiktok/comments/*.csv 2>/dev/null | wc -l" 2>&1

if ($commentCount -and $commentCount -match '^\d+$' -and [int]$commentCount -gt 0) {
    Write-Host "Found $commentCount comment file(s)" -ForegroundColor Yellow
    Write-Host "Running BATCH ingestion job..." -ForegroundColor Yellow
    Write-Host "  Script: raw_ingest_tiktok_comments_batch.py" -ForegroundColor Gray
    Write-Host "  Source: /data/raw/tiktok/comments/" -ForegroundColor Gray
    Write-Host "  Target: s3a://bronze/lakehouse/tiktok_comments/raw/" -ForegroundColor Gray
    Write-Host "  Status: " -NoNewline
    
    $start = Get-Date
    
    try {
        $output = docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
            --master local[*] `
            --deploy-mode client `
            --jars /opt/spark/jars/postgresql-42.7.2.jar `
            --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar `
            --conf spark.driver.memory=2g `
            --conf spark.executor.memory=2g `
            /opt/spark/jobs/bronze/raw_ingest_tiktok_comments_batch.py `
            /data/raw/tiktok/comments `
            bronze `
            tiktok_comments 2>&1
        
        $duration = ((Get-Date) - $start).TotalSeconds
        
        # Check for success
        if ($output -like "*BATCH INGESTION COMPLETED*" -or $output -like "*SUCCESS*") {
            Write-Host "[SUCCESS]" -ForegroundColor Green
            
            # Extract counts
            $successCount = 0
            $skippedCount = 0
            foreach ($line in $output) {
                if ($line -like "*Success:*") {
                    if ($line -match '\d+') { $successCount = $matches[0] }
                }
                if ($line -like "*Skipped:*") {
                    if ($line -match '\d+') { $skippedCount = $matches[0] }
                }
            }
            
            $results += @{
                Name = "TikTok Comments (Batch)"
                Status = "SUCCESS"
                Records = "$successCount new, $skippedCount skipped"
                Checksum = "Multiple"
                Duration = [math]::Round($duration, 2)
            }
        }
        elseif ($output -like "*No new files*") {
            Write-Host "[SKIPPED]" -ForegroundColor Yellow
            $results += @{
                Name = "TikTok Comments (Batch)"
                Status = "SKIPPED"
                Records = "All already ingested"
                Checksum = "N/A"
                Duration = [math]::Round($duration, 2)
            }
        }
        else {
            Write-Host "[FAILED]" -ForegroundColor Red
            Write-Host "  Error:" -ForegroundColor Red
            $output | Select-Object -Last 5 | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
            $results += @{
                Name = "TikTok Comments (Batch)"
                Status = "FAILED"
                Records = "0"
                Checksum = "N/A"
                Duration = [math]::Round($duration, 2)
            }
        }
    }
    catch {
        Write-Host "[ERROR]" -ForegroundColor Red
        Write-Host "  $_" -ForegroundColor Red
        $results += @{
            Name = "TikTok Comments (Batch)"
            Status = "ERROR"
            Records = "0"
            Checksum = "N/A"
            Duration = 0
        }
    }
} else {
    Write-Host "No TikTok comments files found - SKIPPED" -ForegroundColor Yellow
    $results += @{
        Name = "TikTok Comments (Batch)"
        Status = "SKIPPED"
        Records = "N/A"
        Checksum = "N/A"
        Duration = 0
    }
}

# ============================================================================
# SUMMARY
# ============================================================================
Write-Host ""
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "  TEST SUMMARY" -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host ""

# Create table
$table = $results | ForEach-Object {
    [PSCustomObject]@{
        Job = $_.Name
        Status = $_.Status
        Records = $_.Records
        Checksum = $_.Checksum
        Duration = "$($_.Duration)s"
    }
}

$table | Format-Table -AutoSize

# Count results
$success = ($results | Where-Object { $_.Status -eq "SUCCESS" }).Count
$skipped = ($results | Where-Object { $_.Status -eq "SKIPPED" }).Count
$failed = ($results | Where-Object { $_.Status -eq "FAILED" -or $_.Status -eq "ERROR" }).Count

Write-Host "Results Summary:" -ForegroundColor Cyan
Write-Host "  Success: $success" -ForegroundColor Green
Write-Host "  Skipped: $skipped" -ForegroundColor Yellow
Write-Host "  Failed:  $failed" -ForegroundColor Red
Write-Host ""

if ($failed -eq 0) {
    Write-Host "==================================================================" -ForegroundColor Green
    Write-Host "  ALL TESTS PASSED!" -ForegroundColor Green
    Write-Host "==================================================================" -ForegroundColor Green
    Write-Host ""
    exit 0
} else {
    Write-Host "==================================================================" -ForegroundColor Red
    Write-Host "  SOME TESTS FAILED!" -ForegroundColor Red
    Write-Host "==================================================================" -ForegroundColor Red
    Write-Host ""
    exit 1
}
