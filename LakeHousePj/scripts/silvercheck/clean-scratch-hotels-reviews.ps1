# ===================================================================
# Clean Scratch Folder - hotels_reviews
# ===================================================================
# Purpose: Remove old/failed scratch run folders before re-running pipeline
# Usage: ./clean-scratch-hotels-reviews.ps1
# ===================================================================

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "🧹 CLEAN SCRATCH - hotels_reviews" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Create Python script to list and delete scratch folders
$pythonScript = @"
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName('CleanScratch-hotels_reviews') \
    .getOrCreate()

sc = spark.sparkContext
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

# Base path
base_path = sc._jvm.org.apache.hadoop.fs.Path('s3a://scratch/pipeline/silver/hotels_reviews')

print('📁 Checking scratch base path...')
if not fs.exists(base_path):
    print('ℹ️  Base path does not exist yet. Nothing to clean.')
    spark.stop()
    sys.exit(0)

# List all run_ folders
file_status_list = fs.listStatus(base_path)
run_folders = [status.getPath() for status in file_status_list if status.getPath().getName().startswith('run_')]

if not run_folders:
    print('ℹ️  No run folders found. Nothing to clean.')
    spark.stop()
    sys.exit(0)

print('')
print(f'🗂️  Found {len(run_folders)} run folder(s):')
for path in run_folders:
    print(f'   - {path.getName()}')

# Delete all run folders
print('')
print('🗑️  Deleting folders...')
for path in run_folders:
    try:
        fs.delete(path, True)  # True = recursive
        print(f'   ✅ Deleted: {path.getName()}')
    except Exception as e:
        print(f'   ❌ Failed to delete {path.getName()}: {e}')

print('')
print('✅ Cleanup complete!')
spark.stop()
"@

# Save Python script to temp file
$tempFile = [System.IO.Path]::GetTempFileName() + ".py"
$pythonScript | Out-File -FilePath $tempFile -Encoding UTF8

Write-Host "📋 Copying cleanup script to Spark container..." -ForegroundColor Yellow
docker cp $tempFile lakehouse_spark_master:/tmp/clean_scratch.py

Write-Host "🚀 Running cleanup script..." -ForegroundColor Yellow
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master local[1] `
    --deploy-mode client `
    /tmp/clean_scratch.py

# Cleanup temp file
Remove-Item $tempFile -ErrorAction SilentlyContinue
docker exec lakehouse_spark_master rm -f /tmp/clean_scratch.py

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "✅ CLEANUP FINISHED" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
