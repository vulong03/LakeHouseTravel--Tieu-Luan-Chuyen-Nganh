# MinIO Data Structure Explorer
# Simple version without unicode characters

Write-Host "========================================"
Write-Host "  MINIO DATA STRUCTURE EXPLORER"
Write-Host "========================================"
Write-Host ""

# 1. Bronze bucket overview
Write-Host "1. Bronze Bucket Overview" -ForegroundColor Green
docker exec lakehouse_minio ls -lh /data/bronze/

# 2. Videos Metadata
Write-Host "`n2. TikTok Videos Metadata" -ForegroundColor Green
Write-Host "   Location: /data/bronze/tiktok_videos_metadata/"

Write-Host "`n   Partitions (by region):" -ForegroundColor Cyan
docker exec lakehouse_minio ls /data/bronze/tiktok_videos_metadata/data/ 2>$null

Write-Host "`n   Sample partition (Red_River_Delta):" -ForegroundColor Cyan
docker exec lakehouse_minio sh -c "ls -lh /data/bronze/tiktok_videos_metadata/data/region=Red_River_Delta/ 2>/dev/null | head -5"

# 3. Posts Raw
Write-Host "`n3. TikTok Posts Raw" -ForegroundColor Green
Write-Host "   Location: /data/bronze/tiktok_posts_raw/"

$postsCount = (docker exec lakehouse_minio sh -c "ls -1 /data/bronze/tiktok_posts_raw/data/*.parquet 2>/dev/null | wc -l" 2>$null)
Write-Host "   Total parquet files: $postsCount" -ForegroundColor Yellow

Write-Host "`n   Sample files:" -ForegroundColor Cyan
docker exec lakehouse_minio sh -c "ls -lh /data/bronze/tiktok_posts_raw/data/*.parquet 2>/dev/null | head -5"

# 4. Comments Raw
Write-Host "`n4. TikTok Comments Raw" -ForegroundColor Green
Write-Host "   Location: /data/bronze/tiktok_comments_raw/"

Write-Host "`n   Partitions:" -ForegroundColor Cyan
docker exec lakehouse_minio ls /data/bronze/tiktok_comments_raw/data/ 2>$null

$commentsCount = (docker exec lakehouse_minio sh -c "ls -1 /data/bronze/tiktok_comments_raw/data/post_url=/*.parquet 2>/dev/null | wc -l" 2>$null)
Write-Host "   Total parquet files: $commentsCount" -ForegroundColor Yellow

# 5. Iceberg Metadata
Write-Host "`n5. Iceberg Metadata Files" -ForegroundColor Green

Write-Host "`n   Videos Metadata:" -ForegroundColor Cyan
docker exec lakehouse_minio sh -c "ls -1 /data/bronze/tiktok_videos_metadata/metadata/*.metadata.json 2>/dev/null | wc -l" 2>$null | ForEach-Object { Write-Host "      Metadata JSON: $_" -ForegroundColor Gray }
docker exec lakehouse_minio sh -c "ls -1 /data/bronze/tiktok_videos_metadata/metadata/*.avro 2>/dev/null | wc -l" 2>$null | ForEach-Object { Write-Host "      Manifests/Snapshots: $_" -ForegroundColor Gray }

# 6. Storage Stats
Write-Host "`n6. Storage Statistics" -ForegroundColor Green
docker exec lakehouse_minio sh -c "du -sh /data/bronze/*" 2>$null

# Quick Access
Write-Host "`n========================================"
Write-Host "  QUICK ACCESS"
Write-Host "========================================"

Write-Host "`nMinIO Web Console:" -ForegroundColor Green
Write-Host "   URL: http://localhost:9001"
Write-Host "   Username: minioadmin"
Write-Host "   Password: minioadmin"

Write-Host "`nDirect Filesystem:" -ForegroundColor Green
Write-Host '   docker exec lakehouse_minio ls -R /data/bronze/'

Write-Host ""
