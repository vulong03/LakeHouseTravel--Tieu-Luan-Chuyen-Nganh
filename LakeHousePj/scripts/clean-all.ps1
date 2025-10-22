Write-Host "=====================================" -ForegroundColor Red
Write-Host "CLEANING ALL LAKEHOUSE DATA & METADATA" -ForegroundColor Red
Write-Host "=====================================" -ForegroundColor Red

Write-Host "`n⚠️  This will DELETE:" -ForegroundColor Yellow
Write-Host "  - All Docker containers & volumes" -ForegroundColor Yellow
Write-Host "  - PostgreSQL metadata (83 Hive tables)" -ForegroundColor Yellow
Write-Host "  - MinIO buckets & data" -ForegroundColor Yellow
Write-Host "  - Hive Metastore data" -ForegroundColor Yellow

$confirm = Read-Host "`nAre you sure? Type 'yes' to confirm"
if ($confirm -ne "yes") {
    Write-Host "`n❌ Cancelled." -ForegroundColor Green
    exit 0
}

Write-Host "`n[1/5] Stopping all containers..." -ForegroundColor Cyan
docker-compose down -v
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Containers stopped and volumes removed" -ForegroundColor Green
}

Write-Host "`n[2/5] Removing PostgreSQL data..." -ForegroundColor Cyan
if (Test-Path "postgres/data") {
    Remove-Item -Recurse -Force "postgres/data"
    Write-Host "✅ PostgreSQL data removed" -ForegroundColor Green
} else {
    Write-Host "ℹ️  No PostgreSQL data found" -ForegroundColor Gray
}

Write-Host "`n[3/5] Removing MinIO data..." -ForegroundColor Cyan
if (Test-Path "minio/data") {
    Remove-Item -Recurse -Force "minio/data"
    Write-Host "✅ MinIO data removed" -ForegroundColor Green
} else {
    Write-Host "ℹ️  No MinIO data found" -ForegroundColor Gray
}

Write-Host "`n[4/5] Removing Hive data..." -ForegroundColor Cyan
if (Test-Path "hive/data") {
    Remove-Item -Recurse -Force "hive/data"
    Write-Host "✅ Hive data removed" -ForegroundColor Green
} else {
    Write-Host "ℹ️  No Hive data found" -ForegroundColor Gray
}

Write-Host "`n[5/5] Removing Docker images..." -ForegroundColor Cyan
docker rmi lakehousepj-hive-metastore -f 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Docker images removed" -ForegroundColor Green
} else {
    Write-Host "ℹ️  No custom images to remove" -ForegroundColor Gray
}

Write-Host "`n=====================================" -ForegroundColor Green
Write-Host "✅ CLEANUP COMPLETED" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green
Write-Host "`nYour project is now clean. To restart:" -ForegroundColor Cyan
Write-Host "  docker-compose up -d --build" -ForegroundColor White