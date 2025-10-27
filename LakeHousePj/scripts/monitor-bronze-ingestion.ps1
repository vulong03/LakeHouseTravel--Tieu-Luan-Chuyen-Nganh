# Monitor Bronze Layer Ingestion Progress
# Author: AI Assistant
# Date: 2025-10-27

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  BRONZE INGESTION MONITOR" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "🌐 Airflow UI: http://localhost:8080" -ForegroundColor Green
Write-Host "   Username: admin | Password: admin`n" -ForegroundColor Gray

# Check tracking log
Write-Host "📊 Checking ingestion tracking log..." -ForegroundColor Yellow
Write-Host ""

docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "
SELECT 
    file_name, 
    records_ingested,
    CASE 
        WHEN ingestion_details IS NULL THEN '❌ Simple (1→1 table)'
        ELSE '✅ Multi: ' || 
             (ingestion_details::json->'tables'->0->>'name') || 
             ' + ' || 
             (ingestion_details::json->'tables'->1->>'name')
    END as ingestion_type,
    status,
    TO_CHAR(ingestion_timestamp, 'HH24:MI:SS') as time
FROM file_ingestion_log 
ORDER BY ingestion_timestamp DESC 
LIMIT 15;
"

Write-Host ""
Write-Host "📈 Summary Statistics:" -ForegroundColor Yellow

docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "
SELECT 
    status,
    COUNT(*) as count,
    SUM(records_ingested) as total_records,
    COUNT(CASE WHEN ingestion_details IS NOT NULL THEN 1 END) as multi_table_files
FROM file_ingestion_log 
GROUP BY status;
"

Write-Host ""
Write-Host "🔍 Check MinIO Data:" -ForegroundColor Yellow
Write-Host "docker exec lakehouse_minio mc alias set myminio http://localhost:9000 minioadmin minioadmin" -ForegroundColor Gray
Write-Host "docker exec lakehouse_minio mc ls myminio/lakehouse/bronze/ --recursive | head -20" -ForegroundColor Gray

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Press Ctrl+C to exit" -ForegroundColor Gray
Write-Host "========================================" -ForegroundColor Cyan
