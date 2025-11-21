# ============================================
# Query Gold Layer Job Execution Logs
# ============================================
# Purpose: View job execution history from PostgreSQL
# ============================================

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   Gold Layer Job Logs" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "📋 Recent Gold job executions:" -ForegroundColor Yellow
Write-Host ""

docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "
SELECT 
    id,
    table_name,
    records_ingested,
    status,
    ingestion_timestamp,
    EXTRACT(EPOCH FROM (ingestion_timestamp - LAG(ingestion_timestamp) OVER (PARTITION BY table_name ORDER BY ingestion_timestamp))) as seconds_since_last,
    error_message
FROM file_ingestion_log
WHERE layer = 'gold'
ORDER BY ingestion_timestamp DESC
LIMIT 20;
"

Write-Host ""
Write-Host "📊 Summary by table:" -ForegroundColor Yellow
Write-Host ""

docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "
SELECT 
    table_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
    MAX(ingestion_timestamp) as last_run,
    MAX(CASE WHEN status = 'success' THEN records_ingested ELSE 0 END) as last_record_count
FROM file_ingestion_log
WHERE layer = 'gold'
GROUP BY table_name
ORDER BY last_run DESC;
"

Write-Host ""
Write-Host "🔍 Latest dim_province job details:" -ForegroundColor Yellow
Write-Host ""

docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "
SELECT 
    id,
    file_path as source,
    records_ingested,
    status,
    ingestion_timestamp,
    ingestion_details::json->>'execution_time_seconds' as exec_time_sec,
    ingestion_details::json->>'central_cities_count' as cities,
    ingestion_details::json->>'provinces_with_name_change' as name_changes
FROM file_ingestion_log
WHERE layer = 'gold' 
  AND table_name = 'gold.dim_province'
ORDER BY ingestion_timestamp DESC
LIMIT 5;
"

Write-Host ""

