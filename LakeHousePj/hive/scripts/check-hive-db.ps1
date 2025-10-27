# ============================================
# Hive Database Health Check Script
# ============================================

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "  Hive Database Health Check" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

# 1. Check container status
Write-Host "`n[1/6] Checking container status..." -ForegroundColor Yellow
docker ps --filter "name=hive" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 2. Check Hive Metastore port
Write-Host "`n[2/6] Checking Hive Metastore (port 9083)..." -ForegroundColor Yellow
docker exec lakehouse_hive_metastore nc -z localhost 9083 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Hive Metastore is running on port 9083" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] Hive Metastore is NOT running" -ForegroundColor Red
}

# 3. Check HiveServer2 port
Write-Host "`n[3/6] Checking HiveServer2 (port 10000)..." -ForegroundColor Yellow
docker exec lakehouse_hive_server2 nc -z localhost 10000 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] HiveServer2 is running on port 10000" -ForegroundColor Green
} else {
    Write-Host "  [FAIL] HiveServer2 is NOT running" -ForegroundColor Red
}

# 4. Check PostgreSQL metadata database
Write-Host "`n[4/6] Checking PostgreSQL metadata database..." -ForegroundColor Yellow
$tableCount = docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';" 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] PostgreSQL metadata database is accessible" -ForegroundColor Green
    Write-Host "  [INFO] Total metadata tables: $($tableCount.Trim())" -ForegroundColor Cyan
} else {
    Write-Host "  [FAIL] PostgreSQL metadata database is NOT accessible" -ForegroundColor Red
}

# 5. List Hive databases
Write-Host "`n[5/6] Listing Hive databases..." -ForegroundColor Yellow
$hiveDbOutput = docker exec lakehouse_hive_server2 /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -e 'SHOW DATABASES;' 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Successfully connected to HiveServer2" -ForegroundColor Green
    Write-Host "`n  Available databases:" -ForegroundColor Cyan
    $hiveDbOutput | Select-String -Pattern "^\|" | Where-Object { $_ -notmatch "database_name" -and $_ -notmatch "^\+-" } | ForEach-Object {
        $dbName = $_.Line.Trim().Trim('|').Trim()
        if ($dbName) {
            Write-Host "    - $dbName" -ForegroundColor White
        }
    }
} else {
    Write-Host "  [FAIL] Failed to connect to HiveServer2" -ForegroundColor Red
}

# 6. Test creating a table
Write-Host "`n[6/6] Testing table operations..." -ForegroundColor Yellow
$testQuery = "CREATE TABLE IF NOT EXISTS default.health_check_test (id INT, check_time STRING) STORED AS PARQUET;"
docker exec lakehouse_hive_server2 /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -e $testQuery 2>&1 | Out-Null

if ($LASTEXITCODE -eq 0) {
    Write-Host "  [OK] Table operations working correctly" -ForegroundColor Green
    
    # List tables in default database
    Write-Host "`n  Tables in 'default' database:" -ForegroundColor Cyan
    $tablesOutput = docker exec lakehouse_hive_server2 /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -e 'SHOW TABLES IN default;' 2>&1
    
    $tablesOutput | Select-String -Pattern "^\|" | Where-Object { $_ -notmatch "tab_name" -and $_ -notmatch "^\+-" } | ForEach-Object {
        $tableName = $_.Line.Trim().Trim('|').Trim()
        if ($tableName) {
            Write-Host "    - $tableName" -ForegroundColor White
        }
    }
} else {
    Write-Host "  [FAIL] Table operations failed" -ForegroundColor Red
}

# Summary
Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host "  Health Check Complete!" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Write-Host "`nUseful Hive Commands:" -ForegroundColor Yellow
Write-Host "  - Connect to Hive (interactive):" -ForegroundColor White
Write-Host "    docker exec -it lakehouse_hive_server2 /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl'" -ForegroundColor Gray
Write-Host "`n  - Show databases:" -ForegroundColor White
Write-Host "    docker exec lakehouse_hive_server2 /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -e 'SHOW DATABASES;'" -ForegroundColor Gray
Write-Host "`n  - Check PostgreSQL metadata:" -ForegroundColor White
Write-Host "    docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c '\dt'" -ForegroundColor Gray
Write-Host "`n  - View HiveServer2 logs:" -ForegroundColor White
Write-Host "    docker logs lakehouse_hive_server2 --tail 50" -ForegroundColor Gray
Write-Host "`n  - View HiveServer2 detailed logs:" -ForegroundColor White
Write-Host "    docker exec lakehouse_hive_server2 tail -100 /tmp/hive/hive.log" -ForegroundColor Gray
Write-Host ""
