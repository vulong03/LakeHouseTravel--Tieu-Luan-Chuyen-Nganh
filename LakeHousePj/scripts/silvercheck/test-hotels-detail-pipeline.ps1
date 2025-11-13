# Test Hotels Detail Pipeline - 2-Task Pattern
# Step 1: Transform Bronze → Scratch
# Step 2: Clean & Load Scratch → Silver

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "HOTELS DETAIL PIPELINE TEST" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`n[1/2] Running Step 1: Transform (Bronze → Scratch)" -ForegroundColor Yellow
Write-Host "------------------------------------------------" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    /opt/spark/jobs/silver/hotels_detail/step_01_transform.py

$step1Exit = $LASTEXITCODE

if ($step1Exit -ne 0) {
    Write-Host "`n[X] Step 1 FAILED (Exit Code: $step1Exit)" -ForegroundColor Red
    exit $step1Exit
}

Write-Host "`n[OK] Step 1 completed successfully" -ForegroundColor Green

Write-Host "`n[2/2] Running Step 2: Clean & Load (Scratch → Silver)" -ForegroundColor Yellow
Write-Host "------------------------------------------------" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    /opt/spark/jobs/silver/hotels_detail/step_02_clean_load.py

$step2Exit = $LASTEXITCODE

Write-Host "`n========================================" -ForegroundColor Cyan

if ($step2Exit -eq 0) {
    Write-Host "[OK] PIPELINE COMPLETED SUCCESSFULLY" -ForegroundColor Green
    
    Write-Host "`n[*] Verifying data in Silver table..." -ForegroundColor Cyan
    docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        -e "SELECT COUNT(*) as total_records FROM silver.silver.hotels_detail"
    
    Write-Host "`n[*] Checking table location..." -ForegroundColor Cyan
    docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        -e "DESCRIBE EXTENDED silver.silver.hotels_detail" | Select-String "Location"
    
    Write-Host "`n[*] Sample data (5 rows):" -ForegroundColor Cyan
    docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
        --master spark://spark-master:7077 `
        -e "SELECT hotel_name, province, rating_score, review_count_text FROM silver.silver.hotels_detail LIMIT 5"
    
} else {
    Write-Host "[X] PIPELINE FAILED (Exit Code: $step2Exit)" -ForegroundColor Red
}

exit $step2Exit
