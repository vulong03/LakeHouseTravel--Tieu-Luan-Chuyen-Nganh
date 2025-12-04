#!/usr/bin/env pwsh
# Run fact_comment_nlp_engagement job via spark-submit

$ErrorActionPreference = "Stop"

Write-Host "================================" -ForegroundColor Cyan
Write-Host "Running fact_comment_nlp_engagement" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

$jobPath = "fact_comment_nlp_engagement/fact_comment_nlp_engagement_job.py"

Write-Host "`n[*] Submitting Spark job: $jobPath" -ForegroundColor Yellow

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --num-executors 2 `
    --executor-cores 2 `
    --executor-memory 2G `
    --driver-memory 2G `
    --conf spark.sql.shuffle.partitions=20 `
    --conf spark.sql.adaptive.enabled=true `
    --conf spark.sql.adaptive.coalescePartitions.enabled=true `
    /opt/spark/jobs/gold/$jobPath

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n================================" -ForegroundColor Green
    Write-Host "✅ Job completed successfully!" -ForegroundColor Green
    Write-Host "================================" -ForegroundColor Green
} else {
    Write-Host "`n================================" -ForegroundColor Red
    Write-Host "❌ Job failed with exit code: $LASTEXITCODE" -ForegroundColor Red
    Write-Host "================================" -ForegroundColor Red
    exit $LASTEXITCODE
}
