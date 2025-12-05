# reset-province-month-features.ps1

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   RESET province_month_features TABLE" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "This will:" -ForegroundColor Yellow
Write-Host "  1. Drop table gold.gold.province_month_features" -ForegroundColor Yellow
Write-Host "  2. Delete MinIO data files" -ForegroundColor Yellow
Write-Host "  3. Delete Parquet export for ML" -ForegroundColor Yellow
Write-Host ""

$confirmation = Read-Host "Continue? Type 'YES' to confirm"
if ($confirmation -ne "YES") {
    Write-Host ""
    Write-Host "Operation cancelled." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Starting reset..." -ForegroundColor Green
Write-Host ""

# Step 1: Drop Iceberg table
Write-Host "=== STEP 1: Drop Iceberg table ===" -ForegroundColor Yellow
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
  --master spark://spark-master:7077 `
  --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog `
  --conf spark.sql.catalog.gold.type=hive `
  --conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 `
  --conf spark.sql.catalog.gold.warehouse=s3a://gold/ `
  -e "DROP TABLE IF EXISTS gold.gold.province_month_features"

if ($LASTEXITCODE -eq 0) {
    Write-Host "SUCCESS: Table dropped" -ForegroundColor Green
} else {
    Write-Host "WARNING: Table may not exist" -ForegroundColor Yellow
}
Write-Host ""

# Step 2: Clean MinIO data
Write-Host "=== STEP 2: Clean MinIO data ===" -ForegroundColor Yellow
docker exec lakehouse_minio mc rm --recursive --force minio/gold/lakehouse/gold.db/province_month_features/

if ($LASTEXITCODE -eq 0) {
    Write-Host "SUCCESS: MinIO data cleaned" -ForegroundColor Green
} else {
    Write-Host "WARNING: Data may not exist" -ForegroundColor Yellow
}
Write-Host ""

# Step 3: Clean ML Parquet export
Write-Host "=== STEP 3: Clean ML Parquet export ===" -ForegroundColor Yellow
docker exec lakehouse_minio mc rm --recursive --force minio/gold/ml_training/province_month_features.parquet/

if ($LASTEXITCODE -eq 0) {
    Write-Host "SUCCESS: ML export cleaned" -ForegroundColor Green
} else {
    Write-Host "WARNING: Export may not exist" -ForegroundColor Yellow
}
Write-Host ""

Write-Host "============================================" -ForegroundColor Green
Write-Host "   RESET COMPLETE!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Next step: Run aggregation job" -ForegroundColor Cyan
Write-Host "  docker exec lakehouse_spark_master /opt/spark/bin/spark-submit ..." -ForegroundColor Gray
Write-Host ""