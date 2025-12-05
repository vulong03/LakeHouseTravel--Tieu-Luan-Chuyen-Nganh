# ============================================================================
# Train XGBoost Model & Forecast Province Hotness (12 Months Ahead)
# ============================================================================
# 
# Pipeline:
#   1. Tính hotness_score từ 14 metrics (5 weight groups)
#   2. Tạo lag features (lag_1/2/3/12, rolling_avg_3m)
#   3. Train XGBoost model với MLflow tracking
#   4. Forecast 12 tháng tiếp theo (recursive autoregressive)
#
# Output:
#   - Model: province_hotness_forecaster (MLflow registry)
#   - Table: gold.gold.province_month_forecast_next12
#   - MLflow: Metrics, plots, model artifacts
# ============================================================================

Write-Host "`n============================================================================" -ForegroundColor Cyan
Write-Host "ML PIPELINE: TRAIN & FORECAST PROVINCE HOTNESS" -ForegroundColor Cyan
Write-Host "============================================================================`n" -ForegroundColor Cyan

Write-Host "Workflow:" -ForegroundColor Yellow
Write-Host "  1. Calculate hotness_score (14 metrics)" -ForegroundColor White
Write-Host "     - Volume: 20% | Post: 25% | Comment: 10% | Sentiment: 30% | NLP: 15%" -ForegroundColor DarkGray
Write-Host "  2. Create lag features (5 features)" -ForegroundColor White
Write-Host "     - lag_1/2/3/12 + rolling_avg_3m" -ForegroundColor DarkGray
Write-Host "  3. Train XGBoost model (70/30 split)" -ForegroundColor White
Write-Host "     - 12 features: temporal(3) + lag(5) + current(4)" -ForegroundColor DarkGray
Write-Host "  4. Forecast 12 months ahead" -ForegroundColor White
Write-Host "     - Recursive autoregressive: 636 predictions (53×12)" -ForegroundColor DarkGray

Write-Host "`nOutput:" -ForegroundColor Yellow
Write-Host "  - Model: province_hotness_forecaster" -ForegroundColor White
Write-Host "  - Table: gold.gold.province_month_forecast_next12" -ForegroundColor White
Write-Host "  - MLflow: Experiments tracked in PostgreSQL + MinIO" -ForegroundColor White

Write-Host "`n============================================================================`n" -ForegroundColor Cyan

$confirm = Read-Host "Continue? (y/n)"
if ($confirm -ne 'y') {
    Write-Host "Cancelled." -ForegroundColor Red
    exit
}

Write-Host "`nStarting pipeline...`n" -ForegroundColor Green

# Execute Spark job
docker-compose exec spark-master /opt/spark/bin/spark-submit `
    --master local[2] `
    --deploy-mode client `
    --executor-memory 2g `
    --conf spark.app.name="ML_Province_Hotness_Pipeline" `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    --conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.gold.type=hive `
    --conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 `
    /opt/spark/jobs/ml/train_and_forecast.py

Write-Host "n============================================================================" -ForegroundColor Cyan
Write-Host " PIPELINE COMPLETED!" -ForegroundColor Green
Write-Host "============================================================================" -ForegroundColor Cyan

Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Check MLflow: http://localhost:5001" -ForegroundColor White
Write-Host "  2. Query forecast: SELECT * FROM gold.gold.province_month_forecast_next12" -ForegroundColor White
Write-Host "  3. Open Gradio app: http://localhost:7860" -ForegroundColor White
Write-Host ""
