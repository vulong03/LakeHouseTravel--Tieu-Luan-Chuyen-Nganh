#!/usr/bin/env pwsh
# Run Province Recommendation Model Training

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================"
Write-Host "Province Recommendation - Model Training"
Write-Host "========================================"
Write-Host ""

$jobPath = "/opt/spark/ml/scripts/train_province_recommendation.py"

Write-Host "Training Configuration:"
Write-Host "  Script: $jobPath"
Write-Host "  MLflow: http://localhost:5001"
Write-Host ""
Write-Host "Starting training job..."
Write-Host ""

$cmd = "/opt/spark/bin/spark-submit " +
    "--master spark://spark-master:7077 " +
    "--name ProvinceRecommendationTraining " +
    "--executor-memory 2g " +
    "--executor-cores 2 " +
    "--driver-memory 1g " +
    "--conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog " +
    "--conf spark.sql.catalog.gold.type=hive " +
    "--conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 " +
    "--conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse " +
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 " +
    "--conf spark.hadoop.fs.s3a.access.key=minio_admin " +
    "--conf spark.hadoop.fs.s3a.secret.key=minio_password " +
    "--conf spark.hadoop.fs.s3a.path.style.access=true " +
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem " +
    "--packages org.mlflow:mlflow-spark:2.9.2 " +
    "$jobPath"

docker exec lakehouse_spark_master bash -c $cmd

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================"
    Write-Host "Training completed successfully!"
    Write-Host "========================================"
    Write-Host ""
    Write-Host "Next steps:"
    Write-Host "1. Check MLflow UI: http://localhost:5001"
    Write-Host "2. Register model to Production stage"
    Write-Host "3. Run Gradio app: cd gradio && python app.py"
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "========================================"
    Write-Host "Training failed with exit code: $LASTEXITCODE"
    Write-Host "========================================"
    Write-Host ""
    exit $LASTEXITCODE
}
