#!/usr/bin/env pwsh
# Run Province-Month Aggregation Job

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================"
Write-Host "Province-Month Aggregation Job"
Write-Host "========================================"
Write-Host ""

$jobPath = "/opt/spark/jobs/gold/TrainingModel/province_month_features_job.py"
$jobName = "ProvinceMonthAggregation"
$sparkMaster = "spark://spark-master:7077"

Write-Host "Job Configuration:"
Write-Host "  Job: $jobName"
Write-Host "  Script: $jobPath"
Write-Host ""
Write-Host "Starting Spark job..."
Write-Host ""

$cmd = "/opt/spark/bin/spark-submit " +
    "--master $sparkMaster " +
    "--name $jobName " +
    "--executor-memory 2g " +
    "--executor-cores 2 " +
    "--num-executors 2 " +
    "--driver-memory 1g " +
    "--conf spark.sql.shuffle.partitions=20 " +
    "--conf spark.sql.catalog.gold=org.apache.iceberg.spark.SparkCatalog " +
    "--conf spark.sql.catalog.gold.type=hive " +
    "--conf spark.sql.catalog.gold.uri=thrift://hive-metastore:9083 " +
    "--conf spark.sql.catalog.gold.warehouse=s3a://gold/lakehouse " +
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 " +
    "--conf spark.hadoop.fs.s3a.access.key=minio_admin " +
    "--conf spark.hadoop.fs.s3a.secret.key=minio_password " +
    "--conf spark.hadoop.fs.s3a.path.style.access=true " +
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem " +
    "$jobPath"

docker exec lakehouse_spark_master bash -c $cmd

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================"
    Write-Host "Job completed successfully!"
    Write-Host "========================================"
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "========================================"
    Write-Host "Job failed with exit code: $LASTEXITCODE"
    Write-Host "========================================"
    Write-Host ""
    exit $LASTEXITCODE
}
