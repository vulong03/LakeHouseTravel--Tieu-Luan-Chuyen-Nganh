# Test Hotels List Pipeline - Simple Version

Write-Host "================================================================================"
Write-Host "TESTING HOTELS LIST PIPELINE - 2 TASKS"
Write-Host "================================================================================"
Write-Host ""

# Task 1: Transform
Write-Host "================================================================================"
Write-Host "TASK 1: TRANSFORM FROM BRONZE"
Write-Host "================================================================================"
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/lakehouse `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    /opt/spark/jobs/silver/hotels_list/step_01_transform.py

$task1Exit = $LASTEXITCODE

if ($task1Exit -ne 0) {
    Write-Host ""
    Write-Host "FAILED: Task 1 failed" -ForegroundColor Red
    exit $task1Exit
}

Write-Host ""
Write-Host "SUCCESS: Task 1 completed" -ForegroundColor Green
Write-Host ""

# Task 2: Clean & Load
Write-Host "================================================================================"
Write-Host "TASK 2: CLEAN & LOAD TO SILVER"
Write-Host "================================================================================"
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/lakehouse `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem `
    /opt/spark/jobs/silver/hotels_list/step_02_clean_load.py

$task2Exit = $LASTEXITCODE

Write-Host ""
if ($task2Exit -eq 0) {
    Write-Host "SUCCESS: Task 2 completed" -ForegroundColor Green
    Write-Host ""
    Write-Host "Pipeline completed successfully!" -ForegroundColor Green
} else {
    Write-Host "FAILED: Task 2 failed" -ForegroundColor Red
}

Write-Host ""
exit $task2Exit
