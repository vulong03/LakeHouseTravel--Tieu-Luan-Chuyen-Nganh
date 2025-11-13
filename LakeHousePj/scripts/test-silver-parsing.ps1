Write-Host "==========================================="
Write-Host "   TEST SILVER PARSING LOGIC"
Write-Host "==========================================="
Write-Host ""
Write-Host "Testing how Bronze files are parsed into Silver..."
Write-Host "Checking UTF-8, emoji, and Vietnamese characters..."
Write-Host ""

docker exec lakehouse_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --driver-memory 2g `
    --executor-memory 2g `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/warehouse `
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions `
    --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar,/opt/spark/jars/postgresql-42.7.2.jar `
    /opt/spark/jobs/utils/test_silver_parsing.py

Write-Host ""
Write-Host "Done!"
