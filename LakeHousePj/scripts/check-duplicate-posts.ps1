Write-Host "==========================================="
Write-Host "   CHECK DUPLICATE POSTS"
Write-Host "==========================================="
Write-Host ""
Write-Host "Analyzing duplicates in tiktok_post_metadata..."
Write-Host ""

# Copy script to container
docker cp D:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\check_duplicate_posts.py lakehouse_spark_master:/opt/spark/jobs/check_duplicate_posts.py

# Run the script
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
    /opt/spark/jobs/check_duplicate_posts.py

Write-Host ""
Write-Host "Done!"
