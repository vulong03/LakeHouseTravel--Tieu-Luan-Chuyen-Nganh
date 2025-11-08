Write-Host "==========================================="
Write-Host "   DELETE ALL DATA FROM SILVER LAYER"
Write-Host "==========================================="
Write-Host ""
Write-Host "⚠️  WARNING: This will delete ALL data from Silver tables!"
Write-Host ""

$confirmation = Read-Host "Are you sure you want to continue? (yes/no)"

if ($confirmation -ne "yes") {
    Write-Host "❌ Operation cancelled."
    exit
}

Write-Host ""
Write-Host "🗑️  Deleting all data from Silver layer..."
Write-Host ""

# Copy script to container
docker cp D:\CodeStored\Nam_4\TieuLuanCuoiKy\LakeHouse\delete_silver_data.py lakehouse_spark_master:/opt/spark/jobs/delete_silver_data.py

# Run the deletion script
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
    /opt/spark/jobs/delete_silver_data.py

Write-Host ""
Write-Host "✅ Done!"
Write-Host ""
Write-Host "Next steps:"
Write-Host "   1. Run bronze ingestion script"
Write-Host "   2. Check silver data script"
