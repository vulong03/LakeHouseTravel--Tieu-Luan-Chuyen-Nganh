# Check Hive Metastore - Verify Tables and Metadata

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "CHECKING HIVE METASTORE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`n[1] Checking Silver tables in Hive..." -ForegroundColor Yellow

# Query Hive metastore via Spark SQL
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql `
    --master spark://spark-master:7077 `
    --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog `
    --conf spark.sql.catalog.lakehouse.type=hive `
    --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 `
    --conf spark.sql.catalog.lakehouse.warehouse=s3a://silver/lakehouse `
    -e "SHOW TABLES IN silver;"

Write-Host "`n[2] Checking PostgreSQL tracking logs..." -ForegroundColor Yellow

# Query tracking database
docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c `
    "SELECT layer, table_name, COUNT(*) as records FROM file_ingestion_log WHERE layer='silver' GROUP BY layer, table_name ORDER BY table_name;"

Write-Host "`n[3] Checking MinIO Silver bucket..." -ForegroundColor Yellow

# List Silver bucket contents
docker exec lakehouse_minio mc ls minio/silver/lakehouse/

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "METADATA CHECK COMPLETED" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
