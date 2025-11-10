# Clean Scratch Folder - hotels_reviews
Write-Host ""
Write-Host "========================================"
Write-Host "Clean Scratch - hotels_reviews"
Write-Host "========================================"
Write-Host ""

$pythonScript = @'
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("CleanScratch") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
sc = spark.sparkContext

# Get S3A FileSystem with proper URI
uri = sc._jvm.java.net.URI("s3a://scratch/")
conf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

base_path = sc._jvm.org.apache.hadoop.fs.Path("s3a://scratch/pipeline/silver/hotels_reviews")

print("Checking scratch base path...")
if not fs.exists(base_path):
    print("Base path does not exist yet. Nothing to clean.")
    spark.stop()
    sys.exit(0)

file_status_list = fs.listStatus(base_path)
run_folders = [status.getPath() for status in file_status_list if status.getPath().getName().startswith("run_")]

if not run_folders:
    print("No run folders found. Nothing to clean.")
    spark.stop()
    sys.exit(0)

print("")
print("Found " + str(len(run_folders)) + " run folder(s):")
for path in run_folders:
    print("  - " + path.getName())

print("")
print("Deleting folders...")
for path in run_folders:
    try:
        fs.delete(path, True)
        print("  Deleted: " + path.getName())
    except Exception as e:
        print("  Failed to delete " + path.getName() + ": " + str(e))

print("")
print("Cleanup complete!")
spark.stop()
'@

$tempFile = [System.IO.Path]::GetTempFileName() + ".py"
$pythonScript | Out-File -FilePath $tempFile -Encoding UTF8

Write-Host "Copying cleanup script to Spark container..."
docker cp $tempFile lakehouse_spark_master:/tmp/clean_scratch.py

Write-Host "Running cleanup script..."
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit --master local[1] --deploy-mode client /tmp/clean_scratch.py

Remove-Item $tempFile -ErrorAction SilentlyContinue
docker exec lakehouse_spark_master rm -f /tmp/clean_scratch.py

Write-Host ""
Write-Host "========================================"
Write-Host "Cleanup Finished"
Write-Host "========================================"
Write-Host ""
