"""
Spark Session Utility
Creates configured Spark session with Iceberg support
"""

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session(app_name="LakehouseTourism"):
    """
    Create and return a Spark session configured for Iceberg and S3/MinIO
    
    Args:
        app_name (str): Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.lakehouse.type", "hive") \
            .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
            .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.silver.type", "hive") \
            .config("spark.sql.catalog.silver.uri", "thrift://hive-metastore:9083") \
            .config("spark.sql.catalog.silver.warehouse", "s3a://silver/lakehouse") \
            .config("spark.sql.defaultCatalog", "lakehouse") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .enableHiveSupport() \
            .getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"✅ Spark session created: {app_name}")
        logger.info(f"   Spark version: {spark.version}")
        logger.info(f"   Catalog: lakehouse (Iceberg + Hive)")
        
        return spark
    
    except Exception as e:
        logger.error(f"❌ Failed to create Spark session: {str(e)}")
        raise


if __name__ == "__main__":
    print("Spark session utility module loaded successfully")
