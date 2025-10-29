"""
Reusable Spark Operators and Command Templates
"""

class SparkSubmitCommand:
    """
    Spark Submit command builder for Lakehouse jobs
    """
    
    BASE_CMD = """
    docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --jars /opt/spark/jars/postgresql-42.7.2.jar,/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
        --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.lakehouse.type=hive \
        --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
        --conf spark.sql.catalog.lakehouse.warehouse=s3a://{bucket}/ \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        {extra_conf} \
        {job_path} \
        {args}
    """
    
    @classmethod
    def build(cls, job_path: str, bucket: str = 'bronze', extra_conf: str = '', args: str = '') -> str:
        """
        Build Spark submit command
        
        Args:
            job_path: Path to Spark job (e.g., /opt/spark/jobs/bronze/ingest_tiktok_videos.py)
            bucket: S3 bucket name (bronze, silver, gold)
            extra_conf: Additional Spark configurations
            args: Command-line arguments for the job
            
        Returns:
            Complete spark-submit command
        """
        return cls.BASE_CMD.format(
            bucket=bucket,
            extra_conf=extra_conf,
            job_path=job_path,
            args=args
        )
    
    @classmethod
    def bronze_job(cls, job_name: str) -> str:
        """Shortcut for Bronze layer jobs"""
        return cls.build(
            job_path=f'/opt/spark/jobs/bronze/{job_name}.py',
            bucket='bronze',
            extra_conf=f'--conf spark.app.name=Bronze_{job_name}'
        )
    
    @classmethod
    def silver_job(cls, job_name: str) -> str:
        """Shortcut for Silver layer jobs"""
        return cls.build(
            job_path=f'/opt/spark/jobs/silver/{job_name}.py',
            bucket='silver',
            extra_conf=f'--conf spark.app.name=Silver_{job_name}'
        )
    
    @classmethod
    def gold_job(cls, job_name: str) -> str:
        """Shortcut for Gold layer jobs"""
        return cls.build(
            job_path=f'/opt/spark/jobs/gold/{job_name}.py',
            bucket='gold',
            extra_conf=f'--conf spark.app.name=Gold_{job_name}'
        )
