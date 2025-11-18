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
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minio_access_key \
        --conf spark.hadoop.fs.s3a.secret.key=minio_secret_key \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
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
    def bronze_raw_job(cls, job_config: dict) -> str:
        """
        Bronze RAW ingestion job with cluster mode (direct file upload)
        
        Args:
            job_config: Dict with 'script', 'source', 'bucket', 'type'
            
        Returns:
            Complete spark-submit command for RAW ingestion
        
        Note:
            Now uses cluster mode because:
            1. Bronze RAW jobs upload files DIRECTLY via mc client (no temp files)
            2. Source files are in /data/raw (shared volume accessible from all nodes)
            3. No repartition → no part files → no local /tmp access needed
            4. Cluster mode provides better resource utilization
        """
        cmd = f"""
    docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --jars /opt/spark/jars/postgresql-42.7.2.jar \
        --driver-class-path /opt/spark/jars/postgresql-42.7.2.jar \
        --conf spark.app.name=Bronze_RAW_{job_config['script']} \
        --conf spark.driver.memory=2g \
        --conf spark.executor.memory=2g \
        /opt/spark/jobs/bronze/{job_config['script']}.py \
        {job_config['source']} \
        {job_config['bucket']} \
        {job_config['type']}
        """
        return cmd.strip()
    
    @classmethod
    def silver_job(cls, job_name: str) -> str:
        """Shortcut for Silver layer jobs with proper memory allocation"""
        # TikTok comments needs more memory due to large file size
        memory = '4g' if 'comment' in job_name.lower() else '2g'
        return cls.build(
            job_path=f'/opt/spark/jobs/silver/{job_name}.py',
            bucket='silver',
            extra_conf=f'--conf spark.app.name=Silver_{job_name} --conf spark.driver.memory={memory} --conf spark.executor.memory={memory} --conf spark.executor.cores=2'
        )
    
    @classmethod
    def gold_job(cls, job_name: str) -> str:
        """Shortcut for Gold layer jobs"""
        return cls.build(
            job_path=f'/opt/spark/jobs/gold/{job_name}.py',
            bucket='gold',
            extra_conf=f'--conf spark.app.name=Gold_{job_name}'
        )
