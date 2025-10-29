"""
Silver Layer DAG Configuration
Transform Bronze data into Silver layer with UPSERT and file size tracking
"""

from datetime import datetime, timedelta

# DAG metadata
DAG_ID = 'silver_layer_transformation'
DESCRIPTION = 'Transform Bronze CSV files into Silver Iceberg tables with UPSERT/APPEND logic'
TAGS = ['silver', 'transformation', 'upsert', 'deduplication', 'file-tracking']

# Schedule
SCHEDULE_INTERVAL = None  # Manual trigger (change to '@daily' for automation)
START_DATE = datetime(2025, 1, 1)
CATCHUP = False

# Task configuration
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
}

# Required containers for health check
REQUIRED_CONTAINERS = [
    'lakehouse_spark_master',
    'lakehouse_postgres',
    'lakehouse_minio',
    'lakehouse_hive_metastore'
]

# Silver transformation jobs
# Format: job_id: script_name (in /opt/spark/jobs/silver/)
SILVER_JOBS = {
    'transform_hotels_detail': 'transform_booking_hotels_detail',
    'transform_hotels_list': 'transform_booking_hotels_list',
    'transform_hotels_reviews': 'transform_booking_hotels_reviews',
    'transform_tiktok_videos': 'transform_tiktok_videos',
    'transform_tiktok_comments': 'transform_tiktok_comments',
}
