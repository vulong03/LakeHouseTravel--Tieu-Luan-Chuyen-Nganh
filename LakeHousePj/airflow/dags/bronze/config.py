"""
Bronze Layer DAG Configuration
"""

from datetime import datetime, timedelta

# DAG metadata
DAG_ID = 'bronze_layer_ingestion'
DESCRIPTION = 'Ingest TikTok and Booking.com data into Bronze layer with checksum tracking'
TAGS = ['bronze', 'ingestion', 'tiktok', 'booking', 'incremental']

# Schedule
SCHEDULE_INTERVAL = None  # Manual trigger (change to '@daily' or '@hourly' for automation)
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
    'execution_timeout': timedelta(hours=2),
}

# Required containers for health check
REQUIRED_CONTAINERS = [
    'lakehouse_spark_master',
    'lakehouse_postgres',
    'lakehouse_minio',
    'lakehouse_hive_metastore'
]

# Bronze Spark jobs
BRONZE_JOBS = {
    'tiktok_videos': 'ingest_tiktok_videos',
    'tiktok_comments': 'ingest_tiktok_comments',
    'booking_list': 'ingest_booking_hotels_list',
    'booking_detail': 'ingest_booking_hotels_detail',
    'booking_reviews': 'ingest_booking_hotels_reviews',
}
