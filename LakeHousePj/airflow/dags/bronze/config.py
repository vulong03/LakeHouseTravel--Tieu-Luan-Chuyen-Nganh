"""
Bronze Layer RAW Ingestion DAG Configuration
Purpose: Copy raw CSV files from /data/raw to MinIO s3://bronze with UTF-8 encoding
"""

from datetime import datetime, timedelta

# DAG metadata
DAG_ID = 'bronze_raw_ingestion'
DESCRIPTION = 'Ingest raw CSV files to Bronze MinIO with UTF-8 encoding and checksum tracking'
TAGS = ['bronze', 'raw', 'ingestion', 'tiktok', 'booking', 'utf8']

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
    'execution_timeout': timedelta(hours=2),
}

# Required containers for health check
REQUIRED_CONTAINERS = [
    'lakehouse_spark_master',
    'lakehouse_postgres',
    'lakehouse_minio',
]

# Bronze RAW jobs (NEW - using raw_ingest_* scripts)
BRONZE_RAW_JOBS = {
    'booking_list': {
        'script': 'raw_ingest_booking_hotels_list',
        'source': '/data/raw/booking/vietnam_hotels_list.csv',
        'bucket': 'bronze',
        'type': 'booking_hotels_list',
        'description': 'Ingest Booking.com hotels list (10K hotels)',
    },
    'booking_detail': {
        'script': 'raw_ingest_booking_hotels_detail',
        'source': '/data/raw/booking/vietnam_hotels_detail.csv',
        'bucket': 'bronze',
        'type': 'booking_hotels_detail',
        'description': 'Ingest Booking.com hotels detail (10K hotels)',
    },
    'booking_reviews': {
        'script': 'raw_ingest_booking_hotels_reviews',
        'source': '/data/raw/booking/vietnam_hotels_reviews.csv',
        'bucket': 'bronze',
        'type': 'booking_hotels_reviews',
        'description': 'Ingest Booking.com reviews (1M+ records, multiline CSV)',
    },
    'tiktok_videos': {
        'script': 'raw_ingest_tiktok_videos',
        'source': '/data/raw/tiktok/links/merged_videos.csv',
        'bucket': 'bronze',
        'type': 'tiktok_videos',
        'description': 'Ingest TikTok videos metadata (8K videos)',
    },
    'tiktok_comments': {
        'script': 'raw_ingest_tiktok_comments_batch',
        'source': '/data/raw/tiktok/comments',
        'bucket': 'bronze',
        'type': 'tiktok_comments',
        'description': 'Batch ingest TikTok comments (6 files, 16-line metadata headers)',
    },
}
