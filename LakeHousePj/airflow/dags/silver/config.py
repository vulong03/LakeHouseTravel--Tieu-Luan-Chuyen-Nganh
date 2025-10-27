"""
Silver Layer DAG Configuration
"""

from datetime import datetime, timedelta

# DAG metadata
DAG_ID = 'silver_layer_transformation'
DESCRIPTION = 'Clean, validate, and enrich Bronze data into Silver layer'
TAGS = ['silver', 'transformation', 'cleaning', 'validation']

# Schedule
SCHEDULE_INTERVAL = '@daily'  # Run daily after Bronze
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

# Silver Spark jobs
SILVER_JOBS = {
    'clean_tiktok_videos': 'clean_tiktok_videos',
    'clean_tiktok_comments': 'clean_tiktok_comments',
    'clean_booking_hotels': 'clean_booking_hotels',
    'enrich_location_data': 'enrich_location_data',
}
