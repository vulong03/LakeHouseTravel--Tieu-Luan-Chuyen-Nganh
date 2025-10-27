"""
Gold Layer DAG Configuration
"""

from datetime import datetime, timedelta

# DAG metadata
DAG_ID = 'gold_layer_aggregation'
DESCRIPTION = 'Create analytics-ready aggregations from Silver layer'
TAGS = ['gold', 'aggregation', 'analytics', 'metrics']

# Schedule
SCHEDULE_INTERVAL = '@weekly'  # Run weekly for analytics
START_DATE = datetime(2025, 1, 1)
CATCHUP = False

# Task configuration
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=4),
}

# Gold Spark jobs (TODO: Implement when ready)
GOLD_JOBS = {
    'aggregate_hotels': 'aggregate_hotel_ratings',
    'aggregate_tiktok': 'aggregate_tiktok_engagement',
    'build_recommendations': 'build_recommendation_table',
}
