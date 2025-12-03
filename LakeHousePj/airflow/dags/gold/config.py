"""
Gold Layer DAG Configuration
Based on Silver layer pattern with user-suggested flow:
  Phase 1: Common dimensions (parallel)
  Phase 2a: TikTok pipeline (sequential) 
  Phase 2b: Hotel pipeline (sequential)
  Phase 3: Fact tables (sequential)
  Phase 4: ML Training
"""

from datetime import datetime, timedelta

# ============================================
# DAG METADATA
# ============================================

DAG_ID = 'gold_layer_aggregation'
DESCRIPTION = 'Gold layer: Common dims → TikTok/Hotel pipelines → Facts → ML Training'
TAGS = ['gold', 'dimension', 'fact', 'analytics', 'ml', '4-phase']

# ============================================
# SCHEDULE
# ============================================

SCHEDULE_INTERVAL = '@weekly'  # Run weekly for analytics
START_DATE = datetime(2025, 1, 1)
CATCHUP = False

# ============================================
# TASK CONFIGURATION
# ============================================

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=4),
}

# Required containers for health check
REQUIRED_CONTAINERS = [
    'lakehouse_spark_master',
    'lakehouse_spark_worker_1',
    'lakehouse_postgres',
    'lakehouse_minio',
    'lakehouse_hive_metastore'
]

# ============================================
# GOLD JOBS CONFIGURATION (3-Phase Strategy)
# ============================================

GOLD_JOBS = {
    # ===================================
    # PHASE 1: Common Dimensions (PARALLEL - 3 jobs)
    # ===================================
    # These are shared by both TikTok and Hotel pipelines
    
    'dim_date': {
        'job_path': 'dim_date/dim_date_job.py',
        'phase': 1,
        'resource_level': 'light',
        'depends_on': [],
        'description': 'Date dimension from PostgreSQL Date_DB'
    },
    
    'dim_province': {
        'job_path': 'dim_province/dim_province_job.py',
        'phase': 1,
        'resource_level': 'light',
        'depends_on': [],
        'description': 'Province dimension from CSV with merger mappings'
    },
    
    'dim_destination': {
        'job_path': 'dim_destination/dim_destination_job.py',
        'phase': 1,
        'resource_level': 'light',
        'depends_on': [],
        'description': 'Destination dimension from CSV'
    },
    
    # ===================================
    # PHASE 2a: TikTok Pipeline (SEQUENTIAL - 3 jobs)
    # ===================================
    # Must run after Phase 1, sequential order matters
    
    'dim_author': {
        'job_path': 'dim_author/dim_author_job.py',
        'phase': 2,
        'pipeline': 'tiktok',
        'resource_level': 'light',
        'depends_on': [],  # No dimension dependencies, only Phase 1 complete
        'description': 'Author dimension from tiktok_post_metadata'
    },
    
    'dim_post': {
        'job_path': 'dim_post/dim_post_job.py',
        'phase': 2,
        'pipeline': 'tiktok',
        'resource_level': 'medium',  # Joins with multiple dims
        'depends_on': ['dim_author', 'dim_province', 'dim_date'],
        'description': 'Post dimension with author, province, date FKs'
    },
    
    'dim_comment': {
        'job_path': 'dim_comment/dim_comment_job.py',
        'phase': 2,
        'pipeline': 'tiktok',
        'resource_level': 'heavy',  # Large dataset, MERGE operation
        'depends_on': ['dim_post', 'dim_date'],
        'description': 'Comment dimension with post, date FKs (MERGE mode)'
    },
    
    # ===================================
    # PHASE 2b: Hotel Pipeline (SEQUENTIAL - 4 jobs)
    # ===================================
    # Must run after Phase 1, sequential order matters
    
    'dim_room_type': {
        'job_path': 'dim_room_type/dim_room_type_job.py',
        'phase': 2,
        'pipeline': 'hotel',
        'resource_level': 'light',
        'depends_on': [],  # No dimension dependencies, only Phase 1 complete
        'description': 'Room type dimension from hotels_reviews'
    },
    
    'dim_travel_type': {
        'job_path': 'dim_travel_type/dim_travel_type_job.py',
        'phase': 2,
        'pipeline': 'hotel',
        'resource_level': 'light',
        'depends_on': [],  # No dimension dependencies
        'description': 'Travel type dimension from hotels_reviews'
    },
    
    'dim_country': {
        'job_path': 'dim_country/dim_country_job.py',
        'phase': 2,
        'pipeline': 'hotel',
        'resource_level': 'light',
        'depends_on': [],  # No dimension dependencies
        'description': 'Country dimension from hotels_reviews with region mapping'
    },
    
    'dim_hotel': {
        'job_path': 'dim_hotel/dim_hotel_job.py',
        'phase': 2,
        'pipeline': 'hotel',
        'resource_level': 'medium',
        'depends_on': ['dim_province'],
        'description': 'Hotel dimension with province FK'
    },
    
    # ===================================
    # PHASE 3: Fact Tables (SEQUENTIAL - 3 jobs)
    # ===================================
    # Run after both TikTok and Hotel pipelines complete
    # Sequential to avoid resource exhaustion
    
    'fact_province_content_engagement': {
        'job_path': 'fact_province_content_engagement/fact_province_content_engagement_job.py',
        'phase': 3,
        'resource_level': 'heavy',
        'depends_on': ['dim_post'],  # Only needs TikTok pipeline
        'description': 'TikTok engagement metrics by province and date'
    },
    
    'fact_hotel_review_daily': {
        'job_path': 'fact_hotel_review_daily/fact_hotel_review_daily_job.py',
        'phase': 3,
        'resource_level': 'heavy',
        'depends_on': ['dim_hotel', 'dim_travel_type', 'dim_room_type', 'dim_country', 'dim_date', 'fact_province_content_engagement'],  # Sequential: wait for first fact
        'description': 'Daily hotel review metrics with all dimension FKs'
    },
    
    'fact_comment_nlp_engagement': {
        'job_path': 'fact_comment_nlp_engagement/fact_comment_nlp_engagement_job.py',
        'phase': 3,
        'resource_level': 'heavy',  # NLP extraction is CPU intensive
        'depends_on': ['dim_comment', 'dim_post', 'fact_hotel_review_daily'],  # Sequential: run last to avoid resource conflict
        'description': 'ML feature engineering: comment NLP + engagement metrics'
    },
    
    # ===================================
    # PHASE 4: ML Training (After all facts complete)
    # ===================================
    
    'train_province_model': {
        'job_path': 'TrainingModel/train_province_model.py',
        'phase': 4,
        'resource_level': 'heavy',  # ML training is resource intensive
        'depends_on': ['fact_province_content_engagement', 'fact_comment_nlp_engagement'],
        'description': 'Train province engagement prediction model using MLflow'
    },
}

# ============================================
# RESOURCE ALLOCATION PRESETS
# ============================================
# Based on Silver layer pattern
# spark-defaults.conf already sets:
#   - spark.executor.memory=1536m
#   - spark.executor.memoryOverhead=512m (total 2g per executor)
#   - spark.executor.cores=2
#   - spark.executor.instances=2
# Only override what's different from defaults

RESOURCE_PRESETS = {
    'light': {
        'spark.executor.instances': '1',  # Use 1 executor
        'spark.sql.shuffle.partitions': '20',
        'spark.sql.adaptive.enabled': 'true'
    },
    'medium': {
        'spark.executor.instances': '1',  # Still 1 executor but with adaptive query execution
        'spark.sql.shuffle.partitions': '20',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true'
    },
    'heavy': {
        'spark.executor.instances': '2',  # Use full cluster (2 workers)
        'spark.sql.shuffle.partitions': '20',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.iceberg.commit.timeout': '300000',  # 5 mins for large batches
        'spark.hadoop.hive.metastore.client.socket.timeout': '600000'  # 10 mins
    }
}

# ============================================
# SPARK JOB PATHS
# ============================================

SPARK_JOB_BASE_PATH = '/opt/spark/jobs/gold'
