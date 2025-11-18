"""
Silver Layer DAG Configuration
2-Phase Execution: Light jobs parallel → Heavy jobs sequential
"""

from datetime import datetime, timedelta

# DAG metadata
DAG_ID = 'silver_layer_transformation'
DESCRIPTION = 'Silver transformation: 3 light jobs parallel → 2 heavy jobs sequential'
TAGS = ['silver', 'transformation', '2-phase', 'resource-optimized']

# Schedule
SCHEDULE_INTERVAL = None  # Manual trigger
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
    'lakehouse_spark_worker_1',
    'lakehouse_postgres',
    'lakehouse_minio',
    'lakehouse_hive_metastore'
]

# ============================================
# SILVER JOBS CONFIGURATION (2-Phase Strategy)
# ============================================

SILVER_JOBS = {
    # ===================================
    # PHASE 1: Light Jobs (PARALLEL - 3 jobs)
    # ===================================
    'hotels_detail': {
        'step1': 'hotels_detail/step_01_transform.py',
        'step2': 'hotels_detail/step_02_clean_load.py',
        'mode': 'UPSERT',
        'phase': 1,
        'resource_level': 'light',
        'depends_on': [],  # No dependencies - run immediately
        'description': 'Transform hotel detail snapshots'
    },
    
    'hotels_list': {
        'step1': 'hotels_list/step_01_transform.py',
        'step2': 'hotels_list/step_02_clean_load.py',
        'mode': 'UPSERT',
        'phase': 1,
        'resource_level': 'light',
        'depends_on': [],  # No dependencies - run immediately
        'description': 'Transform hotel list snapshots'
    },
    
    'tiktok_videos': {
        'step1': 'tiktok_videos/step_01_transform.py',
        'step2': 'tiktok_videos/step_02_clean_load.py',
        'mode': 'UPSERT',
        'phase': 1,
        'resource_level': 'light',
        'depends_on': [],  # No dependencies - run immediately
        'description': 'Transform TikTok video metadata'
    },
    
    # ===================================
    # PHASE 2: Heavy Job #1 (Isolated - 100% resources)
    # ===================================
    'hotels_reviews': {
        'step1': 'hotels_reviews/step_01_transform.py',
        'step2': 'hotels_reviews/step_02_clean_load.py',
        'mode': 'APPEND',
        'phase': 2,
        'resource_level': 'heavy',
        'depends_on': ['hotels_detail', 'hotels_list', 'tiktok_videos'],  # Wait Phase 1
        'description': 'Transform hotel reviews (HEAVY - millions of rows)',
        'spark_conf': {}  # Use defaults from spark-defaults.conf
    },
    
    # ===================================
    # PHASE 2: Heavy Job #2 (Isolated - 100% resources)
    # ===================================
    'tiktok_comments': {
        'step1': 'tiktok_comments/step_01_transform.py',
        'step2': 'tiktok_comments/step_02_clean_load.py',
        'mode': 'APPEND',
        'phase': 2,
        'resource_level': 'heavy',
        'depends_on': ['hotels_reviews'],  # Wait for reviews to finish
        'description': 'Transform TikTok comments (HEAVY - millions of rows)',
        'spark_conf': {}  # Use defaults from spark-defaults.conf
    }
}

# ============================================
# RESOURCE ALLOCATION PRESETS
# ============================================
# NOTE: spark-defaults.conf already sets:
#   - spark.executor.memory=1536m
#   - spark.executor.memoryOverhead=512m (total 2g per executor)
#   - spark.executor.cores=2
#   - spark.executor.instances=2
# Only override what's different from defaults

RESOURCE_PRESETS = {
    'light': {
        'spark.executor.instances': '1',  # Use 1 executor (save resources)
        'spark.sql.shuffle.partitions': '20',  # Match spark-defaults.conf
        'spark.sql.adaptive.enabled': 'true'
    },
    'heavy': {
        'spark.executor.instances': '2',  # Use full cluster (2 workers)
        'spark.sql.shuffle.partitions': '20',  # Match spark-defaults.conf for consistency
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.iceberg.commit.timeout': '300000',  # 5 mins for large batches
        'spark.hadoop.hive.metastore.client.socket.timeout': '600000'  # 10 mins
    }
}

# ============================================
# SPARK JOB PATHS
# ============================================

SPARK_JOB_BASE_PATH = '/opt/spark/jobs/silver'

# ============================================
# VALIDATION THRESHOLDS
# ============================================

VALIDATION_CONFIG = {
    'min_rows': {
        'hotels_detail': 100,
        'hotels_list': 100,
        'hotels_reviews': 1000,
        'tiktok_videos': 100,
        'tiktok_comments': 1000
    },
    'max_null_percentage': 0.05,  # 5% max nulls in critical columns
}
