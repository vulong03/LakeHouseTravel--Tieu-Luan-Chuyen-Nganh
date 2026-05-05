"""
Data Quality DAG Configuration — TikTok Group
"""

from datetime import datetime, timedelta

# ============================================
# DAG Metadata
# ============================================
DAG_ID            = 'tiktok_data_quality'
DESCRIPTION       = 'Sequential DQ checks: tiktok_videos → tiktok_post_metadata → tiktok_post_comments (Silver layer)'
TAGS              = ['dq', 'silver', 'tiktok', 'data-quality']
SCHEDULE_INTERVAL = None
START_DATE        = datetime(2025, 1, 1)
CATCHUP           = False

# ============================================
# Task Configuration
# ============================================
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'execution_timeout': timedelta(hours=1),
}

# ============================================
# Spark Job Paths
# ============================================
SPARK_JOB_BASE_PATH = '/opt/spark/jobs/DataQualify'

DQ_JOBS = [
    {
        'task_id':     'run_tiktok_videos_dq',
        'job_path':    'tiktok_videos/tiktok_videos_dq_check.py',
        'description': 'Check tiktok_videos quality',
    },
    {
        'task_id':     'run_tiktok_post_metadata_dq',
        'job_path':    'tiktok_post_metadata/tiktok_post_metadata_dq_check.py',
        'description': 'Check tiktok_post_metadata quality',
    },
    {
        'task_id':     'run_tiktok_post_comments_dq',
        'job_path':    'tiktok_post_comments/tiktok_post_comments_dq_check.py',
        'description': 'Check tiktok_post_comments quality',
    }
]

# ============================================
# Resource Config
# ============================================
RESOURCE_PRESET = {
    'spark.executor.instances': '1',
    'spark.executor.memory': '1536m',
    'spark.sql.shuffle.partitions': '10',
    'spark.sql.adaptive.enabled': 'true',
}
