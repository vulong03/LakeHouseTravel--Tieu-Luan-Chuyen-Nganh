"""
Data Quality DAG Configuration — Hotel Group
"""

from datetime import datetime, timedelta

# ============================================
# DAG Metadata
# ============================================
DAG_ID            = 'hotel_data_quality'
DESCRIPTION       = 'Sequential DQ checks: hotels_list → hotels_detail → hotels_reviews (Silver layer)'
TAGS              = ['dq', 'silver', 'hotel', 'data-quality']
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
    'retries': 0,          # DQ fail là deterministic — retry vô ích
    'execution_timeout': timedelta(hours=1),
}

# ============================================
# Spark Job Paths — mỗi bảng 1 subfolder riêng
# ============================================
SPARK_JOB_BASE_PATH = '/opt/spark/jobs/dataqualify'

DQ_JOBS = [
    {
        'task_id':     'run_hotel_list_dq',
        'job_path':    'hotel_list/hotel_list_dq_check.py',
        'description': 'Check hotels_list quality',
    },
    {
        'task_id':     'run_hotel_detail_dq',
        'job_path':    'hotel_detail/hotel_detail_dq_check.py',
        'description': 'Check hotels_detail quality',
    },
    {
        'task_id':     'run_hotel_reviews_dq',
        'job_path':    'hotel_reviews/hotel_reviews_dq_check.py',
        'description': 'Check hotels_reviews quality',
    },
]

# ============================================
# Resource Config (light — chỉ đọc Silver)
# ============================================
RESOURCE_PRESET = {
    'spark.executor.instances': '1',
    'spark.executor.memory': '1536m',
    'spark.sql.shuffle.partitions': '10',
    'spark.sql.adaptive.enabled': 'true',
}
