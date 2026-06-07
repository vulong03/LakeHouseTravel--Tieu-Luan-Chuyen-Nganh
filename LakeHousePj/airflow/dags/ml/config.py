"""
ML Layer DAG Configuration
Decoupled from Gold ETL DAG, manages ML model training pipelines.
"""

from datetime import datetime, timedelta

# ============================================
# DAG METADATA
# ============================================

DAG_ID = 'ml_lstm_training_dag'
DESCRIPTION = 'ML Pipeline: Train LSTM model to forecast province hotel review volume'
TAGS = ['ml', 'training', 'lstm', 'forecasting']

# ============================================
# SCHEDULE
# ============================================

SCHEDULE_INTERVAL = None  # Manual trigger or triggered by gold_layer_aggregation DAG
START_DATE = datetime(2025, 1, 1)
CATCHUP = False

# ============================================
# TASK CONFIGURATION
# ============================================

DEFAULT_ARGS = {
    'owner': 'mlops-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
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
# ML JOBS CONFIGURATION
# ============================================

ML_JOBS = {
    'train_lstm_forecast': {
        'job_path': '/opt/spark/jobs/dl/train_lstm_forecast.py',
        'resource_level': 'heavy',
        'description': 'Train LSTM model on Gold features to forecast hotel review volume'
    }
}

# Resource Allocation presets
RESOURCE_PRESETS = {
    'heavy': {
        'spark.executor.instances': '2',
        'spark.sql.shuffle.partitions': '20',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    }
}
