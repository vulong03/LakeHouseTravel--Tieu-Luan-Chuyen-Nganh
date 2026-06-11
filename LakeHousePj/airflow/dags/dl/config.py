"""
DL Layer DAG Configuration
Manages DL model training pipelines and decoupled LSTM training.
"""

from datetime import datetime, timedelta

# ============================================
# DAG METADATA
# ============================================

DAG_ID_PHOBERT_PIPELINE = 'dl_phobert_training_pipeline'
DESCRIPTION_PHOBERT_PIPELINE = 'PhoBERT Training Pipeline: Weak Labeling -> Train PhoBERT -> PhoBERT Inference'
TAGS_PHOBERT_PIPELINE = ['dl', 'training', 'phobert', 'pipeline']

DAG_ID_LSTM = 'dl_lstm_training_dag'
DESCRIPTION_LSTM = 'DL Pipeline: Train LSTM model to forecast province hotel review volume'
TAGS_LSTM = ['dl', 'training', 'lstm', 'forecasting']

DAG_ID_GRU = 'dl_gru_training_dag'
DESCRIPTION_GRU = 'DL Pipeline: Train GRU model to forecast province hotel review volume'
TAGS_GRU = ['dl', 'training', 'gru', 'forecasting']

# ============================================
# SCHEDULE
# ============================================

SCHEDULE_INTERVAL = None  # Manual trigger only
START_DATE = datetime(2025, 1, 1)
CATCHUP = False

# ============================================
# TASK CONFIGURATION
# ============================================

DEFAULT_ARGS = {
    'owner': 'dlops-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': None,
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
# DL JOBS CONFIGURATION
# ============================================

DL_JOBS = {
    'weak_labeling': {
        'job_path': '/opt/spark/jobs/dl/nlp/weak_labeling.py',
        'resource_level': 'heavy',
        'description': 'Apply rule-based signals to weakly label TikTok comments'
    },
    'train_phobert': {
        'job_path': '/opt/spark/jobs/dl/nlp/train_phobert.py',
        'resource_level': 'heavy',
        'description': 'Fine-tune multi-task PhoBERT on weakly labeled comments'
    },
    'inference_phobert': {
        'job_path': '/opt/spark/jobs/dl/nlp/inference_phobert.py',
        'resource_level': 'heavy',
        'description': 'Run PhoBERT inference on all comments and save to Iceberg'
    },
    'train_lstm_forecast': {
        'job_path': '/opt/spark/jobs/dl/train_province_lstm_v5.py',
        'resource_level': 'heavy',
        'description': 'Train LSTM model on Gold features to forecast hotel review volume'
    },
    'train_gru_forecast': {
        'job_path': '/opt/spark/jobs/dl/train_gru_forecast.py',
        'resource_level': 'heavy',
        'description': 'Train GRU model on Gold features to forecast hotel review volume'
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
