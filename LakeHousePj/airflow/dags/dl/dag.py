"""
DL Layer DAGs
- dl_phobert_training_pipeline: End-to-End manual retraining of PhoBERT model
- dl_lstm_training_dag: LSTM model training triggered by Gold DAG or run manually
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from common.spark_operators import SparkSubmitCommand
from common.health_checks import check_docker_health
from common.notifications import log_dag_start, log_dag_complete
from dl.config import (
    DAG_ID_PHOBERT_PIPELINE, DAG_ID_LSTM, DESCRIPTION_PHOBERT_PIPELINE, DESCRIPTION_LSTM,
    TAGS_PHOBERT_PIPELINE, TAGS_LSTM, SCHEDULE_INTERVAL,
    START_DATE, CATCHUP, DEFAULT_ARGS, DL_JOBS,
    RESOURCE_PRESETS
)

# ============================================
# Helper Functions
# ============================================

def build_spark_command(job_path, resource_level='heavy', custom_conf=None):
    """
    Build Spark submit command with resource configuration
    """
    spark_conf = RESOURCE_PRESETS.get(resource_level, RESOURCE_PRESETS['heavy']).copy()
    
    if custom_conf:
        spark_conf.update(custom_conf)
    
    extra_conf_parts = [f"--conf {k}={v}" for k, v in spark_conf.items()]
    extra_conf_parts.append(f"--conf spark.app.name=DL_Job")
    extra_conf_parts.append("--conf spark.sql.defaultCatalog=gold")
    
    extra_conf = ' '.join(extra_conf_parts)
    
    return SparkSubmitCommand.build(
        job_path=job_path,
        bucket='gold',
        extra_conf=extra_conf,
        args=''
    )

# ============================================
# DAG 1: PhoBERT Retraining Pipeline
# ============================================

with DAG(
    dag_id=DAG_ID_PHOBERT_PIPELINE,
    default_args=DEFAULT_ARGS,
    description=DESCRIPTION_PHOBERT_PIPELINE,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    tags=TAGS_PHOBERT_PIPELINE,
) as dag_pipeline:
    
    # Pre-flight
    health_check = ShortCircuitOperator(
        task_id='check_docker_health',
        python_callable=check_docker_health,
        provide_context=True
    )
    
    start = PythonOperator(
        task_id='start_dag',
        python_callable=log_dag_start,
        provide_context=True
    )
    
    # 1. Weak Labeling
    weak_labeling = BashOperator(
        task_id='weak_labeling_step',
        bash_command=build_spark_command(
            DL_JOBS['weak_labeling']['job_path'],
            resource_level=DL_JOBS['weak_labeling']['resource_level']
        )
    )

    # 2. Train PhoBERT
    train_phobert = BashOperator(
        task_id='train_phobert_step',
        bash_command=build_spark_command(
            DL_JOBS['train_phobert']['job_path'],
            resource_level=DL_JOBS['train_phobert']['resource_level']
        )
    )

    # 3. PhoBERT Inference
    inference_phobert = BashOperator(
        task_id='inference_phobert_step',
        bash_command=build_spark_command(
            DL_JOBS['inference_phobert']['job_path'],
            resource_level=DL_JOBS['inference_phobert']['resource_level']
        )
    )
    
    # Completion
    complete = PythonOperator(
        task_id='complete_dag',
        python_callable=log_dag_complete,
        provide_context=True
    )
    
    # Pipeline dependency flow
    health_check >> start >> weak_labeling >> train_phobert >> inference_phobert >> complete


# ============================================
# DAG 2: Decoupled LSTM Training Only
# ============================================

with DAG(
    dag_id=DAG_ID_LSTM,
    default_args=DEFAULT_ARGS,
    description=DESCRIPTION_LSTM,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    tags=TAGS_LSTM,
) as dag_lstm:
    
    # Pre-flight
    health_check_lstm = ShortCircuitOperator(
        task_id='check_docker_health',
        python_callable=check_docker_health,
        provide_context=True
    )
    
    start_lstm = PythonOperator(
        task_id='start_dag',
        python_callable=log_dag_start,
        provide_context=True
    )
    
    # Train LSTM Model (Only)
    train_lstm_model = BashOperator(
        task_id='train_lstm_forecast',
        bash_command=build_spark_command(
            DL_JOBS['train_lstm_forecast']['job_path'],
            resource_level=DL_JOBS['train_lstm_forecast']['resource_level']
        )
    )
    
    # Completion
    complete_lstm = PythonOperator(
        task_id='complete_dag',
        python_callable=log_dag_complete,
        provide_context=True
    )
    
    # LSTM only dependency flow
    health_check_lstm >> start_lstm >> train_lstm_model >> complete_lstm
