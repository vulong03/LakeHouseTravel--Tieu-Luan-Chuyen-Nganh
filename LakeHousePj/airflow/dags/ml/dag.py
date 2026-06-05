"""
ML Layer LSTM Training DAG
==========================
Decoupled training pipeline for LSTM model, triggered automatically 
by the Gold DAG or run manually on-demand.
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
from ml.config import (
    DAG_ID, DESCRIPTION, TAGS, SCHEDULE_INTERVAL,
    START_DATE, CATCHUP, DEFAULT_ARGS, ML_JOBS,
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
    extra_conf_parts.append(f"--conf spark.app.name=ML_LSTM_Training")
    extra_conf_parts.append("--conf spark.sql.defaultCatalog=gold")
    
    extra_conf = ' '.join(extra_conf_parts)
    
    return SparkSubmitCommand.build(
        job_path=job_path,
        bucket='gold',
        extra_conf=extra_conf,
        args=''
    )

# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    tags=TAGS,
) as dag:
    
    # Pre-flight Tasks
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
    
    # ML LSTM Training Job
    train_lstm_model = BashOperator(
        task_id='train_lstm_forecast',
        bash_command=build_spark_command(
            ML_JOBS['train_lstm_forecast']['job_path'],
            resource_level=ML_JOBS['train_lstm_forecast']['resource_level']
        )
    )
    
    # Completion Task
    complete = PythonOperator(
        task_id='complete_dag',
        python_callable=log_dag_complete,
        provide_context=True
    )
    
    # Flow
    health_check >> start >> train_lstm_model >> complete
