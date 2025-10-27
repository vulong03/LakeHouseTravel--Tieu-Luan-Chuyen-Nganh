"""
Silver Layer Transformation DAG (Template)
Clean, validate, and enrich Bronze data into Silver layer

TODO: Implement Silver transformation jobs when ready
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
from silver.config import (
    DAG_ID, DESCRIPTION, TAGS, SCHEDULE_INTERVAL,
    START_DATE, CATCHUP, DEFAULT_ARGS
)

# ============================================
# DAG Definition
# ============================================

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    tags=TAGS,
)

# ============================================
# Tasks (Placeholder)
# ============================================

# Health Check
health_check = ShortCircuitOperator(
    task_id='check_docker_health',
    python_callable=check_docker_health,
    provide_context=True,
    dag=dag,
)

# Start
start_task = PythonOperator(
    task_id='start_task',
    python_callable=log_dag_start,
    provide_context=True,
    dag=dag,
)

# TODO: Implement these tasks when Silver jobs are ready
clean_tiktok = EmptyOperator(
    task_id='clean_tiktok_data',
    dag=dag,
)

clean_booking = EmptyOperator(
    task_id='clean_booking_data',
    dag=dag,
)

enrich_data = EmptyOperator(
    task_id='enrich_data',
    dag=dag,
)

# Complete
complete_task = PythonOperator(
    task_id='complete_task',
    python_callable=log_dag_complete,
    provide_context=True,
    dag=dag,
)

# ============================================
# Dependencies
# ============================================

health_check >> start_task >> [clean_tiktok, clean_booking] >> enrich_data >> complete_task
