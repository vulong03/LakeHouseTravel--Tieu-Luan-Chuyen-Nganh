"""
Gold Layer Aggregation DAG (Template)
Create analytics-ready tables and metrics

TODO: Implement Gold aggregation jobs when ready
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from common.health_checks import check_docker_health
from common.notifications import log_dag_start, log_dag_complete
from gold.config import (
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

# TODO: Implement these tasks when Gold jobs are ready
aggregate_hotels = EmptyOperator(
    task_id='aggregate_hotel_ratings',
    dag=dag,
)

aggregate_tiktok = EmptyOperator(
    task_id='aggregate_tiktok_engagement',
    dag=dag,
)

build_recommendations = EmptyOperator(
    task_id='build_recommendation_table',
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

health_check >> start_task >> [aggregate_hotels, aggregate_tiktok] >> build_recommendations >> complete_task
