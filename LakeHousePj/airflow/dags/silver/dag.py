"""
Silver Layer Transformation DAG
Transform Bronze CSV files into Silver Iceberg tables with UPSERT/APPEND logic

Features:
- UPSERT mode for hotels_detail, hotels_list, tiktok_videos (latest file only)
- APPEND mode for hotels_reviews, tiktok_comments (multi-file incremental)
- File size tracking in PostgreSQL
- Row-level checksum deduplication
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from common.spark_operators import SparkSubmitCommand
from common.health_checks import check_docker_health
from common.notifications import log_dag_start, log_dag_complete
from silver.config import (
    DAG_ID, DESCRIPTION, TAGS, SCHEDULE_INTERVAL,
    START_DATE, CATCHUP, DEFAULT_ARGS, SILVER_JOBS
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
# Tasks
# ============================================

# Task 0: Health Check
health_check = ShortCircuitOperator(
    task_id='check_docker_health',
    python_callable=check_docker_health,
    provide_context=True,
    dag=dag,
)

# Task 1: Start
start_task = PythonOperator(
    task_id='start_task',
    python_callable=log_dag_start,
    provide_context=True,
    dag=dag,
)

# ============================================
# Silver Transformation Tasks
# ============================================

# Booking.com Hotels (UPSERT mode)
transform_hotels_detail = BashOperator(
    task_id='transform_hotels_detail',
    bash_command=SparkSubmitCommand.silver_job(SILVER_JOBS['transform_hotels_detail']),
    dag=dag,
)

transform_hotels_list = BashOperator(
    task_id='transform_hotels_list',
    bash_command=SparkSubmitCommand.silver_job(SILVER_JOBS['transform_hotels_list']),
    dag=dag,
)

transform_hotels_reviews = BashOperator(
    task_id='transform_hotels_reviews',
    bash_command=SparkSubmitCommand.silver_job(SILVER_JOBS['transform_hotels_reviews']),
    dag=dag,
)

# TikTok (UPSERT for videos, APPEND for comments)
transform_tiktok_videos = BashOperator(
    task_id='transform_tiktok_videos',
    bash_command=SparkSubmitCommand.silver_job(SILVER_JOBS['transform_tiktok_videos']),
    dag=dag,
)

transform_tiktok_comments = BashOperator(
    task_id='transform_tiktok_comments',
    bash_command=SparkSubmitCommand.silver_job(SILVER_JOBS['transform_tiktok_comments']),
    dag=dag,
)

# Task: Complete
complete_task = PythonOperator(
    task_id='complete_task',
    python_callable=log_dag_complete,
    provide_context=True,
    dag=dag,
)

# ============================================
# Dependencies
# ============================================

health_check >> start_task

# Booking.com pipeline (parallel)
start_task >> [transform_hotels_detail, transform_hotels_list, transform_hotels_reviews]

# TikTok pipeline (videos first, then comments)
start_task >> transform_tiktok_videos >> transform_tiktok_comments

# Complete
[
    transform_hotels_detail,
    transform_hotels_list,
    transform_hotels_reviews,
    transform_tiktok_comments
] >> complete_task
