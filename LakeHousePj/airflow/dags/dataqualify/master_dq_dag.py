"""
Master Data Quality DAG — LakeHouse Silver Layer
================================================
Gộp chung tất cả các nhóm DQ (Hotel, TikTok) vào một DAG duy nhất.
Sử dụng TaskGroup để phân loại trên giao diện Airflow UI.

Luồng chạy:
  - Health Check & Start
  - [TaskGroup: Hotel_DQ] --- (Sequential)
  - [TaskGroup: TikTok_DQ] --- (Sequential)
  - Cả 2 nhóm này chạy SONG SONG với nhau
  - Complete
"""

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

import sys
import os
from datetime import datetime, timedelta

# 1. Thêm đường dẫn để import common và các module con
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__))) # airflow/dags/
from common.spark_operators import SparkSubmitCommand
from common.health_checks import check_docker_health
from common.notifications import log_dag_start, log_dag_complete

# 2. Import config từ các folder con
sys.path.insert(0, os.path.dirname(__file__)) # airflow/dags/dataqualify/
from hotel.config import DQ_JOBS as HOTEL_JOBS, RESOURCE_PRESET as HOTEL_RESOURCES
from tiktok.config import DQ_JOBS as TIKTOK_JOBS, RESOURCE_PRESET as TIKTOK_RESOURCES

# ============================================
# DAG GLOBAL CONFIG
# ============================================
DAG_ID = 'master_data_quality'
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
    'execution_timeout': timedelta(hours=2),
}

SPARK_JOB_BASE_PATH = '/opt/spark/jobs/DataQualify'

# ============================================
# Helper Functions
# ============================================

def build_spark_command(job_path, resource_preset):
    """Build Spark submit command cho DQ job"""
    spark_conf = resource_preset.copy()
    job_name = job_path.replace('/', '_').replace('.py', '')
    spark_conf['spark.app.name'] = f'DQ_{job_name}'

    extra_conf = ' '.join([f'--conf {k}={v}' for k, v in spark_conf.items()])

    return SparkSubmitCommand.build(
        job_path=f'{SPARK_JOB_BASE_PATH}/{job_path}',
        bucket='silver',
        extra_conf=extra_conf,
        args=''
    )

# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Unified Data Quality Pipeline for Hotel and TikTok datasets',
    schedule_interval=None,
    catchup=False,
    tags=['dq', 'silver', 'unified', 'master'],
) as dag:

    # --- Pre-flight ---
    health_check = ShortCircuitOperator(
        task_id='check_docker_health',
        python_callable=check_docker_health,
    )

    start = PythonOperator(
        task_id='start_pipeline',
        python_callable=log_dag_start,
    )

    # --- Group 1: Hotel DQ (Sequential inside group) ---
    with TaskGroup(group_id='hotel_quality_checks') as hotel_group:
        prev_task = None
        for job in HOTEL_JOBS:
            task = BashOperator(
                task_id=job['task_id'],
                bash_command=build_spark_command(job['job_path'], HOTEL_RESOURCES),
            )
            if prev_task:
                prev_task >> task
            prev_task = task

    # --- Group 2: TikTok DQ (Sequential inside group) ---
    with TaskGroup(group_id='tiktok_quality_checks') as tiktok_group:
        prev_task = None
        for job in TIKTOK_JOBS:
            task = BashOperator(
                task_id=job['task_id'],
                bash_command=build_spark_command(job['job_path'], TIKTOK_RESOURCES),
            )
            if prev_task:
                prev_task >> task
            prev_task = task

    # --- Post-flight ---
    complete = PythonOperator(
        task_id='complete_pipeline',
        python_callable=log_dag_complete,
    )

    # --- Flow Definition ---
    # Health check và Start chạy trước
    health_check >> start
    
    # Hai nhóm DQ chạy SONG SONG với nhau sau khi start
    start >> hotel_group
    start >> tiktok_group
    
    # Sau khi cả 2 nhóm xong thì kết thúc
    hotel_group >> complete
    tiktok_group >> complete
