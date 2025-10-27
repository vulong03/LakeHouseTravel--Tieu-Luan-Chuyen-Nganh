"""
Bronze Layer Ingestion DAG
Orchestrates raw data ingestion with checksum-based deduplication
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# Import common utilities
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from common.spark_operators import SparkSubmitCommand
from common.health_checks import check_docker_health
from common.notifications import log_dag_start, log_dag_complete
from bronze.config import (
    DAG_ID, DESCRIPTION, TAGS, SCHEDULE_INTERVAL, 
    START_DATE, CATCHUP, DEFAULT_ARGS, BRONZE_JOBS
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

# Task 1: Log Start
start_task = PythonOperator(
    task_id='start_task',
    python_callable=log_dag_start,
    provide_context=True,
    dag=dag,
)

# Task 2: Initialize Tracking Table
init_tracking = BashOperator(
    task_id='init_tracking_table',
    bash_command="""
    docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db << 'EOF'
    CREATE TABLE IF NOT EXISTS file_ingestion_log (
        id SERIAL PRIMARY KEY,
        file_path TEXT NOT NULL,
        file_name TEXT NOT NULL,
        file_size_bytes BIGINT,
        file_checksum TEXT NOT NULL UNIQUE,
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        records_ingested INTEGER,
        table_name TEXT,
        status TEXT CHECK (status IN ('success', 'failed', 'in_progress')),
        error_message TEXT,
        ingestion_details JSONB,
        CONSTRAINT unique_file_checksum UNIQUE(file_checksum)
    );
    
    CREATE INDEX IF NOT EXISTS idx_file_checksum ON file_ingestion_log(file_checksum);
    CREATE INDEX IF NOT EXISTS idx_file_name ON file_ingestion_log(file_name);
    CREATE INDEX IF NOT EXISTS idx_table_name ON file_ingestion_log(table_name);
    CREATE INDEX IF NOT EXISTS idx_ingestion_timestamp ON file_ingestion_log(ingestion_timestamp);
    
    COMMENT ON COLUMN file_ingestion_log.ingestion_details IS 'Optional JSONB for multi-table ingestion breakdown (e.g., TikTok comments split into 2 tables)';
EOF
    """,
    dag=dag,
)

# ============================================
# Bronze Ingestion Tasks
# ============================================

# TikTok
ingest_tiktok_videos = BashOperator(
    task_id='ingest_tiktok_videos',
    bash_command=SparkSubmitCommand.bronze_job(BRONZE_JOBS['tiktok_videos']),
    dag=dag,
)

ingest_tiktok_comments = BashOperator(
    task_id='ingest_tiktok_comments',
    bash_command=SparkSubmitCommand.bronze_job(BRONZE_JOBS['tiktok_comments']),
    dag=dag,
)

# Booking.com
ingest_booking_list = BashOperator(
    task_id='ingest_booking_list',
    bash_command=SparkSubmitCommand.bronze_job(BRONZE_JOBS['booking_list']),
    dag=dag,
)

ingest_booking_detail = BashOperator(
    task_id='ingest_booking_detail',
    bash_command=SparkSubmitCommand.bronze_job(BRONZE_JOBS['booking_detail']),
    dag=dag,
)

ingest_booking_reviews = BashOperator(
    task_id='ingest_booking_reviews',
    bash_command=SparkSubmitCommand.bronze_job(BRONZE_JOBS['booking_reviews']),
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

health_check >> start_task >> init_tracking

# TikTok pipeline (sequential)
init_tracking >> ingest_tiktok_videos >> ingest_tiktok_comments

# Booking.com pipeline (parallel)
init_tracking >> [ingest_booking_list, ingest_booking_detail, ingest_booking_reviews]

# Complete
[
    ingest_tiktok_comments,
    ingest_booking_list,
    ingest_booking_detail,
    ingest_booking_reviews
] >> complete_task
