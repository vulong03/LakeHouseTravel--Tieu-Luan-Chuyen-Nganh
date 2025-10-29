"""
Bronze Layer RAW Ingestion DAG
Purpose: Ingest raw CSV files from /data/raw to MinIO s3://bronze with UTF-8 encoding
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
    START_DATE, CATCHUP, DEFAULT_ARGS, BRONZE_RAW_JOBS
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

# Task 2: Initialize Tracking Table (if not exists)
init_tracking = BashOperator(
    task_id='init_tracking_table',
    bash_command="""
    docker exec -i lakehouse_postgres psql -U lakehouse_user -d metastore_db << 'EOF'
    CREATE TABLE IF NOT EXISTS file_ingestion_log (
        id SERIAL PRIMARY KEY,
        file_path TEXT NOT NULL,
        file_name TEXT NOT NULL,
        file_size_bytes BIGINT,
        file_checksum TEXT NOT NULL,
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        records_ingested INTEGER,
        table_name TEXT,
        layer TEXT,
        status TEXT CHECK (status IN ('success', 'failed', 'in_progress')),
        error_message TEXT,
        ingestion_details JSONB,
        CONSTRAINT unique_file_checksum_layer UNIQUE(file_checksum, layer)
    );
    
    CREATE INDEX IF NOT EXISTS idx_file_checksum ON file_ingestion_log(file_checksum);
    CREATE INDEX IF NOT EXISTS idx_layer ON file_ingestion_log(layer);
    CREATE INDEX IF NOT EXISTS idx_table_name ON file_ingestion_log(table_name);
    CREATE INDEX IF NOT EXISTS idx_ingestion_timestamp ON file_ingestion_log(ingestion_timestamp);
EOF
    """,
    dag=dag,
)

# ============================================
# Bronze RAW Ingestion Tasks
# ============================================

# Booking.com - Hotels List
ingest_booking_list = BashOperator(
    task_id='ingest_booking_list',
    bash_command=SparkSubmitCommand.bronze_raw_job(BRONZE_RAW_JOBS['booking_list']),
    dag=dag,
)

# Booking.com - Hotels Detail
ingest_booking_detail = BashOperator(
    task_id='ingest_booking_detail',
    bash_command=SparkSubmitCommand.bronze_raw_job(BRONZE_RAW_JOBS['booking_detail']),
    dag=dag,
)

# Booking.com - Hotels Reviews (1M+ records)
ingest_booking_reviews = BashOperator(
    task_id='ingest_booking_reviews',
    bash_command=SparkSubmitCommand.bronze_raw_job(BRONZE_RAW_JOBS['booking_reviews']),
    dag=dag,
)

# TikTok - Videos
ingest_tiktok_videos = BashOperator(
    task_id='ingest_tiktok_videos',
    bash_command=SparkSubmitCommand.bronze_raw_job(BRONZE_RAW_JOBS['tiktok_videos']),
    dag=dag,
)

# TikTok - Comments (Batch mode - 6 files)
ingest_tiktok_comments = BashOperator(
    task_id='ingest_tiktok_comments',
    bash_command=SparkSubmitCommand.bronze_raw_job(BRONZE_RAW_JOBS['tiktok_comments']),
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

# Health check first
health_check >> start_task >> init_tracking

# Booking.com pipeline (parallel - independent)
init_tracking >> [
    ingest_booking_list,
    ingest_booking_detail,
    ingest_booking_reviews,
]

# TikTok pipeline (videos → comments sequential for better logging)
init_tracking >> ingest_tiktok_videos >> ingest_tiktok_comments

# All complete
[
    ingest_booking_list,
    ingest_booking_detail,
    ingest_booking_reviews,
    ingest_tiktok_comments,  # tiktok_videos already upstream
] >> complete_task
