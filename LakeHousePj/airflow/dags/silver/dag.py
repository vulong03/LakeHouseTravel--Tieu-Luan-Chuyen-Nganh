"""
Silver Layer Transformation DAG - REFACTORED
2-step pipeline per dataset: Transform → Clean & Load
2-phase execution: Light jobs parallel → Heavy jobs sequential

Phase 1 (PARALLEL): hotels_detail, hotels_list, tiktok_videos
Phase 2 (SEQUENTIAL): hotels_reviews → tiktok_comments (full resources each)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from common.spark_operators import SparkSubmitCommand
from common.health_checks import check_docker_health
from common.notifications import log_dag_start, log_dag_complete
from silver.config import (
    DAG_ID, DESCRIPTION, TAGS, SCHEDULE_INTERVAL,
    START_DATE, CATCHUP, DEFAULT_ARGS, SILVER_JOBS,
    SPARK_JOB_BASE_PATH, RESOURCE_PRESETS
)

# ============================================
# Helper Functions
# ============================================

def build_spark_command(job_path, resource_level='light', custom_conf=None):
    """
    Build Spark submit command with resource configuration
    Uses SparkSubmitCommand.build() with proper parameters
    
    Args:
        job_path: Relative path to job (e.g., 'hotels_detail/step_01_transform.py')
        resource_level: 'light' or 'heavy'
        custom_conf: Additional Spark configurations
    
    Returns:
        Complete spark-submit command string
    """
    # Get resource preset
    spark_conf = RESOURCE_PRESETS.get(resource_level, RESOURCE_PRESETS['light']).copy()
    
    # Override with custom config if provided
    if custom_conf:
        spark_conf.update(custom_conf)
    
    # Build extra conf string for SparkSubmitCommand
    extra_conf_parts = [f"--conf {k}={v}" for k, v in spark_conf.items()]
    
    # Add app name
    job_name = job_path.replace('/', '_').replace('.py', '')
    extra_conf_parts.append(f"--conf spark.app.name=Silver_{job_name}")
    
    # NOTE: Other configs (driver/executor memory, S3 credentials, etc.) 
    # are already set in spark-defaults.conf and will be used by default
    
    extra_conf = ' '.join(extra_conf_parts)
    
    # Use SparkSubmitCommand.build() from common module
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
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    tags=TAGS,
) as dag:
    
    # ============================================
    # Pre-flight Tasks
    # ============================================
    
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
    
    # Initialize tracking table (shared with Bronze layer)
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
        """
    )
    
    # ============================================
    # PHASE 1: Light Jobs (Parallel)
    # ============================================
    
    phase1_tasks = []
    
    for dataset_name in ['hotels_detail', 'hotels_list', 'tiktok_videos']:
        job_config = SILVER_JOBS[dataset_name]
        
        with TaskGroup(group_id=f"{dataset_name}_pipeline") as tg:
            
            # Step 1: Transform
            step1 = BashOperator(
                task_id='step_01_transform',
                bash_command=build_spark_command(
                    job_config['step1'],
                    resource_level=job_config['resource_level']
                )
            )
            
            # Step 2: Clean & Load
            step2 = BashOperator(
                task_id='step_02_clean_load',
                bash_command=build_spark_command(
                    job_config['step2'],
                    resource_level=job_config['resource_level']
                )
            )
            
            # Pipeline: Transform → Load
            step1 >> step2
        
        phase1_tasks.append(tg)
    
    # Phase 1 barrier
    wait_phase1 = EmptyOperator(
        task_id='wait_phase1_complete'
    )
    
    # ============================================
    # PHASE 2: Heavy Job #1 - Hotels Reviews
    # ============================================
    
    with TaskGroup(group_id='hotels_reviews_pipeline') as hotels_reviews_tg:
        
        job_config = SILVER_JOBS['hotels_reviews']
        
        reviews_step1 = BashOperator(
            task_id='step_01_transform',
            bash_command=build_spark_command(
                job_config['step1'],
                resource_level=job_config['resource_level'],
                custom_conf=job_config.get('spark_conf')
            )
        )
        
        reviews_step2 = BashOperator(
            task_id='step_02_clean_load',
            bash_command=build_spark_command(
                job_config['step2'],
                resource_level=job_config['resource_level'],
                custom_conf=job_config.get('spark_conf')
            )
        )
        
        reviews_step1 >> reviews_step2
    
    # Reviews barrier
    wait_reviews = EmptyOperator(
        task_id='wait_reviews_complete'
    )
    
    # ============================================
    # PHASE 2: Heavy Job #2 - TikTok Comments
    # ============================================
    
    with TaskGroup(group_id='tiktok_comments_pipeline') as tiktok_comments_tg:
        
        job_config = SILVER_JOBS['tiktok_comments']
        
        comments_step1 = BashOperator(
            task_id='step_01_transform',
            bash_command=build_spark_command(
                job_config['step1'],
                resource_level=job_config['resource_level'],
                custom_conf=job_config.get('spark_conf')
            )
        )
        
        comments_step2 = BashOperator(
            task_id='step_02_clean_load',
            bash_command=build_spark_command(
                job_config['step2'],
                resource_level=job_config['resource_level'],
                custom_conf=job_config.get('spark_conf')
            )
        )
        
        comments_step1 >> comments_step2
    
    # ============================================
    # Completion Task
    # ============================================
    
    complete = PythonOperator(
        task_id='complete_dag',
        python_callable=log_dag_complete,
        provide_context=True
    )
    
    # ============================================
    # DAG Flow (2-Phase Strategy)
    # ============================================
    
    # Pre-flight
    health_check >> start >> init_tracking
    
    # Phase 1: All light jobs in parallel
    init_tracking >> phase1_tasks >> wait_phase1
    
    # Phase 2: Heavy jobs sequential (isolated resources)
    wait_phase1 >> hotels_reviews_tg >> wait_reviews
    wait_reviews >> tiktok_comments_tg
    
    # Complete
    tiktok_comments_tg >> complete
