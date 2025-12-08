"""
Gold Layer Aggregation DAG
===========================
4-Phase execution strategy based on dependencies:
  Phase 1: Common dimensions (parallel)
  Phase 2a: TikTok pipeline (dim_author → dim_post → dim_comment)
  Phase 2b: Hotel pipeline (dim_room_type, dim_travel_type, dim_country → dim_hotel)
  Phase 3: Fact tables (sequential: content → hotel → comment_nlp)
  Phase 4: ML Training (train province engagement model)

Flow optimized for resource usage and dependency resolution.
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
from gold.config import (
    DAG_ID, DESCRIPTION, TAGS, SCHEDULE_INTERVAL,
    START_DATE, CATCHUP, DEFAULT_ARGS, GOLD_JOBS,
    SPARK_JOB_BASE_PATH, RESOURCE_PRESETS
)

# ============================================
# Helper Functions
# ============================================

def build_spark_command(job_path, resource_level='light', custom_conf=None):
    """
    Build Spark submit command with resource configuration
    
    Args:
        job_path: Relative path to job (e.g., 'dim_province/dim_province_job.py')
        resource_level: 'light', 'medium', or 'heavy'
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
    extra_conf_parts.append(f"--conf spark.app.name=Gold_{job_name}")
    
    # Set default catalog to gold
    extra_conf_parts.append("--conf spark.sql.defaultCatalog=gold")
    
    extra_conf = ' '.join(extra_conf_parts)
    
    # Use SparkSubmitCommand.build() from common module
    return SparkSubmitCommand.build(
        job_path=f'{SPARK_JOB_BASE_PATH}/{job_path}',
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
    
    # Create Gold database if not exists
    create_gold_db = BashOperator(
        task_id='create_gold_database',
        bash_command="""
        docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \\
          --conf spark.sql.defaultCatalog=gold \\
          -e "CREATE NAMESPACE IF NOT EXISTS gold.gold;"
        """
    )
    
    # ============================================
    # PHASE 1: Common Dimensions (Parallel then Sequential)
    # ============================================
    
    with TaskGroup(group_id='phase1_common_dims') as phase1_tg:
        
        # Step 1: dim_date and dim_province (parallel - no dependencies)
        dim_date = BashOperator(
            task_id='dim_date',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_date']['job_path'],
                resource_level=GOLD_JOBS['dim_date']['resource_level']
            )
        )
        
        dim_province = BashOperator(
            task_id='dim_province',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_province']['job_path'],
                resource_level=GOLD_JOBS['dim_province']['resource_level']
            )
        )
        
        # Step 2: dim_destination (needs province FK, runs after province)
        dim_destination = BashOperator(
            task_id='dim_destination',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_destination']['job_path'],
                resource_level=GOLD_JOBS['dim_destination']['resource_level']
            )
        )
        
        # Flow: date and province parallel → destination after province
        dim_province >> dim_destination
    
    # Phase 1 complete barrier
    wait_phase1 = EmptyOperator(
        task_id='wait_phase1_complete'
    )
    
    # ============================================
    # PHASE 2a: TikTok Pipeline (Sequential)
    # ============================================
    
    with TaskGroup(group_id='phase2a_tiktok_pipeline') as tiktok_pipeline_tg:
        
        # Step 1: dim_author (no dim dependencies)
        dim_author = BashOperator(
            task_id='dim_author',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_author']['job_path'],
                resource_level=GOLD_JOBS['dim_author']['resource_level']
            )
        )
        
        # Step 2: dim_post (depends on author, province, date)
        dim_post = BashOperator(
            task_id='dim_post',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_post']['job_path'],
                resource_level=GOLD_JOBS['dim_post']['resource_level']
            )
        )
        
        # Step 3: dim_comment (depends on post, date)
        dim_comment = BashOperator(
            task_id='dim_comment',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_comment']['job_path'],
                resource_level=GOLD_JOBS['dim_comment']['resource_level']
            )
        )
        
        # Sequential flow within TikTok pipeline
        dim_author >> dim_post >> dim_comment
    
    # ============================================
    # PHASE 2b: Hotel Pipeline (Sequential)
    # ============================================
    
    with TaskGroup(group_id='phase2b_hotel_pipeline') as hotel_pipeline_tg:
        
        # Step 1: dim_room_type, dim_travel_type, dim_country (parallel - no dependencies)
        dim_room_type = BashOperator(
            task_id='dim_room_type',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_room_type']['job_path'],
                resource_level=GOLD_JOBS['dim_room_type']['resource_level']
            )
        )
        
        dim_travel_type = BashOperator(
            task_id='dim_travel_type',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_travel_type']['job_path'],
                resource_level=GOLD_JOBS['dim_travel_type']['resource_level']
            )
        )
        
        dim_country = BashOperator(
            task_id='dim_country',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_country']['job_path'],
                resource_level=GOLD_JOBS['dim_country']['resource_level']
            )
        )
        
        # Step 2: dim_hotel (depends on province)
        dim_hotel = BashOperator(
            task_id='dim_hotel',
            bash_command=build_spark_command(
                GOLD_JOBS['dim_hotel']['job_path'],
                resource_level=GOLD_JOBS['dim_hotel']['resource_level']
            )
        )
        
        # Flow: 3 parallel dims → dim_hotel
        [dim_room_type, dim_travel_type, dim_country] >> dim_hotel
    
    # Phase 2 complete barrier (wait for both pipelines)
    wait_phase2 = EmptyOperator(
        task_id='wait_phase2_complete'
    )
    
    # ============================================
    # PHASE 3: Fact Tables (Parallel)
    # ============================================
    
    with TaskGroup(group_id='phase3_fact_tables') as phase3_tg:
        
        # fact_province_content_engagement (TikTok metrics)
        fact_content = BashOperator(
            task_id='fact_province_content_engagement',
            bash_command=build_spark_command(
                GOLD_JOBS['fact_province_content_engagement']['job_path'],
                resource_level=GOLD_JOBS['fact_province_content_engagement']['resource_level']
            )
        )
        
        # fact_hotel_review (Hotel metrics)
        fact_hotel = BashOperator(
            task_id='fact_hotel_review',
            bash_command=build_spark_command(
                GOLD_JOBS['fact_hotel_review']['job_path'],
                resource_level=GOLD_JOBS['fact_hotel_review']['resource_level']
            )
        )
        
        # fact_comment_nlp_engagement (ML features)
        fact_comment_nlp = BashOperator(
            task_id='fact_comment_nlp_engagement',
            bash_command=build_spark_command(
                GOLD_JOBS['fact_comment_nlp_engagement']['job_path'],
                resource_level=GOLD_JOBS['fact_comment_nlp_engagement']['resource_level']
            )
        )
        
        # Sequential: content → hotel → comment_nlp (avoid resource exhaustion)
        fact_content >> fact_hotel >> fact_comment_nlp
    
    # Phase 3 complete barrier
    wait_phase3 = EmptyOperator(
        task_id='wait_phase3_complete'
    )
    
    # ============================================
    # PHASE 4: ML Training
    # ============================================
    
    with TaskGroup(group_id='phase4_ml_training') as phase4_tg:
        
        # Train province engagement prediction model
        train_model = BashOperator(
            task_id='train_province_model',
            bash_command=build_spark_command(
                GOLD_JOBS['train_province_model']['job_path'],
                resource_level=GOLD_JOBS['train_province_model']['resource_level']
            )
        )
    
    # ============================================
    # Completion Task
    # ============================================
    
    complete = PythonOperator(
        task_id='complete_dag',
        python_callable=log_dag_complete,
        provide_context=True
    )
    
    # ============================================
    # DAG Flow (4-Phase Strategy)
    # ============================================
    
    # Pre-flight
    health_check >> start >> create_gold_db
    
    # Phase 1: Common dimensions (parallel)
    create_gold_db >> phase1_tg >> wait_phase1
    
    # Phase 2: TikTok and Hotel pipelines (parallel from each other)
    wait_phase1 >> tiktok_pipeline_tg >> wait_phase2
    wait_phase1 >> hotel_pipeline_tg >> wait_phase2
    
    # Phase 3: Fact tables (sequential, after both pipelines)
    wait_phase2 >> phase3_tg >> wait_phase3
    
    # Phase 4: ML Training (after all facts complete)
    wait_phase3 >> phase4_tg
    
    # Complete
    phase4_tg >> complete
