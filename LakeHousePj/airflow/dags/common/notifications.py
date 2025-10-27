"""
Notification and Logging Helpers
"""

import logging
from datetime import datetime


def send_alert(message: str, level: str = 'info', **context):
    """
    Send alert notification (can be extended to Slack/Email/Telegram)
    
    Args:
        message: Alert message
        level: Log level (info, warning, error)
        context: Airflow context
    """
    log_func = getattr(logging, level, logging.info)
    log_func(f"ALERT: {message}")
    
    # TODO: Extend to send Slack/Email notifications
    # slack_webhook = Variable.get("slack_webhook_url", default_var=None)
    # if slack_webhook:
    #     requests.post(slack_webhook, json={"text": message})


def log_dag_start(**context) -> dict:
    """
    Log DAG run start
    
    Returns:
        Dict with run metadata
    """
    dag_run = context['dag_run']
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    
    logging.info("=" * 80)
    logging.info(f"🚀 DAG STARTED: {dag_id}")
    logging.info("=" * 80)
    logging.info(f"Run ID: {dag_run.run_id}")
    logging.info(f"Execution Date: {execution_date}")
    logging.info(f"Trigger: {dag_run.external_trigger}")
    logging.info(f"Start Time: {datetime.now()}")
    logging.info("=" * 80)
    
    return {
        'dag_id': dag_id,
        'run_id': dag_run.run_id,
        'execution_date': str(execution_date),
        'start_time': str(datetime.now())
    }


def log_dag_complete(**context) -> str:
    """
    Log DAG run completion
    
    Returns:
        Success message
    """
    ti = context['task_instance']
    dag_id = context['dag'].dag_id
    
    # Try to get start info from XCom
    start_info = ti.xcom_pull(task_ids='start_task')
    
    logging.info("=" * 80)
    logging.info(f"✅ DAG COMPLETED: {dag_id}")
    logging.info("=" * 80)
    
    if start_info:
        logging.info(f"Run ID: {start_info.get('run_id')}")
        logging.info(f"Started: {start_info.get('start_time')}")
    
    logging.info(f"Completed: {datetime.now()}")
    logging.info("=" * 80)
    
    return f"{dag_id} completed successfully"
