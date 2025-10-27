# Airflow DAGs Structure

## 📁 Folder Organization

```
airflow/dags/
├── bronze/              # Bronze layer ingestion
│   ├── __init__.py
│   ├── config.py       # DAG configuration
│   └── dag.py          # Main DAG (bronze_layer_ingestion)
│
├── silver/              # Silver layer transformation (TODO)
│   ├── __init__.py
│   ├── config.py
│   └── dag.py          # Main DAG (silver_layer_transformation)
│
├── gold/                # Gold layer aggregation (TODO)
│   ├── __init__.py
│   ├── config.py
│   └── dag.py          # Main DAG (gold_layer_aggregation)
│
└── common/              # Shared utilities
    ├── __init__.py
    ├── spark_operators.py   # Spark command builders
    ├── health_checks.py     # Docker health checks
    └── notifications.py     # Logging & alerts
```

## 🎯 DAGs Available

### ✅ Bronze Layer (Ready)
- **DAG ID**: `bronze_layer_ingestion`
- **File**: `bronze/dag.py`
- **Schedule**: Manual (change to `@daily` for automation)
- **Features**:
  - Checksum-based deduplication
  - PostgreSQL file tracking
  - Parallel execution (Booking.com)
  - Sequential execution (TikTok: videos → comments)

### 🚧 Silver Layer (Template)
- **DAG ID**: `silver_layer_transformation`
- **File**: `silver/dag.py`
- **Schedule**: `@daily`
- **Status**: Template only (EmptyOperator tasks)

### 🚧 Gold Layer (Template)
- **DAG ID**: `gold_layer_aggregation`
- **File**: `gold/dag.py`
- **Schedule**: `@weekly`
- **Status**: Template only (EmptyOperator tasks)

## 🚀 Usage

### Trigger Bronze DAG (Manual)
```bash
docker exec lakehouse_airflow airflow dags trigger bronze_layer_ingestion
```

### View DAGs in UI
```bash
# Access Airflow UI
http://localhost:8082

# Login credentials (default)
Username: admin
Password: admin
```

## 📝 Adding New Tasks

### Bronze Layer Example
Edit `bronze/dag.py`:
```python
new_task = BashOperator(
    task_id='ingest_new_source',
    bash_command=SparkSubmitCommand.bronze_job('ingest_new_source'),
    dag=dag,
)

# Add to dependencies
init_tracking >> new_task >> complete_task
```

### Silver/Gold Layer
1. Implement Spark jobs in `spark/jobs/silver/` or `spark/jobs/gold/`
2. Replace `EmptyOperator` with `BashOperator` in `silver/dag.py` or `gold/dag.py`
3. Update dependencies

## 🔗 DAG Dependencies (Future)

```
Bronze DAG (@hourly)
    ↓ (TriggerDagRunOperator)
Silver DAG (@daily)
    ↓ (TriggerDagRunOperator)
Gold DAG (@weekly)
```

## 🛠️ Common Utilities

### Spark Command Builder
```python
from common.spark_operators import SparkSubmitCommand

# Bronze job
cmd = SparkSubmitCommand.bronze_job('ingest_tiktok_videos')

# Silver job
cmd = SparkSubmitCommand.silver_job('clean_tiktok_data')

# Gold job
cmd = SparkSubmitCommand.gold_job('aggregate_hotels')
```

### Health Checks
```python
from common.health_checks import check_docker_health, check_containers_health

# Check all required containers
check_docker_health()

# Check specific containers
check_containers_health(['lakehouse_spark_master', 'lakehouse_postgres'])
```

### Notifications
```python
from common.notifications import log_dag_start, log_dag_complete, send_alert

# Log DAG start (returns metadata dict)
start_info = log_dag_start(**context)

# Log DAG completion
log_dag_complete(**context)

# Send alert (can extend to Slack/Email)
send_alert("DAG failed!", level='error', **context)
```

## 📊 Monitoring

### Check DAG Status
```bash
# List all DAGs
docker exec lakehouse_airflow airflow dags list

# Check specific DAG runs
docker exec lakehouse_airflow airflow dags list-runs -d bronze_layer_ingestion

# View task logs
docker exec lakehouse_airflow airflow tasks logs bronze_layer_ingestion ingest_tiktok_videos <execution_date>
```

### Airflow UI Views
- **DAGs**: Overview of all DAGs
- **Graph**: Visual task dependencies
- **Tree**: Historical run timeline
- **Gantt**: Task execution duration
- **Code**: View DAG source code

## 🔧 Configuration

### Change Schedule
Edit `bronze/config.py`:
```python
SCHEDULE_INTERVAL = '@daily'  # or '@hourly', '0 2 * * *', etc.
```

### Update Spark Config
Edit `common/spark_operators.py`:
```python
BASE_CMD = """
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
    --conf spark.driver.memory=4g \    # Add custom configs
    --conf spark.executor.memory=8g \
    {job_path}
"""
```

## ⚠️ Important Notes

1. **Folder structure**: Airflow scans `dags/` for Python files with DAG objects
2. **Import paths**: Use `sys.path.insert()` to import from `common/`
3. **DAG ID uniqueness**: Each DAG must have unique `dag_id`
4. **File tracking**: Bronze uses PostgreSQL `file_ingestion_log` table
5. **Checksum**: MD5 hash prevents duplicate ingestion

## 📚 Related Documentation

- [Checksum Tracking Guide](../../docs/CHECKSUM_TRACKING.md)
- [Bronze Verification Summary](../../docs/BRONZE_VERIFICATION_SUMMARY.md)
- [Deployment Guide](../../docs/DEPLOYMENT.md)

---

**Last Updated**: October 27, 2025
