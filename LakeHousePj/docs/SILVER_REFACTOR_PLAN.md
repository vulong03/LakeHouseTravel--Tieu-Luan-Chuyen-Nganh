# Silver Layer Refactoring Plan - Multi-Task Pipeline

**Date:** November 8, 2025  
**Target:** Refactor `hotels_list` job first (pilot), then roll out to remaining 5 tables  
**Goal:** Implement multi-task pipeline with observability using scratch bucket for tmp data

---

## 📊 Current State Analysis

### Existing Job: `transform_booking_hotels_list.py`

**Current Flow (Single Task):**
```
Bronze CSV → Read → Validate → Add Metadata → Checksum → MERGE → Silver Iceberg
```

**Issues:**
1. ❌ **Monolithic**: All steps in one task → hard to debug specific step
2. ❌ **No Observability**: Can't see which step is running/failing in Airflow UI
3. ❌ **No Cleaning Logic**: Only validates NOT NULL, doesn't clean data
4. ❌ **Direct Write**: Writes directly to Silver → hard to rollback if issue
5. ❌ **Limited Validation**: Only basic NULL checks

**Strengths to Keep:**
1. ✅ File tracking with checksums (PostgreSQL)
2. ✅ MERGE/UPSERT strategy (hotel_url as business key)
3. ✅ Row-level checksum for change detection
4. ✅ Metadata tracking (source_file, timestamp)

---

## 🎯 Target Architecture

### New Multi-Task Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AIRFLOW DAG: silver_hotels_list                  │
└─────────────────────────────────────────────────────────────────────────┘

Task 1: EXTRACT                    Task 2: CLEAN                     Task 3: VALIDATE
┌──────────────────┐              ┌──────────────────┐             ┌──────────────────┐
│ Read from Bronze │              │ Apply Cleaning   │             │ Apply Quality    │
│ - Latest CSV     │──────────────▶ - Remove nulls   │─────────────▶ - Check schema   │
│ - Check tracking │              │ - Deduplicate    │             │ - Validate rules │
│ - Basic parse    │              │ - Standardize    │             │ - Data profiling │
└──────────────────┘              └──────────────────┘             └──────────────────┘
        │                                  │                                 │
        ▼                                  ▼                                 ▼
  s3a://scratch/                    s3a://scratch/                   s3a://scratch/
  pipeline/silver/                  pipeline/silver/                 pipeline/silver/
  hotels_list/                      hotels_list/                     hotels_list/
  run_TIMESTAMP/                    run_TIMESTAMP/                   run_TIMESTAMP/
  01_extracted/                     02_cleaned/                      03_validated/


Task 4: LOAD                       Task 5: CLEANUP
┌──────────────────┐              ┌──────────────────┐
│ MERGE to Silver  │              │ Delete tmp files │
│ - UPSERT mode    │──────────────▶ - Remove run_*   │
│ - Track stats    │              │ - Keep metadata  │
│ - Log success    │              │ - Retain policy  │
└──────────────────┘              └──────────────────┘
        │                                  
        ▼                                  
  s3a://silver/
  lakehouse/
  hotels_list/
  data/
```

---

## 📁 Scratch Bucket Structure

```
scratch/
├── pipeline/
│   └── silver/                          # Layer identifier
│       └── hotels_list/                 # Table name
│           └── run_20251108_143022/     # Run timestamp
│               ├── 01_extracted/        # After Extract task
│               │   └── *.parquet
│               ├── 02_cleaned/          # After Clean task
│               │   └── *.parquet
│               ├── 03_validated/        # After Validate task
│               │   └── *.parquet
│               └── _metadata.json       # Run metadata & stats
│
└── _cleanup/
    └── retention_policy.json
```

---

## 🔧 Implementation Plan

### Phase 1: Create Utility Modules ✨

#### 1.1 Create `utils/path_manager.py`
```python
"""
Manage scratch bucket paths for multi-task pipeline
"""

class ScratchPathManager:
    def __init__(self, layer: str, table_name: str, run_timestamp: str):
        self.layer = layer
        self.table_name = table_name
        self.run_timestamp = run_timestamp
        self.base_path = f"s3a://scratch/pipeline/{layer}/{table_name}/run_{run_timestamp}"
    
    def get_step_path(self, step: str) -> str:
        """Get path for specific step"""
        step_map = {
            "extracted": "01_extracted",
            "cleaned": "02_cleaned",
            "validated": "03_validated"
        }
        return f"{self.base_path}/{step_map[step]}"
    
    def get_metadata_path(self) -> str:
        """Get path for metadata file"""
        return f"{self.base_path}/_metadata.json"
```

#### 1.2 Create `utils/cleaning.py`
```python
"""
Data cleaning utilities for Silver layer
"""

def clean_hotels_list(df: DataFrame) -> DataFrame:
    """
    Clean hotels_list data
    - Remove exact duplicates
    - Remove records with NULL in required fields
    - Trim whitespace
    - Standardize province names
    """
    # Implementation details...
```

#### 1.3 Create `utils/validation.py`
```python
"""
Data quality validation for Silver layer
"""

def validate_hotels_list(df: DataFrame) -> dict:
    """
    Validate hotels_list data quality
    Returns: {
        "passed": bool,
        "metrics": {...},
        "issues": [...]
    }
    """
    # Implementation details...
```

---

### Phase 2: Refactor Job to Multi-Task ✨

#### 2.1 New File Structure
```
spark/jobs/silver/hotels_list/
├── __init__.py
├── step_01_extract.py          # Task 1: Extract from Bronze
├── step_02_clean.py            # Task 2: Clean data
├── step_03_validate.py         # Task 3: Validate quality
├── step_04_load.py             # Task 4: Load to Silver
├── step_05_cleanup.py          # Task 5: Cleanup tmp files
└── config.py                   # Shared config & paths
```

#### 2.2 Task 1: Extract (`step_01_extract.py`)
**Responsibilities:**
- Read latest Bronze CSV file
- Check if already processed (checksum tracking)
- Parse CSV with basic schema
- Write to scratch: `01_extracted/`
- Log extraction stats

**Output:**
- Raw data in Parquet format
- Extraction metadata (record count, file info)

#### 2.3 Task 2: Clean (`step_02_clean.py`)
**Responsibilities:**
- Read from `01_extracted/`
- Remove NULL values in required fields
- Remove exact duplicates
- Trim whitespace from strings
- Standardize province names
- Write to scratch: `02_cleaned/`
- Log cleaning stats

**Output:**
- Cleaned data in Parquet
- Cleaning metrics (nulls removed, duplicates removed)

#### 2.4 Task 3: Validate (`step_03_validate.py`)
**Responsibilities:**
- Read from `02_cleaned/`
- Validate schema constraints
- Check business rules
- Calculate data quality metrics
- Write to scratch: `03_validated/`
- Log validation results

**Output:**
- Validated data in Parquet
- Validation report (pass/fail, metrics)

#### 2.5 Task 4: Load (`step_04_load.py`)
**Responsibilities:**
- Read from `03_validated/`
- Calculate row checksums
- MERGE into Silver Iceberg table
- Track merge stats (inserted, updated, skipped)
- Log to PostgreSQL tracking

**Output:**
- Data in Silver table
- Merge statistics

#### 2.6 Task 5: Cleanup (`step_05_cleanup.py`)
**Responsibilities:**
- Delete tmp folders: `01_extracted/`, `02_cleaned/`, `03_validated/`
- Keep `_metadata.json` for audit trail
- Apply retention policy (keep last 2 runs)
- Log cleanup activity

**Output:**
- Clean scratch bucket
- Cleanup logs

---

### Phase 3: Create Airflow DAG ✨

#### 3.1 DAG Structure (`dags/silver_hotels_list.py`)
```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='silver_hotels_list',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
) as dag:
    
    extract = SparkSubmitOperator(
        task_id='extract_from_bronze',
        application='/opt/spark/jobs/silver/hotels_list/step_01_extract.py',
        ...
    )
    
    clean = SparkSubmitOperator(
        task_id='clean_data',
        application='/opt/spark/jobs/silver/hotels_list/step_02_clean.py',
        ...
    )
    
    validate = SparkSubmitOperator(
        task_id='validate_quality',
        application='/opt/spark/jobs/silver/hotels_list/step_03_validate.py',
        ...
    )
    
    load = SparkSubmitOperator(
        task_id='load_to_silver',
        application='/opt/spark/jobs/silver/hotels_list/step_04_load.py',
        ...
    )
    
    cleanup = SparkSubmitOperator(
        task_id='cleanup_tmp',
        application='/opt/spark/jobs/silver/hotels_list/step_05_cleanup.py',
        ...
    )
    
    extract >> clean >> validate >> load >> cleanup
```

---

### Phase 4: Testing & Validation ✨

#### 4.1 Unit Tests
- Test each step function independently
- Test path manager utility
- Test cleaning logic
- Test validation rules

#### 4.2 Integration Tests
- Test full pipeline end-to-end
- Test failure scenarios (rollback)
- Test idempotency (re-run same data)
- Test cleanup logic

#### 4.3 Performance Tests
- Measure task execution time
- Monitor scratch bucket space usage
- Validate merge performance

---

## 📋 Implementation Checklist

### Week 1: Setup & Utils
- [ ] Create `utils/path_manager.py`
- [ ] Create `utils/cleaning.py`
- [ ] Create `utils/validation.py`
- [ ] Write unit tests for utils
- [ ] Create scratch bucket structure

### Week 2: Refactor hotels_list
- [ ] Create job folder structure
- [ ] Implement `step_01_extract.py`
- [ ] Implement `step_02_clean.py`
- [ ] Implement `step_03_validate.py`
- [ ] Implement `step_04_load.py`
- [ ] Implement `step_05_cleanup.py`
- [ ] Create `config.py` for shared settings

### Week 3: Airflow Integration
- [ ] Create `silver_hotels_list` DAG
- [ ] Test individual tasks in Airflow
- [ ] Test full pipeline flow
- [ ] Setup monitoring & alerts
- [ ] Document observability features

### Week 4: Testing & Optimization
- [ ] Run integration tests
- [ ] Performance tuning
- [ ] Fix bugs & edge cases
- [ ] Update documentation
- [ ] Create runbook for operations

### Week 5: Rollout to Remaining Tables
- [ ] Refactor `hotels_detail` (similar pattern)
- [ ] Refactor `hotels_reviews` (LEFT ANTI JOIN pattern)
- [ ] Refactor `tiktok_videos`
- [ ] Refactor `tiktok_comments` (2 tables)
- [ ] Update all DAGs

---

## 🎯 Success Metrics

### Observability Improvements
- ✅ Can see which task is running in Airflow UI
- ✅ Can identify which step failed
- ✅ Can view logs per step
- ✅ Can retry individual failed tasks

### Data Quality Improvements
- ✅ Track cleaning metrics (nulls removed, duplicates removed)
- ✅ Track validation results per run
- ✅ Automatic data profiling
- ✅ Quality trend analysis over time

### Operational Improvements
- ✅ Faster debugging (isolated steps)
- ✅ Easier maintenance (modular code)
- ✅ Better resource usage (cleanup tmp files)
- ✅ Audit trail (metadata per run)

---

## 🚀 Next Steps

1. **Review this plan** - Get approval from team
2. **Create utils** - Start with path_manager.py
3. **Pilot refactor** - Implement hotels_list first
4. **Test thoroughly** - Validate all scenarios
5. **Document** - Update runbooks
6. **Rollout** - Apply to remaining 5 tables

---

## 📝 Notes

- **Backward Compatibility:** Old job will remain available during transition
- **Gradual Migration:** Refactor one table at a time
- **Rollback Plan:** Can switch back to old job if issues
- **Monitoring:** Setup alerts for task failures
- **Documentation:** Update README and runbooks

---

**Status:** 📋 Planning  
**Next Action:** Create `utils/path_manager.py`  
**Owner:** TBD  
**Timeline:** 5 weeks
