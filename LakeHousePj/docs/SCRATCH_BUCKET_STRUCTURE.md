# Scratch Bucket Structure & Management

**Purpose:** Temporary storage for multi-task pipeline intermediate data  
**Location:** `s3a://scratch/pipeline/`  
**Retention:** 2 latest runs per table (configurable)

---

## 📁 Directory Structure

```
scratch/
│
├── pipeline/                              # Pipeline tmp data (NEW)
│   │
│   ├── silver/                           # Silver layer pipelines
│   │   │
│   │   ├── hotels_list/                  # Table 1: Hotels List
│   │   │   ├── run_20251108_140000/      # Run 1 (older)
│   │   │   │   ├── 01_extracted/
│   │   │   │   │   └── part-*.parquet
│   │   │   │   ├── 02_cleaned/
│   │   │   │   │   └── part-*.parquet
│   │   │   │   ├── 03_validated/
│   │   │   │   │   └── part-*.parquet
│   │   │   │   └── _metadata.json
│   │   │   │
│   │   │   └── run_20251108_150000/      # Run 2 (latest) ✨
│   │   │       ├── 01_extracted/
│   │   │       ├── 02_cleaned/
│   │   │       ├── 03_validated/
│   │   │       └── _metadata.json
│   │   │
│   │   ├── hotels_detail/                # Table 2: Hotels Detail
│   │   │   └── run_20251108_140100/
│   │   │       ├── 01_extracted/
│   │   │       ├── 02_cleaned/
│   │   │       ├── 03_validated/
│   │   │       └── _metadata.json
│   │   │
│   │   ├── hotels_reviews/               # Table 3: Hotels Reviews
│   │   │   └── run_20251108_140200/
│   │   │       ├── 01_extracted/
│   │   │       ├── 02_cleaned/
│   │   │       ├── 03_validated/
│   │   │       └── _metadata.json
│   │   │
│   │   ├── tiktok_videos/                # Table 4: TikTok Videos
│   │   │   └── run_20251108_140300/
│   │   │       ├── 01_extracted/
│   │   │       ├── 02_cleaned/
│   │   │       ├── 03_validated/
│   │   │       └── _metadata.json
│   │   │
│   │   ├── tiktok_post_metadata/         # Table 5: TikTok Metadata
│   │   │   └── run_20251108_140400/
│   │   │       ├── 01_extracted/
│   │   │       ├── 02_cleaned/
│   │   │       ├── 03_validated/
│   │   │       └── _metadata.json
│   │   │
│   │   └── tiktok_post_comments/         # Table 6: TikTok Comments
│   │       └── run_20251108_140500/
│   │           ├── 01_extracted/
│   │           ├── 02_cleaned/
│   │           ├── 03_validated/
│   │           └── _metadata.json
│   │
│   └── gold/                             # Gold layer pipelines (FUTURE)
│       ├── dim_hotels/
│       ├── dim_locations/
│       ├── fact_reviews/
│       └── fact_tiktok_engagement/
│
├── hive/                                 # Hive tmp data (EXISTING)
│   └── {uuid}/_tmp_space.db/
│
└── _cleanup/                             # Cleanup management
    ├── retention_policy.json             # Retention configuration
    └── cleanup_history.log               # Cleanup audit log
```

---

## 📋 Metadata File Format

Each run folder contains `_metadata.json` with complete run information:

```json
{
  "run_id": "run_20251108_143022",
  "layer": "silver",
  "table_name": "hotels_list",
  "started_at": "2025-11-08T14:30:22Z",
  "completed_at": "2025-11-08T14:32:15Z",
  "status": "success",
  "duration_seconds": 113,
  
  "source": {
    "layer": "bronze",
    "path": "s3a://bronze/lakehouse/booking_hotels_list/raw",
    "file_name": "vietnam_hotels_list_20251029_194555_a1b2c3d4.csv",
    "file_checksum": "a1b2c3d4",
    "file_size_bytes": 2548736
  },
  
  "steps": {
    "extract": {
      "status": "success",
      "started_at": "2025-11-08T14:30:22Z",
      "completed_at": "2025-11-08T14:30:45Z",
      "duration_seconds": 23,
      "records_extracted": 1250,
      "output_path": "s3a://scratch/pipeline/silver/hotels_list/run_20251108_143022/01_extracted"
    },
    "clean": {
      "status": "success",
      "started_at": "2025-11-08T14:30:45Z",
      "completed_at": "2025-11-08T14:31:12Z",
      "duration_seconds": 27,
      "records_input": 1250,
      "records_output": 1248,
      "metrics": {
        "nulls_removed": 2,
        "duplicates_removed": 0,
        "whitespace_trimmed": 15
      },
      "output_path": "s3a://scratch/pipeline/silver/hotels_list/run_20251108_143022/02_cleaned"
    },
    "validate": {
      "status": "success",
      "started_at": "2025-11-08T14:31:12Z",
      "completed_at": "2025-11-08T14:31:38Z",
      "duration_seconds": 26,
      "records_validated": 1248,
      "validation_passed": true,
      "quality_metrics": {
        "completeness": 100.0,
        "uniqueness": 100.0,
        "consistency": 98.5
      },
      "output_path": "s3a://scratch/pipeline/silver/hotels_list/run_20251108_143022/03_validated"
    },
    "load": {
      "status": "success",
      "started_at": "2025-11-08T14:31:38Z",
      "completed_at": "2025-11-08T14:32:10Z",
      "duration_seconds": 32,
      "merge_stats": {
        "inserted": 10,
        "updated": 5,
        "skipped": 1233
      },
      "target_table": "lakehouse.silver.hotels_list"
    },
    "cleanup": {
      "status": "success",
      "started_at": "2025-11-08T14:32:10Z",
      "completed_at": "2025-11-08T14:32:15Z",
      "duration_seconds": 5,
      "deleted_folders": ["01_extracted", "02_cleaned", "03_validated"],
      "space_freed_mb": 45.8
    }
  },
  
  "pipeline": {
    "dag_id": "silver_hotels_list",
    "execution_date": "2025-11-08T02:00:00Z",
    "run_type": "scheduled"
  },
  
  "errors": [],
  "warnings": [
    "2 records with NULL province were removed"
  ]
}
```

---

## 🧹 Retention Policy

### Configuration: `_cleanup/retention_policy.json`

```json
{
  "version": "1.0",
  "updated_at": "2025-11-08T00:00:00Z",
  
  "default_policy": {
    "keep_latest_runs": 2,
    "max_age_days": 7,
    "cleanup_schedule": "0 3 * * *"
  },
  
  "layer_policies": {
    "silver": {
      "keep_latest_runs": 2,
      "max_age_days": 3,
      "keep_failed_runs": 5,
      "max_failed_age_days": 7
    },
    "gold": {
      "keep_latest_runs": 3,
      "max_age_days": 7,
      "keep_failed_runs": 10,
      "max_failed_age_days": 14
    }
  },
  
  "table_overrides": {
    "hotels_list": {
      "keep_latest_runs": 3,
      "reason": "High update frequency"
    }
  }
}
```

### Retention Rules

| Run Status | Keep Latest | Max Age | Notes |
|------------|-------------|---------|-------|
| Success | 2 runs | 3 days | For rollback/debug |
| Failed | 5 runs | 7 days | For troubleshooting |
| Running | All | N/A | Don't cleanup active runs |

---

## 🔧 Path Management Utilities

### Python Helper Functions

```python
# utils/path_manager.py

class ScratchPathManager:
    """Manage scratch bucket paths for multi-task pipeline"""
    
    BASE_PATH = "s3a://scratch/pipeline"
    
    def __init__(self, layer: str, table_name: str, run_timestamp: str = None):
        self.layer = layer
        self.table_name = table_name
        self.run_timestamp = run_timestamp or self._generate_timestamp()
        self.run_id = f"run_{self.run_timestamp}"
    
    @staticmethod
    def _generate_timestamp() -> str:
        """Generate timestamp: YYYYMMDD_HHMMSS"""
        from datetime import datetime
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def get_run_base_path(self) -> str:
        """Get base path for this run"""
        return f"{self.BASE_PATH}/{self.layer}/{self.table_name}/{self.run_id}"
    
    def get_step_path(self, step: str) -> str:
        """Get path for specific pipeline step"""
        step_map = {
            "extracted": "01_extracted",
            "cleaned": "02_cleaned",
            "validated": "03_validated"
        }
        if step not in step_map:
            raise ValueError(f"Invalid step: {step}")
        return f"{self.get_run_base_path()}/{step_map[step]}"
    
    def get_metadata_path(self) -> str:
        """Get path for metadata JSON file"""
        return f"{self.get_run_base_path()}/_metadata.json"
    
    def list_all_runs(self, spark) -> list:
        """List all runs for this table"""
        table_path = f"{self.BASE_PATH}/{self.layer}/{self.table_name}"
        # Use Spark to list directories
        # Implementation depends on Spark version
        pass
    
    def cleanup_old_runs(self, spark, keep_latest: int = 2):
        """Delete old run folders, keep N latest"""
        runs = self.list_all_runs(spark)
        runs.sort(reverse=True)  # Sort by timestamp descending
        
        runs_to_delete = runs[keep_latest:]
        for run_path in runs_to_delete:
            # Delete using Hadoop FileSystem
            self._delete_directory(spark, run_path)
```

### Usage Example

```python
from utils.path_manager import ScratchPathManager

# In Extract task
path_mgr = ScratchPathManager(
    layer="silver",
    table_name="hotels_list",
    run_timestamp="20251108_143022"
)

# Get paths for each step
extract_path = path_mgr.get_step_path("extracted")
# s3a://scratch/pipeline/silver/hotels_list/run_20251108_143022/01_extracted

clean_path = path_mgr.get_step_path("cleaned")
# s3a://scratch/pipeline/silver/hotels_list/run_20251108_143022/02_cleaned

validate_path = path_mgr.get_step_path("validated")
# s3a://scratch/pipeline/silver/hotels_list/run_20251108_143022/03_validated

metadata_path = path_mgr.get_metadata_path()
# s3a://scratch/pipeline/silver/hotels_list/run_20251108_143022/_metadata.json

# Write data
df.write.parquet(extract_path)

# In Cleanup task
path_mgr.cleanup_old_runs(spark, keep_latest=2)
```

---

## 📊 Space Management

### Estimated Space Usage (per table, per run)

| Table | Records | Extract | Clean | Validate | Total | After Cleanup |
|-------|---------|---------|-------|----------|-------|---------------|
| hotels_list | 1,248 | 15 MB | 14 MB | 14 MB | 43 MB | 0 MB (deleted) |
| hotels_detail | 1,248 | 25 MB | 24 MB | 24 MB | 73 MB | 0 MB (deleted) |
| hotels_reviews | 12,000 | 45 MB | 42 MB | 42 MB | 129 MB | 0 MB (deleted) |
| tiktok_videos | 35,000 | 120 MB | 115 MB | 115 MB | 350 MB | 0 MB (deleted) |
| tiktok_post_metadata | 35,000 | 80 MB | 75 MB | 75 MB | 230 MB | 0 MB (deleted) |
| tiktok_post_comments | 1.7M | 650 MB | 620 MB | 620 MB | 1,890 MB | 0 MB (deleted) |

**Total per full pipeline run:** ~2.7 GB  
**With 2 runs retained (metadata only):** ~10 MB (just JSON files)  
**After cleanup:** Minimal footprint

### Space Optimization

1. **Aggressive Cleanup:** Delete tmp folders immediately after load
2. **Keep Metadata Only:** Preserve `_metadata.json` for audit trail
3. **Compression:** Use Snappy compression for Parquet files
4. **Partitioning:** Don't partition tmp data (no need, will be deleted)

---

## 🔍 Monitoring & Alerts

### Metrics to Track

1. **Storage Metrics:**
   - Total scratch bucket size
   - Size per table
   - Oldest run age

2. **Pipeline Metrics:**
   - Run success rate
   - Average run duration
   - Data volume per run

3. **Cleanup Metrics:**
   - Space freed per cleanup
   - Number of runs deleted
   - Failed cleanups

### Alert Conditions

| Alert | Condition | Action |
|-------|-----------|--------|
| 🚨 Space Full | Scratch > 50GB | Force cleanup |
| ⚠️ Old Runs | Runs > 7 days | Review retention |
| ❌ Cleanup Failed | Cleanup errors | Manual intervention |
| 🐌 Slow Pipeline | Duration > 2x avg | Investigate performance |

---

## 🚀 Operations

### Daily Operations

```bash
# Check scratch bucket size
docker exec lakehouse_minio mc du local/scratch/

# List all runs for a table
docker exec lakehouse_minio mc ls --recursive local/scratch/pipeline/silver/hotels_list/

# Manual cleanup (if needed)
docker exec lakehouse_minio mc rm --recursive --force local/scratch/pipeline/silver/hotels_list/run_20251101_*/
```

### Emergency Cleanup

```bash
# Delete all tmp data (keep metadata)
docker exec lakehouse_minio mc rm --recursive --force local/scratch/pipeline/silver/*/run_*/01_extracted/
docker exec lakehouse_minio mc rm --recursive --force local/scratch/pipeline/silver/*/run_*/02_cleaned/
docker exec lakehouse_minio mc rm --recursive --force local/scratch/pipeline/silver/*/run_*/03_validated/

# Complete wipe (nuclear option)
docker exec lakehouse_minio mc rm --recursive --force local/scratch/pipeline/
```

---

## 📝 Best Practices

1. **Always use ScratchPathManager** - Don't hardcode paths
2. **Write metadata** - Document every run
3. **Cleanup aggressively** - Delete tmp data ASAP
4. **Monitor space** - Set up alerts
5. **Test cleanup** - Verify cleanup works before running in production
6. **Document errors** - Log failures in metadata.json
7. **Audit trail** - Keep metadata.json indefinitely (small size)

---

**Last Updated:** November 8, 2025  
**Owner:** Data Engineering Team  
**Review:** Monthly
