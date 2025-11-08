"""
Configuration for hotels_list Silver pipeline
Shared across all 3 tasks: Transform → Clean → Validate+Load
"""

from datetime import datetime

# ============================================================================
# TABLE METADATA
# ============================================================================
TABLE_NAME = "hotels_list"
LAYER = "silver"
BUSINESS_KEY = "hotel_url"  # Primary key for MERGE operation
BUSINESS_COLUMNS = ["stt", "hotel_name", "hotel_url", "province"]
REQUIRED_COLUMNS = ["hotel_name", "hotel_url", "province"]  # NOT NULL columns

# ============================================================================
# SOURCE CONFIGURATION (Bronze Layer)
# ============================================================================
BRONZE_BASE_PATH = "s3a://bronze/lakehouse/booking_hotels_list/raw"

# ============================================================================
# TARGET CONFIGURATION (Silver Layer)
# ============================================================================
SILVER_DATABASE = "silver"
SILVER_TABLE = f"silver.{SILVER_DATABASE}.{TABLE_NAME}"  # Use 'silver' catalog, not 'lakehouse'
PARTITION_COLUMNS = ["province"]

# ============================================================================
# SCRATCH BUCKET PATHS
# ============================================================================
# Generate run timestamp once (shared across all tasks via environment variable or file)
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
SCRATCH_BASE = f"s3a://scratch/pipeline/{LAYER}/{TABLE_NAME}/run_{RUN_TIMESTAMP}"

PATHS = {
    "transformed": f"{SCRATCH_BASE}/01_transformed",   # After Task 1: Transform
    "metadata": f"{SCRATCH_BASE}/_metadata.json"       # Run metadata
}
# Note: Task 2 cleans and writes directly to Silver (no intermediate tmp folder)

# ============================================================================
# DATA QUALITY RULES
# ============================================================================
QUALITY_RULES = {
    "max_null_percentage": 5.0,           # Max 5% nulls in any required column
    "min_records": 100,                    # Must have at least 100 records
    "max_duplicate_percentage": 1.0,       # Max 1% duplicates on business key
}

# ============================================================================
# POSTGRES TRACKING
# ============================================================================
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}

# ============================================================================
# DATA QUALITY RULES - INVALID VALUES
# ============================================================================
# Values that should be treated as invalid/NULL
INVALID_VALUES = ["0", ""]  # "0" appears as invalid province in data
