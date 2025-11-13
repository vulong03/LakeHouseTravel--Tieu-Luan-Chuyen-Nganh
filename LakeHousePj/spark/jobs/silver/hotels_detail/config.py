"""
Configuration for Silver Layer - Hotels Detail Pipeline
Based on original transform_booking_hotels_detail.py logic
"""

# Database and table names
SILVER_DATABASE = "silver"
TABLE_NAME = "hotels_detail"
SILVER_TABLE = f"silver.{SILVER_DATABASE}.{TABLE_NAME}"

# Bronze source
BRONZE_BASE_PATH = "s3a://bronze/lakehouse/booking_hotels_detail/raw"

# Scratch bucket for intermediate data
SCRATCH_BASE_PATH = "s3a://scratch/pipeline/silver/hotels_detail"

# Business key for UPSERT
BUSINESS_KEY = "hotel_url"

# Business columns for checksum calculation (change detection)
BUSINESS_COLUMNS = [
    "hotel_name",
    "hotel_url", 
    "province",
    "description",
    "top_amenities",
    "rating_score",
    "review_count_text",
    "review_count",  # NEW: Extracted review count
    "rating_breakdown",
    "activities"
]

# Partition columns (S3-safe cleaned province)
PARTITION_COLUMNS = ["province"]

# PostgreSQL connection for tracking
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}

# Data validation - columns that cannot be NULL
NOT_NULL_COLUMNS = ["hotel_name", "hotel_url", "province"]
