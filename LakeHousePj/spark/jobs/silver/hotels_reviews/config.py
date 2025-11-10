"""
Configuration for hotels_reviews Silver Layer Pipeline

Pipeline: hotels_reviews
Source: Bronze CSV files (s3a://bronze/lakehouse/booking_hotels_reviews/raw/*.csv)
Target: Silver Iceberg table (silver.silver.hotels_reviews)
Strategy: 2-Task Pattern
  - Task 1 (Transform): Bronze CSV → Scratch Parquet (preserve all data)
  - Task 2 (Clean & Load): Scratch Parquet → Silver Iceberg (cleaning, deduplication, LEFT ANTI JOIN)
Partition: hotel_name (semantic, co-located joins)
Deduplication: LEFT ANTI JOIN on row_checksum (all 12 business columns)
"""

# Database and table names
SILVER_DATABASE = "silver"
TABLE_NAME = "hotels_reviews"
SILVER_TABLE = f"silver.{SILVER_DATABASE}.{TABLE_NAME}"

# Bronze source path
BRONZE_BASE_PATH = "s3a://bronze/lakehouse/booking_hotels_reviews/raw"

# Scratch bucket for intermediate data (Parquet files, NOT Iceberg)
SCRATCH_BASE_PATH = "s3a://scratch/pipeline/silver/hotels_reviews"

# Bronze filename pattern
# Format: vietnam_hotels_reviews_YYYYMMDD_HHMMSS_checksum.csv
BRONZE_FILE_PATTERN = r'vietnam_hotels_reviews_(\d{8}_\d{6})_([a-f0-9]{8})\.csv'

# Business columns (for row_checksum calculation)
BUSINESS_COLUMNS = [
    "hotel_name",
    "hotel_url",
    "reviewer_name",
    "reviewer_country",
    "room_type",
    "stay_date",
    "traveler_type",
    "review_date",
    "review_title",
    "review_score",
    "review_positive",
    "review_negative"
]

# Partition columns - NO PARTITION (avoid data skew with many hotels)
PARTITION_COLUMNS = []

# NOT NULL constraints (for data validation)
NOT_NULL_COLUMNS = ["hotel_name"]

# PostgreSQL connection parameters
POSTGRES_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'metastore_db',
    'user': 'lakehouse_user',
    'password': 'lakehouse_pass'
}
