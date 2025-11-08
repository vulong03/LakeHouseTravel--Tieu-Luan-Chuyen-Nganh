"""
Configuration for TikTok Videos Silver Pipeline
"""

# Table Configuration
SILVER_TABLE = "silver.silver.tiktok_videos"

# Source Paths
BRONZE_BASE_PATH = "s3a://bronze/lakehouse/tiktok_videos/raw"
SCRATCH_BASE_PATH = "s3a://scratch/pipeline/silver/tiktok_videos"

# Business Key (for UPSERT detection)
BUSINESS_KEY = "url"

# Business Columns (for checksum calculation)
BUSINESS_COLUMNS = [
    "url",
    "posted_date",
    "read_status",
    "keyword",
    "ques_id",
    "target_type",
    "region",
    "has_sub"
    # Note: vi_sub DROPPED (full NULL)
]

# Partition Strategy
PARTITION_COLUMNS = ["region"]

# Data Quality Rules
NOT_NULL_COLUMNS = ["url"]  # Critical fields that must not be NULL

# Cleaning Rules
CLEANING_CONFIG = {
    "drop_columns": ["vi_sub"],  # Drop vi_sub column (full NULL)
    "date_format_columns": {
        "posted_date": {
            "input_format": None,  # Will auto-detect from Bronze
            "output_format": "yyyy-MM-dd"  # Standard date format
        }
    },
    "boolean_conversion": {
        "read_status": {
            "true_values": ["1", "true", "True"],
            "false_values": ["0", "false", "False"]
        }
    }
}

# Bronze File Pattern
BRONZE_FILE_PATTERN = r'merged_videos_(\d{8}_\d{6})_([a-f0-9]{8})\.csv'
