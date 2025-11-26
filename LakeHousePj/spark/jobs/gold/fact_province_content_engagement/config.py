"""
Configuration for the fact_province_content_engagement job.
"""

# Source tables
DIM_POST_TABLE = "gold.gold.dim_post"
POST_METRICS_TABLE = "silver.silver.tiktok_post_metadata"

# Target fact table
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "fact_province_content_engagement"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Description for logging
SOURCE_DESCRIPTION = f"{DIM_POST_TABLE} + {POST_METRICS_TABLE}"


