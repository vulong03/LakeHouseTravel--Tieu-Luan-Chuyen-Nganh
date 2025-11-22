"""
Gold Layer - Dimension Author Configuration
"""

# Source data (Silver layer)
SOURCE_SILVER_TABLE = "silver.silver.tiktok_post_metadata"

# Target table
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "dim_author"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key
BUSINESS_KEY = ["author_tag"]

