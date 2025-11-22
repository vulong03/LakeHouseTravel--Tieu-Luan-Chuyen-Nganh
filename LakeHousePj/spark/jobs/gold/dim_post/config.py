"""
Gold Layer - Dimension Post Configuration
"""

# Source data (Silver layer)
SOURCE_POSTS_TABLE = "silver.silver.tiktok_post_metadata"
SOURCE_VIDEOS_TABLE = "silver.silver.tiktok_videos"

# Target table
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "dim_post"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key
BUSINESS_KEY = ["post_url"]

# Dimension tables (for FK joins)
DIM_AUTHOR_TABLE = "gold.gold.dim_author"
DIM_PROVINCE_TABLE = "gold.gold.dim_province"
DIM_DATE_TABLE = "gold.gold.dim_date"

