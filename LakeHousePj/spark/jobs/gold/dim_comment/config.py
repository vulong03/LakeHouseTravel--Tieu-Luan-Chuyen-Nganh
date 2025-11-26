"""
Gold Layer - Dimension Comment Configuration
"""

# Source data (Silver layer)
SOURCE_COMMENTS_TABLE = "silver.silver.tiktok_post_comments"

# Target table
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "dim_comment"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key (composite key for unique comment identification)
BUSINESS_KEY = ["post_url_nk", "stt"]

# Business columns for row_checksum calculation
BUSINESS_COLUMNS = [
    "post_url_nk",
    "stt",
    "commenter_tag",
    "commenter_name",
    "commenter_url",
    "comment_text",
    "comment_level",
    "replied_to_tag_name"
]

# Dimension tables (for FK joins)
DIM_POST_TABLE = "gold.gold.dim_post"
DIM_DATE_TABLE = "gold.gold.dim_date"

