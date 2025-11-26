"""
Gold Layer - Dimension Hotel Configuration
"""

# Source data (Silver layer)
SOURCE_HOTELS_TABLE = "silver.silver.hotels_detail"

# Target table
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "dim_hotel"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key
BUSINESS_KEY = ["hotel_url"]

# Dimension tables (for FK joins)
DIM_PROVINCE_TABLE = "gold.gold.dim_province"
