"""
Gold Layer - Fact: fact_hotel_review_daily Configuration

Source: silver.silver.hotels_reviews
Target: gold.fact_hotel_review_daily
"""

# Source table in Silver catalog
SOURCE_TABLE = "hotels_reviews"
SOURCE_CATALOG = "silver"
SOURCE_DATABASE = "silver"

# Target table (catalog.database.table)
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "fact_hotel_review_daily"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Dimension tables used for FK joins
DIM_HOTEL_TABLE = "gold.gold.dim_hotel"
DIM_TRAVEL_TYPE_TABLE = "gold.gold.dim_travel_type"
DIM_ROOM_TYPE_TABLE = "gold.gold.dim_room_type"
DIM_COUNTRY_TABLE = "gold.gold.dim_country"
DIM_DATE_TABLE = "gold.gold.dim_date"

# Score thresholds (assumptions - adjust if you prefer different cutoffs)
LOW_SCORE_THRESHOLD = 2.0
HIGH_SCORE_THRESHOLD = 4.0
