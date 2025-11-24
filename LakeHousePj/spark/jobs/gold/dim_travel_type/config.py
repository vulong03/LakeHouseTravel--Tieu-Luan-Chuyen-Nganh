"""
Gold Layer - Dimension Traveler Type Configuration

Source: derived from `silver.hotels_reviews` (column `traveler_type`)
Target: gold.dim_travel_type (Iceberg table)
"""

# Source table in Silver catalog
SOURCE_TABLE = "hotels_reviews"
SOURCE_COLUMN = "traveler_type"

# Source catalog/database for Silver
SOURCE_CATALOG = "silver"
SOURCE_DATABASE = "silver"

# Target table (catalog.database.table)
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "dim_travel_type"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key: traveler type name
BUSINESS_KEY = ["traveler_type_name"]
