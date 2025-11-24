"""
Gold Layer - Dimension Room Type Configuration

Source: derived from `silver.hotels_reviews` (column `room_type`)
Target: gold.dim_room_type (Iceberg table)
"""

# Source table in Silver catalog
SOURCE_TABLE = "hotels_reviews"
SOURCE_COLUMN = "room_type"

# Source catalog/database for Silver. Use fully-qualified name when referencing tables
# e.g. <catalog>.<database>.<table>
# Adjust these if your Silver namespace uses a different database name (e.g. `default`).
SOURCE_CATALOG = "silver"
SOURCE_DATABASE = "silver"

# Target table (catalog.database.table)
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "dim_room_type"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key: room type name
BUSINESS_KEY = ["room_type_name"]
