"""
Gold Layer - Dimension Destination Configuration
"""

# Source
SOURCE_CSV_PATH = "/data/VietNam_Province/List_Destination.csv"

# Target table (catalog.database.table)
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "dim_destination"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Supporting tables
DIM_PROVINCE_TABLE = f"{GOLD_CATALOG}.{GOLD_DATABASE}.dim_province"

# Business key used for deduplication
BUSINESS_KEY = ["destination_name", "province_name"]

# Valid destination types (used for validation / logging)
DESTINATION_TYPES = [
    "temple_pagoda",
    "national_park",
    "waterfall",
    "mountain",
    "lake",
    "cave",
    "historical_site",
    "beach",
    "museum",
    "hot_spring",
    "scenic_spot",
    "village",
    "old_quarter",
    "island",
    "bay",
    "bridge",
    "river_island",
    "other",
]


