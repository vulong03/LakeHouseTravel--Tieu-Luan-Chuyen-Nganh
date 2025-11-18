"""
Hotels Detail Silver Layer Pipeline

2-Step Pattern:
  Step 1 (Transform): Bronze CSV → Scratch Parquet
  Step 2 (Clean & Load): Scratch Parquet → Silver Iceberg

Table:
  - silver.hotels_detail (hotel details with amenities and descriptions)
"""
