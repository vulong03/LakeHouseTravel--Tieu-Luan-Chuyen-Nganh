"""
Hotels Reviews Silver Layer Pipeline

2-Step Pattern:
  Step 1 (Transform): Bronze CSV → Scratch Parquet
  Step 2 (Clean & Load): Scratch Parquet → Silver Iceberg

Table:
  - silver.hotels_reviews (hotel reviews with deduplication)
"""
