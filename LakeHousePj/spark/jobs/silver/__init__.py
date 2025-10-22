"""
Silver Layer Jobs
=================

Data cleaning, validation, and transformation (Bronze → Silver).

Purpose:
    - Read from Bronze Iceberg tables
    - Apply data quality checks (nulls, duplicates, ranges)
    - Transform data (clean text, standardize values)
    - Write to Silver layer (s3a://silver/)

Jobs will be added here:
    - clean_hotels.py (validate hotels data)
    - clean_reviews.py (clean review text, sentiment)
    - merge_reviews.py (join Booking + TikTok reviews)
"""
