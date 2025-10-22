"""
Bronze Layer Jobs
=================

Raw data ingestion from source files to Bronze Iceberg tables.

Purpose:
    - Read CSV/JSON files from data/raw/
    - Minimal validation (schema check only)
    - Write as-is to Bronze layer (s3a://bronze/)
    - Add ingestion metadata (timestamp, source)

Jobs will be added here:
    - ingest_booking_hotels.py
    - ingest_booking_reviews.py
    - ingest_tiktok_reviews.py
"""
