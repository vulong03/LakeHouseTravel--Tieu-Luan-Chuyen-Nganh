"""
Gold Layer Jobs
===============

Analytics-ready aggregations and features (Silver → Gold).

Purpose:
    - Read from Silver Iceberg tables
    - Aggregate data (group by, sum, avg)
    - Create business metrics
    - Write to Gold layer (s3a://gold/)

Jobs will be added here:
    - hotel_ratings_agg.py (aggregate hotel ratings by city/category)
    - sentiment_analysis.py (TikTok sentiment scores by hotel)
"""
