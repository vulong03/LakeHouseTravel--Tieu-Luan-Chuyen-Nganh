"""
TikTok Comments Silver Layer Pipeline

2-Step Pattern:
  Step 1 (Transform): Bronze CSV → Scratch Parquet
  Step 2 (Clean & Load): Scratch Parquet → Silver Iceberg

Tables:
  1. silver.tiktok_post_metadata (posts info from header)
  2. silver.tiktok_post_comments (comments data from CSV)
"""
