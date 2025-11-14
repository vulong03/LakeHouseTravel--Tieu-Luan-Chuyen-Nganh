"""
Silver Layer Package - REFACTORED
2-Phase Execution Strategy:
  Phase 1: Light jobs (hotels_detail, hotels_list, tiktok_videos) - Parallel
  Phase 2: Heavy jobs (hotels_reviews, tiktok_comments) - Sequential with full resources
  
Each dataset has 2-step pipeline:
  Step 1: Transform & Clean
  Step 2: Load to Iceberg with UPSERT/APPEND
"""
