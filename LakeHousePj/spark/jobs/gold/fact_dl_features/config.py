"""
Configuration for fact_province_month_dl_features job.
ML-optimized fact table aggregating TikTok + Booking data for Deep Learning.
"""

# ============================================================
# Source Tables
# ============================================================

FACT_COMMENT_NLP_TABLE = "gold.gold.fact_comment_nlp_engagement"
FACT_COMMENT_NLP_V2_TABLE = "gold.gold.fact_comment_nlp_v2"
FACT_PROVINCE_CONTENT_TABLE = "gold.gold.fact_province_content_engagement"
FACT_HOTEL_REVIEW_TABLE = "gold.gold.fact_hotel_review_daily"

DIM_PROVINCE_TABLE = "gold.gold.dim_province"
DIM_DATE_TABLE = "gold.gold.dim_date"
DIM_POST_TABLE = "gold.gold.dim_post"
DIM_HOTEL_TABLE = "gold.gold.dim_hotel"
DIM_COUNTRY_TABLE = "gold.gold.dim_country"
DIM_TRAVEL_TYPE_TABLE = "gold.gold.dim_travel_type"

# ============================================================
# Target Table
# ============================================================

GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "fact_province_month_dl_features"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

PARQUET_EXPORT_PATH = "s3a://gold/ml_training/dl_features.parquet"

# ============================================================
# Description
# ============================================================

SOURCE_DESCRIPTION = (
    f"{FACT_COMMENT_NLP_TABLE} + {FACT_PROVINCE_CONTENT_TABLE} + {FACT_HOTEL_REVIEW_TABLE}"
)

# ============================================================
# Hotness Score Weights (same as XGBoost/RF for fair comparison)
# ============================================================

HOTNESS_WEIGHTS = {
    "volume": 0.25,
    "engagement": 0.35,
    "sentiment": 0.20,
    "emoji_vibe": 0.05,
    "nlp_richness": 0.15,
}

# Vietnam country_sk in dim_country (for domestic_review_ratio)
VIETNAM_COUNTRY_NAME = "Việt Nam"

# Peak tourist months (Tet, summer, national holidays)
PEAK_SEASON_MONTHS = {1, 4, 6, 7, 8, 12}

# Hotel review score thresholds
LOW_SCORE_THRESHOLD = 2.0
HIGH_SCORE_THRESHOLD = 4.0

# ============================================================
# Feature Groups (for documentation / feature selection)
# ============================================================

VOLUME_FEATURES = [
    "total_posts", "total_comments", "total_hotel_reviews",
    "unique_authors", "comments_per_post", "post_frequency",
]

ENGAGEMENT_FEATURES = [
    "avg_likes_per_post", "avg_saves_per_post", "avg_shares_per_post",
    "median_likes_per_post", "p90_likes_per_post",
    "viral_post_ratio", "engagement_score",
]

NLP_FEATURES = [
    "avg_sentiment", "sentiment_std", "positive_ratio", "negative_ratio",
    "sentiment_polarity",
    "avg_word_count", "word_count_std", "avg_unique_word_ratio",
    "emoji_sentiment_ratio", "reply_ratio",
]

HOTEL_FEATURES = [
    "avg_hotel_score", "hotel_score_std", "hotel_review_volume",
    "high_score_ratio", "low_score_ratio",
    "unique_reviewer_countries", "domestic_review_ratio",
    "couple_ratio", "family_ratio", "business_ratio", "solo_ratio",
    "hotel_vol_growth",
]

TEMPORAL_FEATURES = [
    "month_sin", "month_cos", "is_peak_season", "hotness_score",
]

LAG_FEATURES = [
    "hotness_lag_1", "hotness_lag_2", "hotness_lag_3", "hotness_lag_12",
    "hotness_rolling_3m", "hotness_momentum",
]

HOTEL_LAG_FEATURES = [
    "hotel_vol_lag_1", "hotel_vol_lag_2", "hotel_vol_lag_3", "hotel_vol_lag_12",
    "hotel_vol_rolling_3m", "hotel_vol_momentum",
]

CUSTOM_FEATURES = [
    "social_to_booking_ratio", "sentiment_polarity_change", "hotel_vol_std_rolling_3m",
]

ALL_ML_FEATURES = (
    VOLUME_FEATURES + ENGAGEMENT_FEATURES + NLP_FEATURES
    + HOTEL_FEATURES + TEMPORAL_FEATURES + LAG_FEATURES + HOTEL_LAG_FEATURES
    + CUSTOM_FEATURES
)
