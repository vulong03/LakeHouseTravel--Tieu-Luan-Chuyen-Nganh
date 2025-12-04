"""
Configuration for the province_month_features job.
"""

# Source tables
FACT_COMMENT_NLP_TABLE = "gold.gold.fact_comment_nlp_engagement"
DIM_DATE_TABLE = "gold.gold.dim_date"

# Target fact table
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "province_month_features"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Description for logging
SOURCE_DESCRIPTION = f"{FACT_COMMENT_NLP_TABLE} + {DIM_DATE_TABLE}"
