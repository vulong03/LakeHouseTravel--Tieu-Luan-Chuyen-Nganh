"""
Configuration for fact_comment_nlp_engagement job
ML Feature Engineering - Comment-level NLP + Engagement Metrics
"""

# Source tables
DIM_COMMENT_TABLE = "gold.gold.dim_comment"
DIM_POST_TABLE = "gold.gold.dim_post"
SILVER_COMMENTS_TABLE = "silver.silver.tiktok_post_comments"

# Target fact table
GOLD_CATALOG = "gold"
GOLD_DATABASE = "gold"
GOLD_TABLE = "fact_comment_nlp_engagement"
GOLD_TABLE_FULL = f"{GOLD_CATALOG}.{GOLD_DATABASE}.{GOLD_TABLE}"

# Business key (for MERGE matching)
BUSINESS_KEY = ["comment_sk"]

# Business columns for row_checksum calculation (all except keys & metadata)
BUSINESS_COLUMNS = [
    # NLP features
    "word_count",
    "unique_word_ratio",
    "exclamation_count",
    "sentiment_score",
    "sentiment_label",
    "emoji_count",
    "positive_emoji_count",
    "negative_emoji_count",
    # Comment metrics
    "comment_likes",
    "comment_level",
]

# Description for logging
SOURCE_DESCRIPTION = f"{DIM_COMMENT_TABLE} + {SILVER_COMMENTS_TABLE}"

# NLP Configuration
MAX_TEXT_LENGTH = 5000  # Truncate texts longer than this
DEFAULT_NLP_VALUES = {
    "word_count": 0,
    "unique_word_ratio": 0.0,
    "exclamation_count": 0,
    "sentiment_score": 0.0,
    "sentiment_label": "neutral",
    "emoji_count": 0,
    "positive_emoji_count": 0,
    "negative_emoji_count": 0,
}

# Emoji classification
POSITIVE_EMOJIS = {
    # Love / happy / positive vibes
    '😍','❤️','💕','🥰','😘','😊','😃','😄','🤩','😎','🙂','😇','😁','😌','🤗','☺️',
    '👍','👏','🙌','💪','🔥','✨','🌟','⭐','💯','🎉','🥳','🤝','👌','🫶','💖','💗','💘',
    
    # Fun / laugh
    '😂','🤣','😹','🥹','😺','😆','😄','😝','😜','🤪',

    # Travel / nature / aesthetic (rất hay dùng trên TikTok Việt Nam)
    '🏖️','🌊','🏝️','🌅','🌄','🗻','🏔️','🌈','🍃','🌸','🌺','🌼','🌻','✨','💐','🌷',

    # Trendy TikTok symbols
    '🤍','💙','💚','💛','🩵','🩷','🔥','⭐','🌟','💯','💫',
}

NEGATIVE_EMOJIS = {
    # Sad / crying
    '😢','😭','😞','😔','😟','😕','🙁','☹️','😣','😖','😫','😩','🥺',

    # Angry / hate
    '😡','😠','🤬','😤','💢','👿','😾',

    # Disgust / sick
    '🤮','😷','🤢','🤧','🥵','🥶',

    # Broken heart / drama
    '💔','👎','🙅','😒','😑','😐','🫤',

    # Stress / confusion / shock
    '😨','😰','😱','😳','😵','😖','😣',
}
