"""
NLP Pipeline Configuration
Weak Labeling + PhoBERT Fine-tuning for Vietnamese Tourism Comments
"""

# ============================================================
# Source
# ============================================================
DIM_COMMENT_TABLE = "gold.gold.dim_comment"
SILVER_COMMENTS_TABLE = "silver.silver.tiktok_post_comments"

# ============================================================
# Weak Labeling Output
# ============================================================
LABELED_PARQUET_PATH = "s3a://gold/dl_training/nlp_weak_labeled.parquet"

# ============================================================
# PhoBERT Model
# ============================================================
PHOBERT_MODEL_NAME = "vinai/phobert-base-v2"
FINE_TUNED_MODEL_NAME = "tourism_comment_nlp"
MAX_SEQ_LENGTH = 128
TRAIN_TEST_SPLIT = 0.75   # Train proportion (75% of total)
VAL_SPLIT        = 0.10   # Val proportion  (10% of total)
# Remaining 15% → Test set (unbiased final evaluation)
TRAIN_EVAL_SAMPLES = 3000  # Subset size used to track train F1 per epoch (performance)
BATCH_SIZE = 32
LEARNING_RATE = 2e-5
EPOCHS = 5
WARMUP_RATIO = 0.1

# ============================================================
# MLflow
# ============================================================
MLFLOW_TRACKING_URI = "http://mlflow:5000"
MLFLOW_EXPERIMENT = "tourism_nlp_phobert_s3"

# ============================================================
# Sentiment Labels
# ============================================================
SENTIMENT_LABELS = ["negative", "neutral", "positive"]
SENTIMENT_TO_ID = {l: i for i, l in enumerate(SENTIMENT_LABELS)}

# ============================================================
# Aspect Labels (multi-label)
# ============================================================
ASPECT_LABELS = [
    "scenery",       # cảnh đẹp, view, thiên nhiên
    "food",          # đồ ăn, ẩm thực, quán ăn
    "price",         # giá cả, rẻ, đắt, hợp lý
    "service",       # dịch vụ, nhân viên, phục vụ
    "transport",     # di chuyển, đường đi, xe
    "accommodation", # khách sạn, homestay, phòng
]

# ============================================================
# Intent Labels
# ============================================================
INTENT_LABELS = [
    "recommend",  # khuyên nghị, nên đi, đáng đi
    "complain",   # phàn nàn, tệ, dở
    "question",   # hỏi thông tin, bao nhiêu, ở đâu
    "share",      # chia sẻ trải nghiệm, vừa đi về
]
INTENT_TO_ID = {l: i for i, l in enumerate(INTENT_LABELS)}

# ============================================================
# Keyword Rules for Weak Labeling
# ============================================================

POSITIVE_KEYWORDS = [
    "đẹp", "tuyệt vời", "tuyệt", "xuất sắc", "hay", "thích", "yêu",
    "iu", "amazing", "beautiful", "love", "perfect", "chill",
    "đỉnh", "xịn", "sịn", "nên đi", "đáng đi", "recommend",
    "phải đi", "nhất định", "10 điểm", "mê", "ghiền", "nghiện",
    "quá đã", "quá đẹp", "quá ngon", "siêu", "max", "ơi",
    "hài lòng", "ưng", "ok", "ổn", "tốt", "nice", "good", "great",
    "wow", "xỉu", "phê", "sống ảo", "check in", "checkin",
]

NEGATIVE_KEYWORDS = [
    "tệ", "dở", "chán", "xấu", "bẩn", "đắt", "lừa đảo", "lừa",
    "thất vọng", "hối hận", "không nên", "đừng đi", "tránh",
    "scam", "bad", "terrible", "worst", "dirty", "rip off",
    "hư", "hỏng", "kém", "tồi", "phí tiền", "mất tiền",
    "thối", "tanh", "ồn", "đông", "nguy hiểm", "sợ",
    "nhạt", "không ok", "không ổn", "dở ẹc",
]

ASPECT_KEYWORD_MAP = {
    "scenery": [
        "cảnh", "view", "biển", "núi", "sông", "hồ", "thác", "đảo",
        "thiên nhiên", "hoàng hôn", "bình minh", "sunset", "sunrise",
        "đẹp", "phong cảnh", "bãi biển", "rừng", "hang", "động",
        "ruộng bậc thang", "đồi", "vịnh",
    ],
    "food": [
        "ăn", "đồ ăn", "ẩm thực", "quán", "nhà hàng", "món",
        "ngon", "bún", "phở", "bánh", "hải sản", "cơm",
        "uống", "café", "cà phê", "bia", "nước", "chè",
        "đặc sản", "food", "yummy", "delicious",
    ],
    "price": [
        "giá", "rẻ", "đắt", "hợp lý", "phải chăng", "tiết kiệm",
        "free", "miễn phí", "vé", "chi phí", "budget", "tốn",
        "phí", "tiền", "vnđ", "k", "triệu", "chặt chém",
    ],
    "service": [
        "dịch vụ", "nhân viên", "phục vụ", "lễ tân", "hướng dẫn",
        "service", "staff", "guide", "thái độ", "chuyên nghiệp",
        "nhiệt tình", "tử tế", "tốt bụng", "nhanh", "chậm",
    ],
    "transport": [
        "đường", "xe", "taxi", "grab", "bus", "xe máy", "ô tô",
        "bay", "tàu", "đi lại", "di chuyển", "xa", "gần",
        "đường đi", "bản đồ", "map", "cách", "km",
    ],
    "accommodation": [
        "khách sạn", "hotel", "homestay", "resort", "phòng", "room",
        "giường", "sạch", "đẹp", "tiện nghi", "view phòng",
        "check in", "nhận phòng", "trả phòng", "đặt phòng", "booking",
    ],
}

# Emoji signals reuse from existing config
POSITIVE_EMOJIS = {
    '😍','❤️','💕','🥰','😘','😊','😃','😄','🤩','😎','🙂','😇','😁','😌','🤗','☺️',
    '👍','👏','🙌','💪','🔥','✨','🌟','⭐','💯','🎉','🥳','🤝','👌','🫶','💖','💗','💘',
    '😂','🤣','😹','🥹','😺','😆','😝','😜','🤪',
    '🏖️','🌊','🏝️','🌅','🌄','🗻','🏔️','🌈','🍃','🌸','🌺','🌼','🌻','💐','🌷',
    '🤍','💙','💚','💛','🩵','🩷','💫',
}

NEGATIVE_EMOJIS = {
    '😢','😭','😞','😔','😟','😕','🙁','☹️','😣','😖','😫','😩','🥺',
    '😡','😠','🤬','😤','💢','👿','😾',
    '🤮','😷','🤢','🤧','🥵','🥶',
    '💔','👎','🙅','😒','😑','😐','🫤',
    '😨','😰','😱','😳','😵',
}
