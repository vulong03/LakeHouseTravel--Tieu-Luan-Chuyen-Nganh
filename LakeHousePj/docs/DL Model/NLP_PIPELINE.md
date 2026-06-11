# NLP Pipeline — PhoBERT Fine-tuning for Tourism Comments

## Tong quan

Pipeline 3 buoc de nang cap NLP tu `underthesea` (3 muc sentiment) len **PhoBERT fine-tuned** (continuous sentiment + aspect detection + intent classification).

```
Step 1: Weak Labeling       → Auto-label 465K comments bang rules
Step 2: PhoBERT Fine-tune   → Train multi-task model tren weak labels
Step 3: Inference            → Re-score tat ca comments → fact_comment_nlp_v2
```

**File location**: `spark/jobs/dl/nlp/`

---

## Architecture

```
dim_comment (465K comments)
    |
    v
[Step 1] weak_labeling.py
    | underthesea + emoji + keywords → weak labels
    | Filter: use_for_sentiment (conf >= 0.6) OR use_for_intent (conf >= 0.5)
    | + 30K neutral weak samples (conf >= 0.35)
    v
s3://gold/dl_training/nlp_weak_labeled.parquet
    |
    v
[Step 2] train_phobert.py
    | Fine-tune vinai/phobert-base-v2 (Stratified sampling up to 81K)
    | Multi-task: sentiment + aspect + intent
    | MLflow registry: tourism_comment_nlp
    v
MLflow Model Registry
    |
    v
[Step 3] inference_phobert.py
    | Load model from registry
    | Score all 465K comments
    v
gold.gold.fact_comment_nlp_v2
    |
    v
fact_dl_features_job.py (auto-detects v2, falls back to v1)
    |
    v
GRU/LSTM forecasting model
```

---

## Step 1: Weak Labeling (`weak_labeling.py`)

### 3 Tasks duoc gan nhan tu dong

**Sentiment** (3 classes):
- 4 signals duoc ket hop: underthesea result, emoji polarity, keyword matching, exclamation count
- Moi signal cho diem, tong hop thanh confidence score
- Tách biệt kiểm tra độc lập: Đánh dấu `use_for_sentiment = True` khi confidence >= 0.6
- Giữ thêm tối đa 30,000 dòng Neutral có confidence yếu (>=0.35) để chống lệch nhãn (Stratified balanced class training).

**Aspect** (6 classes, multi-label):
- Rule-based keyword matching cho moi aspect
- Threshold: >= 2 keyword matches, hoac 1 match trong comment ngan (<=15 tu)

| Aspect | Vi du keywords |
|---|---|
| `scenery` | canh, view, bien, nui, hoang hon, bai bien |
| `food` | an, do an, ngon, hai san, ca phe, dac san |
| `price` | gia, re, dat, hop ly, tiet kiem, ve |
| `service` | dich vu, nhan vien, phuc vu, le tan, nhiet tinh |
| `transport` | duong, xe, taxi, grab, bay, di chuyen |
| `accommodation` | khach san, hotel, homestay, phong, room |

**Intent** (4 classes):
- Keyword matching + `?` detection
- Đánh dấu `use_for_intent = True` khi confidence >= 0.5
- `recommend`: "nen di", "dang di", "phai di"
- `complain`: "te", "do", "that vong", "lua"
- `question`: "bao nhieu", "o dau", "?", "cho hoi"
- `share`: default khi khong match cac intent khac

### Output
- Path: `s3a://gold/dl_training/nlp_weak_labeled.parquet`
- Columns: comment_sk, comment_text, comment_level, comment_likes, sentiment_label, sentiment_confidence, aspects, intent_label, intent_confidence, aspect_*_pos, aspect_*_neg, use_for_sentiment, use_for_intent, is_weak_label

---

## Step 2: PhoBERT Fine-tuning (`train_phobert.py`)

### Model Architecture

```
Input text → PhoBERT Tokenizer (max_length=128)
    |
    v
PhoBERT backbone (vinai/phobert-base-v2, 135M params)
    | [CLS] token output (768-dim)
    v
    +→ Sentiment Head: Linear(768→128→3) + Softmax
    +→ Aspect Head:    Linear(768→128→18) + Sigmoid (3 sentiment scores per aspect)
    +→ Intent Head:    Linear(768→64→4) + Softmax
```

### Training Config

| Parameter | Value |
|---|---|
| Base model | `vinai/phobert-base-v2` |
| Max sequence length | 128 |
| Batch size | 32 |
| Learning rate | Backbone: 2e-5 \| Heads: 1e-4 |
| Epochs | 5 |
| Warmup ratio | 10% |
| Loss weights | Multi-task loss masking: sentiment 0.5 + aspect 0.1 (ABSA) + intent 0.4 |
| Layer Freezing | Freeze bottom 8 of 12 layers |
| Gradient Accumulation | 4 steps (Effective batch size = 128) |
| Optimizer | AdamW (weight_decay=0.01) |
| Early Stopping | patience=3 epochs (tracks Val Composite F1) |

### MLflow Tracking

- Experiment: `tourism_nlp_phobert_s3`
- Registry: `tourism_comment_nlp`
- Artifacts: model weights, training_loss.png, training_curves.png

---

## Step 3: Inference (`inference_phobert.py`)

### Output Table: `gold.gold.fact_comment_nlp_v2`

| Column | Type | Description |
|---|---|---|
| comment_sk | LONG | FK to dim_comment |
| post_sk | LONG | FK to dim_post |
| province_sk | INT | FK to dim_province |
| comment_date_sk | INT | FK to dim_date |
| **sentiment_score** | DOUBLE | **0.0-1.0 continuous** |
| sentiment_label | STRING | negative/neutral/positive |
| **aspect_scenery** | DOUBLE | Max prob of scenery components |
| **aspect_food** | DOUBLE | Max prob of food components |
| **aspect_price** | DOUBLE | Max prob of price components |
| **aspect_service** | DOUBLE | Max prob of service components |
| **aspect_transport** | DOUBLE | Max prob of transport components |
| **aspect_accommodation** | DOUBLE | Max prob of accommodation components |
| **aspect_scenery_pos / neg** | DOUBLE | Positive / Negative components of aspect scenery |
| **aspect_food_pos / neg** | DOUBLE | Positive / Negative components of aspect food |
| **aspect_price_pos / neg** | DOUBLE | Positive / Negative components of aspect price |
| **aspect_service_pos / neg** | DOUBLE | Positive / Negative components of aspect service |
| **aspect_transport_pos / neg** | DOUBLE | Positive / Negative components of aspect transport |
| **aspect_accommodation_pos / neg** | DOUBLE | Positive / Negative components of aspect accommodation |
| aspect_labels | STRING | Comma-separated list of active aspects (>=0.5) |
| **intent_label** | STRING | recommend/complain/question/share |
| **intent_confidence** | DOUBLE | 0.0-1.0 |
| word_count | INT | from v1 |
| unique_word_ratio | DOUBLE | from v1 |
| emoji_count | INT | from v1 |
| positive_emoji_count | LONG | from v1 |
| negative_emoji_count | LONG | from v1 |
| comment_likes | LONG | from Silver |
| comment_level | INT | 1 or 2 |

---

## Integration voi DL Features

`fact_dl_features_job.py` tu dong detect NLP v2:

```python
use_v2 = _try_nlp_v2(spark)  # Check if table exists and has data

if use_v2:
    # Use continuous sentiment_score + aspect columns
else:
    # Fallback to v1 (3-level sentiment, aspects = 0.0)
```

---

## Cach chay

### Buoc 1: Weak labeling
```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/dl/nlp/weak_labeling.py
```

### Buoc 2: Fine-tune PhoBERT
```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.executor.memory=4g \
    /opt/spark/jobs/dl/nlp/train_phobert.py
```

### Buoc 3: Inference

#### Option A: Chạy bằng Google Colab (Khuyên dùng - GPU nhanh, mất ~10-15 phút)

1. **Export dữ liệu comments ra file Parquet**:
   ```bash
   docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
       --master spark://spark-master:7077 \
       /opt/spark/jobs/dl/nlp/GoogleColab/export_comments_for_colab.py
   ```
   *File xuất ra sẽ nằm tại `LakeHousePj/data/GoogleColab/comments_to_score.parquet`.*

2. **Chạy inference trên Google Colab**:
   - Upload file `comments_to_score.parquet` và file trọng số model `phobert_multi_task.pt` lên Google Colab (sử dụng runtime GPU T4).
   - Copy nội dung hoặc chạy trực tiếp script [colab_inference_phobert.py](file:///d:/CodeStored/Nam_4/TieuLuanCuoiKy/LakeHouse/LakeHousePj/spark/jobs/dl/nlp/GoogleColab/colab_inference_phobert.py).
   - Tải file kết quả `colab_inference_results.parquet` về máy và lưu vào thư mục `LakeHousePj/data/GoogleColab/`.

3. **Import kết quả ngược trở lại Iceberg local**:
   ```bash
   docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
       --master spark://spark-master:7077 \
       /opt/spark/jobs/dl/nlp/GoogleColab/import_colab_results.py
   ```

#### Option B: Chạy local (CPU - Rất chậm)
```bash
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.executor.memory=4g \
    /opt/spark/jobs/dl/nlp/inference_phobert.py
```

---

## Files

```
spark/jobs/dl/nlp/
├── __init__.py
├── config.py              # Keywords, labels, emoji dicts, MLflow config
├── weak_labeling.py       # Step 1: Auto-label comments
├── train_phobert.py       # Step 2: Fine-tune PhoBERT multi-task
└── inference_phobert.py   # Step 3: Score all comments → fact_comment_nlp_v2
```

---

**Last Updated**: June 11, 2026
