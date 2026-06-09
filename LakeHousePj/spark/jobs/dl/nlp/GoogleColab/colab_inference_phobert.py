"""
Google Colab Inference Script for PhoBERT (NLP Step 3)
======================================================
This script runs fine-tuned PhoBERT inference on 861K comments using Google Colab's GPU.
It processes data in batches, calculates sentiment, aspects, and intents, and outputs
a results file `colab_inference_results.parquet`.

How to run on Google Colab:
1. Open a new Colab Notebook and set Runtime -> Change runtime type -> T4 GPU.
2. Upload the exported file `comments_to_score.parquet` and the weights `phobert_multi_task.pt`.
3. Install dependencies in Colab:
   !pip install transformers[torch] emoji pandas pyarrow
4. Run this script:
   !python colab_inference_phobert.py
5. Download `colab_inference_results.parquet` back to your local `LakeHousePj/data/GoogleColab/` directory.
"""

import os
import re
import time
import numpy as np
import pandas as pd
from datetime import datetime

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, AutoModel
import emoji as emoji_lib

# ============================================================
# Configurations
# ============================================================
PHOBERT_MODEL_NAME = "vinai/phobert-base-v2"
MAX_SEQ_LENGTH = 128
BATCH_SIZE = 128  # High batch size for fast GPU inference
ASPECT_THRESHOLD = 0.5

SENTIMENT_LABELS = ["negative", "neutral", "positive"]
ASPECT_LABELS = ["scenery", "food", "price", "service", "transport", "accommodation"]
INTENT_LABELS = ["recommend", "complain", "question", "share"]

# Emoji sets for sentiment evaluation (Vietnam tourism comments context)
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

# ============================================================
# Model Class
# ============================================================
class PhoBERTMultiTask(nn.Module):
    def __init__(self, model_name, num_sentiments, num_aspects, num_intents, dropout=0.3):
        super().__init__()
        self.backbone = AutoModel.from_pretrained(model_name)
        hidden = self.backbone.config.hidden_size
        self.dropout = nn.Dropout(dropout)

        self.sentiment_head = nn.Sequential(
            nn.Linear(hidden, 128), nn.ReLU(), nn.Dropout(dropout), nn.Linear(128, num_sentiments),
        )
        self.aspect_head = nn.Sequential(
            nn.Linear(hidden, 128), nn.ReLU(), nn.Dropout(dropout), nn.Linear(128, num_aspects),
        )
        self.intent_head = nn.Sequential(
            nn.Linear(hidden, 64), nn.ReLU(), nn.Dropout(dropout), nn.Linear(64, num_intents),
        )

    def forward(self, input_ids, attention_mask):
        outputs = self.backbone(input_ids=input_ids, attention_mask=attention_mask)
        cls = self.dropout(outputs.last_hidden_state[:, 0, :])
        return self.sentiment_head(cls), self.aspect_head(cls), self.intent_head(cls)


# ============================================================
# Dataset for Inference
# ============================================================
class InferenceDataset(Dataset):
    def __init__(self, texts):
        self.texts = texts

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        return str(self.texts[idx])


def clean_text(t):
    s = str(t) if t else ""
    s = re.sub(r'http\S+|www\S+|@\w+', '', s)
    return ' '.join(s.split()).strip()


def main():
    print("=" * 70)
    print("Google Colab: PhoBERT Batch Inference")
    print("=" * 70)

    # Check GPU
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")
    if device.type != 'cuda':
        print("⚠️ WARNING: Running on CPU. This will be slow! Set Colab runtime to GPU (T4/L4).")

    # Load Data
    input_file = "comments_to_score.parquet"
    if not os.path.exists(input_file):
        print(f"❌ ERROR: File '{input_file}' not found. Please upload it to Colab.")
        return

    print(f"\n[1/4] Loading comments from {input_file}...")
    df = pd.read_parquet(input_file)
    print(f"  Loaded {len(df):,} rows.")

    # Load Model
    weights_path = "phobert_multi_task.pt"
    if not os.path.exists(weights_path):
        print(f"❌ ERROR: Weights '{weights_path}' not found. Please upload it to Colab.")
        return

    print(f"\n[2/4] Loading PhoBERT model and fine-tuned weights...")
    tokenizer = AutoTokenizer.from_pretrained(PHOBERT_MODEL_NAME)
    model = PhoBERTMultiTask(
        model_name=PHOBERT_MODEL_NAME,
        num_sentiments=len(SENTIMENT_LABELS),
        num_aspects=len(ASPECT_LABELS),
        num_intents=len(INTENT_LABELS),
    )
    
    try:
        model.load_state_dict(torch.load(weights_path, map_location=device))
        model = model.to(device)
        model.eval()
        print("  ✓ Model loaded successfully and set to eval mode.")
    except Exception as e:
        print(f"❌ ERROR loading model state dict: {e}")
        return

    # Pre-calculate counts and clean texts
    print(f"\n[3/4] Pre-processing comments and extracting basic stats...")
    texts_list = df['comment_text'].tolist()
    
    print("  Calculating word counts, emoji counts, and unique word ratios...")
    word_counts = []
    unique_word_ratios = []
    emoji_counts = []
    positive_emoji_counts = []
    negative_emoji_counts = []
    
    for t in texts_list:
        if not t or pd.isna(t):
            word_counts.append(0)
            unique_word_ratios.append(0.0)
            emoji_counts.append(0)
            positive_emoji_counts.append(0)
            negative_emoji_counts.append(0)
            continue
        
        text_str = str(t)
        text_lower = text_str.lower()
        words = text_lower.split()
        wc = len(words)
        unique_wc = len(set(words))
        ratio = float(unique_wc) / wc if wc > 0 else 0.0
        
        # Extract emoji counts
        emojis = [c for c in text_str if c in emoji_lib.EMOJI_DATA]
        ec = len(emojis)
        pos_ec = sum(1 for e in emojis if e in POSITIVE_EMOJIS)
        neg_ec = sum(1 for e in emojis if e in NEGATIVE_EMOJIS)
        
        word_counts.append(wc)
        unique_word_ratios.append(round(ratio, 4))
        emoji_counts.append(ec)
        positive_emoji_counts.append(pos_ec)
        negative_emoji_counts.append(neg_ec)
    
    print("  Cleaning texts...")
    cleaned_texts = [clean_text(t) for t in texts_list]

    # Batch Inference
    print(f"\n[4/4] Starting GPU inference (Batch Size: {BATCH_SIZE})...")
    
    dataset = InferenceDataset(cleaned_texts)
    
    def collate_fn(batch):
        return tokenizer(
            batch,
            max_length=MAX_SEQ_LENGTH,
            padding=True,
            truncation=True,
            return_tensors='pt'
        )

    # Use 2 workers for faster data loading
    dataloader = DataLoader(
        dataset, 
        batch_size=BATCH_SIZE, 
        shuffle=False, 
        collate_fn=collate_fn, 
        num_workers=2
    )

    results = []
    total_batches = len(dataloader)
    start_time = time.time()
    
    # Disable gradient computation
    with torch.no_grad():
        for batch_idx, encoding in enumerate(dataloader):
            input_ids = encoding['input_ids'].to(device)
            attention_mask = encoding['attention_mask'].to(device)

            # Auto-cast to FP16 if on GPU
            with torch.autocast(device_type=device.type, enabled=(device.type == 'cuda')):
                sent_logits, aspect_logits, intent_logits = model(input_ids, attention_mask)

            # Post-process logits
            sent_probs = torch.softmax(sent_logits, dim=1).cpu().numpy()
            aspect_probs = torch.sigmoid(aspect_logits).cpu().numpy()
            intent_probs = torch.softmax(intent_logits, dim=1).cpu().numpy()

            for j in range(len(input_ids)):
                sp = sent_probs[j]
                ap = aspect_probs[j]
                ip = intent_probs[j]
                intent_idx = int(ip.argmax())
                
                aspect_labels = [
                    ASPECT_LABELS[k]
                    for k, v in enumerate(ap)
                    if v >= ASPECT_THRESHOLD
                ]

                res = {
                    # FIX ISSUE-08: Use bipolar formula for intuitive sentiment score
                    "sentiment_score": round(float((sp[2] - sp[0] + 1) / 2), 4),
                    "sentiment_label": SENTIMENT_LABELS[int(sp.argmax())],
                    "sentiment_confidence": round(float(sp.max()), 4),
                    "sentiment_negative_prob": round(float(sp[0]), 4),
                    "sentiment_neutral_prob": round(float(sp[1]), 4),
                    "sentiment_positive_prob": round(float(sp[2]), 4),

                    "aspect_scenery":       round(float(ap[0]), 4),
                    "aspect_food":          round(float(ap[1]), 4),
                    "aspect_price":         round(float(ap[2]), 4),
                    "aspect_service":       round(float(ap[3]), 4),
                    "aspect_transport":     round(float(ap[4]), 4),
                    "aspect_accommodation": round(float(ap[5]), 4),
                    "aspect_labels": ",".join(aspect_labels),

                    "intent_label":      INTENT_LABELS[intent_idx],
                    "intent_confidence": round(float(ip[intent_idx]), 4),
                }
                results.append(res)

            # Print progress every 50 batches
            if (batch_idx + 1) % 50 == 0 or (batch_idx + 1) == total_batches:
                processed_rows = len(results)
                pct = (processed_rows / len(df)) * 100
                elapsed = time.time() - start_time
                speed = processed_rows / elapsed
                eta_sec = (len(df) - processed_rows) / speed if speed > 0 else 0
                eta_min = eta_sec / 60
                print(f"  Processed {processed_rows:,}/{len(df):,} ({pct:.1f}%) | "
                      f"Speed: {speed:.1f} rows/s | ETA: {eta_min:.1f} mins")

    print(f"\nInference completed in { (time.time() - start_time)/60:.1f} minutes.")

    # Combine back to DataFrame
    print("\nAssembling final DataFrame...")
    results_df = pd.DataFrame(results)
    
    # Add pre-computed counts and ratios
    results_df["word_count"] = word_counts
    results_df["unique_word_ratio"] = unique_word_ratios
    results_df["emoji_count"] = emoji_counts
    results_df["positive_emoji_count"] = positive_emoji_counts
    results_df["negative_emoji_count"] = negative_emoji_counts

    # Join results with original keys
    df_out = pd.concat([df.reset_index(drop=True), results_df], axis=1)
    
    # Drop raw comment_text to save bandwidth and storage
    if "comment_text" in df_out.columns:
        df_out = df_out.drop(columns=["comment_text"])

    output_file = "colab_inference_results.parquet"
    print(f"Saving final results to '{output_file}'...")
    df_out.to_parquet(output_file, index=False, compression="snappy")
    
    print("\n🎉 ALL DONE SUCCESS!")
    print(f"  Output path: {output_file}")
    print("  Next Step: Download this file and put it in 'LakeHousePj/data/GoogleColab/colab_inference_results.parquet'")
    print("  Then run import_colab_results.py locally to write it to your Iceberg table.")


if __name__ == "__main__":
    main()
