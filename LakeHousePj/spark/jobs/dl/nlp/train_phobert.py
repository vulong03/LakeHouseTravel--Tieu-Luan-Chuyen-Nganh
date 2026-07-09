"""
NLP Pipeline Step 2: Fine-tune PhoBERT for Tourism Comments
============================================================
[OPTIMIZED v2] Applied 4 key optimizations:
  1. Layer Freezing      — Bottom 8/12 transformer layers frozen (~50% fewer params)
  2. Mixed Precision     — FP16 via torch.autocast + GradScaler (GPU only, auto-off on CPU)
  3. Gradient Accum      — 4 steps → effective batch = BATCH_SIZE * 4 (smoother gradients)
  4. Early Stopping      — Tracks Val Sentiment F1, stops after patience=2 epochs
  5. Differential LR     — Backbone: 2e-5 | Heads: 1e-4
  6. Stratified Sampling — Up to 27K/class (~80K total) instead of 5K/class

Multi-task model:
  Head 1: Sentiment classification (negative/neutral/positive)
  Head 2: Aspect detection (multi-label: scenery/food/price/service/transport/accommodation)
  Head 3: Intent classification (recommend/complain/question/share)

Input:  Weak-labeled Parquet from Step 1 (weak_labeling.py)
Output: Fine-tuned PhoBERT model registered in MLflow
"""

import sys
import os

# Set Hugging Face cache directories to a writable location
os.environ['HF_HOME'] = '/tmp/huggingface'
os.environ['TRANSFORMERS_CACHE'] = '/tmp/huggingface'

sys.path.append('/opt/spark/jobs')


import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.metrics import accuracy_score, f1_score, classification_report
from sklearn.model_selection import train_test_split

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

from transformers import AutoTokenizer, AutoModel, get_linear_schedule_with_warmup

import mlflow
import mlflow.pytorch
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

from config import (
    LABELED_PARQUET_PATH,
    PHOBERT_MODEL_NAME, FINE_TUNED_MODEL_NAME,
    MAX_SEQ_LENGTH, TRAIN_TEST_SPLIT, VAL_SPLIT, BATCH_SIZE,
    LEARNING_RATE, EPOCHS, WARMUP_RATIO,
    MLFLOW_TRACKING_URI, MLFLOW_EXPERIMENT,
    SENTIMENT_LABELS, SENTIMENT_TO_ID,
    ASPECT_LABELS,
    INTENT_LABELS, INTENT_TO_ID,
    TRAIN_EVAL_SAMPLES,
)

# ============================================================
# Optimization Hyperparameters
# ============================================================
FREEZE_N_LAYERS     = 8       # Freeze bottom N of 12 transformer layers
GRAD_ACCUM_STEPS    = 4       # Effective batch = BATCH_SIZE * GRAD_ACCUM_STEPS
EARLY_STOP_PATIENCE = 3       # Stop after N epochs with no Val F1 improvement
BACKBONE_LR         = 2e-5    # Lower LR for frozen-adjacent backbone layers
HEAD_LR             = 1e-4    # Higher LR for new classification heads
MAX_PER_CLASS       = 27_000  # ~80K total (27K × 3 sentiment classes)
USE_AMP             = torch.cuda.is_available()  # FP16 only when GPU present


# ============================================================
# Model
# ============================================================

class PhoBERTMultiTask(nn.Module):
    """
    PhoBERT backbone + 2 heads:
      - Sentiment : 3-class softmax
      - Aspect    : 6-class sigmoid (multi-label)
    """
    def __init__(self, model_name, num_sentiments, num_aspects, dropout=0.3):
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

    def forward(self, input_ids, attention_mask):
        outputs = self.backbone(input_ids=input_ids, attention_mask=attention_mask)
        cls = self.dropout(outputs.last_hidden_state[:, 0, :])
        return self.sentiment_head(cls), self.aspect_head(cls)


def apply_layer_freezing(model, n=FREEZE_N_LAYERS):
    """
    Freeze embeddings + bottom N transformer layers.
    Only top (12 - N) layers + all 3 heads remain trainable.
    Prevents Catastrophic Forgetting, cuts trainable params ~50%.
    """
    for param in model.backbone.embeddings.parameters():
        param.requires_grad = False

    for i, layer in enumerate(model.backbone.encoder.layer):
        if i < n:
            for param in layer.parameters():
                param.requires_grad = False

    total     = sum(p.numel() for p in model.parameters())
    trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"  [Freeze] {n}/12 layers frozen | "
          f"Trainable: {trainable:,}/{total:,} ({trainable/total*100:.1f}%)")
    return model


# ============================================================
# Dataset
# ============================================================

class CommentDataset(Dataset):
    def __init__(self, texts, sentiment_ids, aspect_vectors, use_sent, is_weak, tokenizer, max_len):
        self.texts = texts
        self.sentiment_ids = sentiment_ids
        self.aspect_vectors = aspect_vectors
        self.use_sent = use_sent
        self.is_weak = is_weak
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        enc = self.tokenizer(
            str(self.texts[idx]),
            max_length=self.max_len,
            padding='max_length',
            truncation=True,
            return_tensors='pt',
        )
        return {
            'input_ids':      enc['input_ids'].squeeze(),
            'attention_mask': enc['attention_mask'].squeeze(),
            'sentiment':      torch.tensor(self.sentiment_ids[idx], dtype=torch.long),
            'aspects':        torch.tensor(self.aspect_vectors[idx], dtype=torch.float),
            'use_sent':       torch.tensor(self.use_sent[idx], dtype=torch.bool),
            'is_weak':        torch.tensor(self.is_weak[idx], dtype=torch.bool),
        }


# ============================================================
# Data Preparation
# ============================================================

def load_and_prepare_data():
    print("\n[1/4] Loading weak-labeled data...")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("NLP_Load_Data") \
        .config("spark.hadoop.fs.s3a.endpoint",        "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key",      "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key",      "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    pdf = spark.read.parquet(LABELED_PARQUET_PATH).toPandas()
    spark.stop()
    print(f"  Loaded {len(pdf):,} raw samples")

    # Encode labels
    pdf['sentiment_id']  = pdf['sentiment_label'].map(SENTIMENT_TO_ID).fillna(1).astype(int)

    # Handle missing columns if running on old parquet
    if 'use_for_sentiment' not in pdf.columns:
        pdf['use_for_sentiment'] = True
    if 'is_weak_label' not in pdf.columns:
        pdf['is_weak_label'] = False

    def aspects_to_vector(row):
        vec = [0.0] * (len(ASPECT_LABELS) * 2)
        aspects_str = str(row['aspects']) if pd.notna(row['aspects']) else ""
        if not aspects_str:
            return vec
        
        for a in aspects_str.split(','):
            a = a.strip()
            if a in ASPECT_LABELS:
                idx = ASPECT_LABELS.index(a)
                pos_col = f"aspect_{a}_pos"
                neg_col = f"aspect_{a}_neg"
                pos_val = float(row[pos_col]) if pos_col in row and pd.notna(row[pos_col]) else 0.0
                neg_val = float(row[neg_col]) if neg_col in row and pd.notna(row[neg_col]) else 0.0
                
                vec[idx * 2 + 0] = neg_val
                vec[idx * 2 + 1] = pos_val
        return vec

    pdf['aspect_vector'] = pdf.apply(aspects_to_vector, axis=1)

    # Stratified sampling — up to MAX_PER_CLASS per sentiment class (~80K total)
    print("\n  Distribution before sampling:")
    print(pdf['sentiment_label'].value_counts())

    parts = []
    for label in SENTIMENT_LABELS:
        sub = pdf[pdf['sentiment_label'] == label]
        n   = min(len(sub), MAX_PER_CLASS)
        parts.append(sub.sample(n=n, random_state=42) if len(sub) > n else sub)

    pdf_out = pd.concat(parts, ignore_index=True).sample(frac=1, random_state=42)
    
    # Intent distribution logging removed
    
    print(f"\n  After stratified sampling: {len(pdf_out):,}")
    print(pdf_out['sentiment_label'].value_counts())
    return pdf_out


def create_dataloaders(pdf, tokenizer):
    """
    3-way split: Train (75%) / Val (10%) / Test (15%)
    - Val: dùng cho Early Stopping — đỳ phát hiện overfit sớm
    - Test: chỉ dùng cho final evaluation — không chạm trong quá trình train
    - train_eval_loader: subset nhỏ của train để log train_F1 hiệu quả
    """
    print("\n[2/4] Creating DataLoaders (train / val / test 3-way split)...")

    import random as _rnd

    texts         = pdf['comment_text'].tolist()
    sentiment_ids = pdf['sentiment_id'].tolist()
    aspect_vecs   = pdf['aspect_vector'].tolist()
    use_sent      = pdf['use_for_sentiment'].tolist()
    is_weak       = pdf['is_weak_label'].tolist()
    n             = len(texts)

    # Step 1: tách Test set (15% của tổng)
    test_ratio = 1.0 - TRAIN_TEST_SPLIT - VAL_SPLIT          # 0.15
    idx_trainval, idx_test = train_test_split(
        range(n), test_size=test_ratio,
        random_state=42, stratify=sentiment_ids,
    )

    # Step 2: tách Val ra khỏi trainval (~10% tổng = 10/85 trong trainval)
    val_within = VAL_SPLIT / (TRAIN_TEST_SPLIT + VAL_SPLIT)  # ≈ 0.118
    idx_train, idx_val = train_test_split(
        list(idx_trainval), test_size=val_within,
        random_state=42, stratify=[sentiment_ids[i] for i in idx_trainval],
    )

    def make_ds(idx):
        return CommentDataset(
            [texts[i] for i in idx], [sentiment_ids[i] for i in idx],
            [aspect_vecs[i] for i in idx],
            [use_sent[i] for i in idx],
            [is_weak[i] for i in idx],
            tokenizer, MAX_SEQ_LENGTH,
        )

    train_loader     = DataLoader(make_ds(idx_train), batch_size=BATCH_SIZE,
                                  shuffle=True,  num_workers=2, pin_memory=USE_AMP)
    val_loader       = DataLoader(make_ds(idx_val),   batch_size=BATCH_SIZE,
                                  shuffle=False, num_workers=2, pin_memory=USE_AMP)
    test_loader      = DataLoader(make_ds(idx_test),  batch_size=BATCH_SIZE,
                                  shuffle=False, num_workers=2, pin_memory=USE_AMP)

    # Subset nhỏ của train để theo dõi train F1 mỗi epoch (tránh evaluate toàn bộ chậm)
    sample_n = min(TRAIN_EVAL_SAMPLES, len(idx_train))
    idx_train_eval    = _rnd.sample(list(idx_train), sample_n)
    train_eval_loader = DataLoader(make_ds(idx_train_eval), batch_size=BATCH_SIZE,
                                   shuffle=False, num_workers=2, pin_memory=USE_AMP)

    eff_batch = BATCH_SIZE * GRAD_ACCUM_STEPS
    print(f"  Train: {len(idx_train):,} | Val: {len(idx_val):,} | Test: {len(idx_test):,}")
    print(f"  Train-eval sample : {sample_n:,} (for overfit monitoring)")
    print(f"  Effective batch   : {BATCH_SIZE} × {GRAD_ACCUM_STEPS} = {eff_batch}")
    return train_loader, val_loader, test_loader, train_eval_loader


# ============================================================
# Training
# ============================================================

def train_model(model, train_loader, val_loader, test_loader, train_eval_loader, device):
    print("\n[3/4] Training PhoBERT (optimized)...")
    print(f"  FP16 AMP         : {'ON' if USE_AMP else 'OFF (CPU)'}")
    print(f"  Grad accumulation: {GRAD_ACCUM_STEPS} steps")
    print(f"  Early stopping   : patience={EARLY_STOP_PATIENCE} (tracks Val F1)")
    print(f"  LR backbone/heads: {BACKBONE_LR} / {HEAD_LR}")
    print(f"  Split            : Train/Val/Test (75%%/10%%/15%%)")

    # Differential learning rate
    backbone_params = [p for p in model.backbone.parameters() if p.requires_grad]
    head_params = (
        list(model.sentiment_head.parameters())
        + list(model.aspect_head.parameters())
    )
    optimizer = torch.optim.AdamW([
        {"params": backbone_params, "lr": BACKBONE_LR, "weight_decay": 0.01},
        {"params": head_params,     "lr": HEAD_LR,     "weight_decay": 0.01},
    ])

    steps_per_epoch = len(train_loader) // GRAD_ACCUM_STEPS
    total_steps     = steps_per_epoch * EPOCHS
    scheduler = get_linear_schedule_with_warmup(
        optimizer,
        num_warmup_steps=int(total_steps * WARMUP_RATIO),
        num_training_steps=total_steps,
    )

    loss_sent_fn = nn.CrossEntropyLoss()
    loss_asp_fn  = nn.BCEWithLogitsLoss()
    loss_int_fn  = nn.CrossEntropyLoss()

    # AMP GradScaler — no-op on CPU
    amp_scaler = torch.cuda.amp.GradScaler(enabled=USE_AMP)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name=f"phobert_opt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_params({
            "model":               PHOBERT_MODEL_NAME,
            "max_seq_length":      MAX_SEQ_LENGTH,
            "batch_size":          BATCH_SIZE,
            "effective_batch":     BATCH_SIZE * GRAD_ACCUM_STEPS,
            "backbone_lr":         BACKBONE_LR,
            "head_lr":             HEAD_LR,
            "epochs":              EPOCHS,
            "warmup_ratio":        WARMUP_RATIO,
            "freeze_n_layers":     FREEZE_N_LAYERS,
            "grad_accum_steps":    GRAD_ACCUM_STEPS,
            "early_stop_patience": EARLY_STOP_PATIENCE,
            "use_fp16":            USE_AMP,
            "train_size":          len(train_loader.dataset),
            "test_size":           len(test_loader.dataset),
        })

        best_composite   = 0.0
        best_state       = None
        no_improve       = 0
        train_losses     = []
        val_f1_history   = []
        train_f1_history = []   # Track train F1 → quan sát overfit gap

        for epoch in range(EPOCHS):
            model.train()
            epoch_loss = 0.0
            optimizer.zero_grad()

            for step, batch in enumerate(train_loader):
                ids   = batch['input_ids'].to(device)
                mask  = batch['attention_mask'].to(device)
                s_lbl = batch['sentiment'].to(device)
                a_lbl = batch['aspects'].to(device)
                u_sent = batch['use_sent'].to(device)
                is_weak = batch['is_weak'].to(device)

                # FP16 forward
                with torch.autocast(device_type=device.type, enabled=USE_AMP):
                    s_logits, a_logits = model(ids, mask)
                    
                    # FIX ISSUE-04: Apply label smoothing for weak labels
                    loss_s_unreduced = nn.CrossEntropyLoss(reduction='none')(s_logits, s_lbl)
                    loss_s_smooth = nn.CrossEntropyLoss(reduction='none', label_smoothing=0.1)(s_logits, s_lbl)
                    loss_s = torch.where(is_weak, loss_s_smooth, loss_s_unreduced)
                    
                    # FIX ISSUE-02: Mask loss per-task
                    loss_s = (loss_s * u_sent).sum() / max(u_sent.sum(), 1)

                    # FIX ISSUE-05: Adjusted aspect/sentiment loss weight (no intent)
                    loss = (
                        loss_s * 0.8
                        + loss_asp_fn(a_logits, a_lbl) * 0.2
                    ) / GRAD_ACCUM_STEPS

                amp_scaler.scale(loss).backward()

                if (step + 1) % GRAD_ACCUM_STEPS == 0:
                    amp_scaler.unscale_(optimizer)
                    torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
                    amp_scaler.step(optimizer)
                    amp_scaler.update()
                    scheduler.step()
                    optimizer.zero_grad()

                epoch_loss += loss.item() * GRAD_ACCUM_STEPS

            avg_loss = epoch_loss / len(train_loader)
            train_losses.append(avg_loss)

            # --- Evaluate: Val set (dùng cho early stopping) ---
            val_metrics = evaluate(model, val_loader, device)
            val_f1      = val_metrics['sentiment_f1']
            val_f1_history.append(val_f1)
            
            # Use sentiment F1 for composite metric
            val_composite = val_metrics['sentiment_f1']

            # --- Evaluate: Train subset (monitor overfit gap) ---
            train_metrics = evaluate(model, train_eval_loader, device)
            train_f1      = train_metrics['sentiment_f1']
            train_f1_history.append(train_f1)
            overfit_gap   = train_f1 - val_f1  # > 0.1 bắt đầu đáng lo ngại

            gap_flag = " ⚠️ GAP" if overfit_gap > 0.10 else ""
            print(f"  Epoch {epoch+1}/{EPOCHS} | Loss: {avg_loss:.4f} | "
                  f"Train-F1: {train_f1:.4f} | Val-F1: {val_f1:.4f} | "
                  f"Gap: {overfit_gap:+.4f}{gap_flag}")

            mlflow.log_metrics({
                "train_loss":          avg_loss,
                "train_sentiment_f1":  train_f1,
                "val_sentiment_f1":    val_f1,
                "val_sent_accuracy":   val_metrics['sentiment_accuracy'],
                "val_composite_f1":    val_composite,
                "overfit_gap":         overfit_gap,
            }, step=epoch + 1)

            # --- Early Stopping (theo Val F1, không theo Train F1) ---
            if val_composite > best_composite:
                best_composite = val_composite
                best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
                no_improve = 0
                print(f"  ✓ New best Val Composite F1: {best_composite:.4f}")
            else:
                no_improve += 1
                print(f"  No improve {no_improve}/{EARLY_STOP_PATIENCE}")
                if no_improve >= EARLY_STOP_PATIENCE:
                    print(f"\n  ⚑ Early stopping at epoch {epoch+1} (best Val Composite F1={best_composite:.4f})")
                    break

        # Restore best checkpoint
        model.load_state_dict(best_state)
        model.to(device)

        # --- Final evaluation trên TEST SET (không dùng trong training) ---
        print("\n  === Final Evaluation on TEST SET (unbiased) ===")
        final = evaluate(model, test_loader, device, verbose=True)
        mlflow.log_metrics({
            "best_val_composite_f1": best_composite,
            "test_sentiment_f1":  final['sentiment_f1'],
            "test_sentiment_acc": final['sentiment_accuracy'],
            "epochs_trained":     len(train_losses),
            "final_overfit_gap":  train_f1_history[-1] - val_f1_history[-1] if train_f1_history else 0.0,
        })

        # --- Plots: Loss + Train vs Val F1 (Overfit Monitor) ---
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        ax1.plot(range(1, len(train_losses) + 1), train_losses, marker='o', color='steelblue')
        ax1.set(xlabel='Epoch', ylabel='Huber Loss', title='Training Loss')

        epochs_x = range(1, len(val_f1_history) + 1)
        ax2.plot(epochs_x, train_f1_history, marker='o', color='steelblue', label='Train F1 (subset)')
        ax2.plot(epochs_x, val_f1_history,   marker='s', color='green',     label='Val F1')
        if len(val_f1_history) > 1:
            ax2.fill_between(epochs_x, val_f1_history, train_f1_history,
                             alpha=0.20, color='orange', label='Overfit Gap')
        ax2.axhline(best_composite, color='red', linestyle='--', label=f'Best Val Composite F1={best_composite:.4f}')
        ax2.set(xlabel='Epoch', ylabel='Sentiment F1 (macro)',
                title='Train vs Val F1 — Overfit Monitor')
        ax2.legend()
        plt.tight_layout()
        mlflow.log_figure(fig, "training_curves.png")
        plt.close()

        # Register model
        mlflow.pytorch.log_model(model, "model")
        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, FINE_TUNED_MODEL_NAME)

        print(f"\n  Model registered: {FINE_TUNED_MODEL_NAME}")
        print(f"  Best Val Composite F1 : {best_composite:.4f}")
        print(f"  Epochs trained     : {len(train_losses)}")

    return model


# ============================================================
# Evaluation
# ============================================================

def evaluate(model, test_loader, device, verbose=False):
    model.eval()
    all_sent_true, all_sent_pred   = [], []

    with torch.no_grad():
        for batch in test_loader:
            ids  = batch['input_ids'].to(device)
            mask = batch['attention_mask'].to(device)

            with torch.autocast(device_type=device.type, enabled=USE_AMP):
                s_logits, _ = model(ids, mask)

            all_sent_true.extend(batch['sentiment'].numpy())
            all_sent_pred.extend(s_logits.argmax(dim=1).cpu().numpy())

    if verbose:
        print("\n  Sentiment Report:")
        print(classification_report(all_sent_true, all_sent_pred, target_names=SENTIMENT_LABELS))

    return {
        'sentiment_f1':       f1_score(all_sent_true,   all_sent_pred,   average='macro'),
        'sentiment_accuracy': accuracy_score(all_sent_true,   all_sent_pred),
    }


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 70)
    print("NLP Pipeline Step 2: Fine-tune PhoBERT [OPTIMIZED v2]")
    print("=" * 70)
    print(f"Tasks  : Sentiment(3) + Aspect(6)")
    print(f"Optims : LayerFreeze({FREEZE_N_LAYERS}/12) | FP16={USE_AMP} | "
          f"GradAccum={GRAD_ACCUM_STEPS} | EarlyStop(p={EARLY_STOP_PATIENCE})")
    print(f"Start  : {datetime.now()}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device : {device}")

    pdf       = load_and_prepare_data()
    tokenizer = AutoTokenizer.from_pretrained(PHOBERT_MODEL_NAME)

    train_loader, val_loader, test_loader, train_eval_loader = create_dataloaders(pdf, tokenizer)

    model = PhoBERTMultiTask(
        model_name=PHOBERT_MODEL_NAME,
        num_sentiments=len(SENTIMENT_LABELS),
        num_aspects=len(ASPECT_LABELS) * 2,
    ).to(device)

    # === Optimization 1: Layer Freezing ===
    model = apply_layer_freezing(model)

    train_model(model, train_loader, val_loader, test_loader, train_eval_loader, device)

    print(f"\nCompleted : {datetime.now()}")
    print(f"Next step : Run inference_phobert.py to re-score all comments")


if __name__ == "__main__":
    main()
