"""
Google Colab Training Script for PhoBERT (NLP Step 2)
======================================================
This script is adapted directly from local train_phobert.py to run on Google Colab GPU.
It uses the pre-extracted stratified dataset (nlp_weak_labeled_81k.csv) 
and does not require Spark or Postgres connections.

How to run on Google Colab:
1. Open a Google Colab notebook.
2. Set Runtime type to GPU (T4 GPU is recommended).
3. Upload 'nlp_weak_labeled_81k.csv' and this script 'colab_train_phobert.py'.
4. Install dependencies:
   !pip install transformers[torch] emoji underthesea pandas scikit-learn matplotlib mlflow
5. Run the script:
   !python colab_train_phobert.py
"""

import os
import re
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.metrics import accuracy_score, f1_score, classification_report
from sklearn.model_selection import train_test_split

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

from transformers import AutoTokenizer, AutoModel, get_linear_schedule_with_warmup
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

try:
    import mlflow
    import mlflow.pytorch
    mlflow_available = True
except ImportError:
    mlflow_available = False
    class DummyContextManager:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): return False
    
    class DummyMLflow:
        def start_run(self, *args, **kwargs): return DummyContextManager()
        def log_params(self, *args, **kwargs): pass
        def log_metrics(self, *args, **kwargs): pass
        def set_experiment(self, *args, **kwargs): pass
        
    mlflow = DummyMLflow()
    print("Warning: mlflow package is not installed. Script will run without MLflow logging.")


# ============================================================
# Configurations
# ============================================================
PHOBERT_MODEL_NAME = "vinai/phobert-base-v2"
MAX_SEQ_LENGTH = 128
TRAIN_TEST_SPLIT = 0.75   # 75% train
VAL_SPLIT        = 0.10   # 10% val
TRAIN_EVAL_SAMPLES = 3000

BATCH_SIZE = 32
LEARNING_RATE = 2e-5
EPOCHS = 5
WARMUP_RATIO = 0.1

FREEZE_N_LAYERS     = 8       # Freeze bottom 8 of 12 transformer layers
GRAD_ACCUM_STEPS    = 4       # Effective batch = 32 * 4 = 128
EARLY_STOP_PATIENCE = 3       # Stop if no Val F1 improvement for 3 epochs
BACKBONE_LR         = 2e-5
HEAD_LR             = 1e-4

SENTIMENT_LABELS = ["negative", "neutral", "positive"]
SENTIMENT_TO_ID = {l: i for i, l in enumerate(SENTIMENT_LABELS)}
ASPECT_LABELS = ["scenery", "food", "price", "service", "transport", "accommodation"]
INTENT_LABELS = ["recommend", "complain", "question", "share"]
INTENT_TO_ID = {l: i for i, l in enumerate(INTENT_LABELS)}

USE_AMP = torch.cuda.is_available()

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

def apply_layer_freezing(model, n=FREEZE_N_LAYERS):
    for param in model.backbone.embeddings.parameters():
        param.requires_grad = False
    for i, layer in enumerate(model.backbone.encoder.layer):
        if i < n:
            for param in layer.parameters():
                param.requires_grad = False
    total     = sum(p.numel() for p in model.parameters())
    trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"  [Freeze] {n}/12 layers frozen | Trainable: {trainable:,}/{total:,} ({trainable/total*100:.1f}%)")
    return model

# ============================================================
# PyTorch Dataset
# ============================================================
class CommentDataset(Dataset):
    def __init__(self, texts, sentiment_ids, aspect_vectors, intent_ids, use_sent, use_int, is_weak, tokenizer, max_len):
        self.texts = texts
        self.sentiment_ids = sentiment_ids
        self.aspect_vectors = aspect_vectors
        self.intent_ids = intent_ids
        self.use_sent = use_sent
        self.use_int = use_int
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
            'intent':         torch.tensor(self.intent_ids[idx], dtype=torch.long),
            'use_sent':       torch.tensor(self.use_sent[idx], dtype=torch.bool),
            'use_int':        torch.tensor(self.use_int[idx], dtype=torch.bool),
            'is_weak':        torch.tensor(self.is_weak[idx], dtype=torch.bool),
        }

# ============================================================
# Main Training Loop
# ============================================================
def load_and_prepare_data(csv_path):
    print(f"\n[1/4] Loading CSV dataset from {csv_path}...")
    pdf = pd.read_csv(csv_path)
    print(f"  Loaded {len(pdf):,} samples")

    # Encode labels
    pdf['sentiment_id']  = pdf['sentiment_label'].map(SENTIMENT_TO_ID).fillna(1).astype(int)
    pdf['intent_id']     = pdf['intent_label'].map(INTENT_TO_ID).fillna(3).astype(int)

    # Handle missing columns if running on old parquet
    if 'use_for_sentiment' not in pdf.columns:
        pdf['use_for_sentiment'] = True
    if 'use_for_intent' not in pdf.columns:
        pdf['use_for_intent'] = True
    if 'is_weak_label' not in pdf.columns:
        pdf['is_weak_label'] = False

    def aspects_to_vector(s):
        vec = [0] * len(ASPECT_LABELS)
        for a in (str(s) if pd.notna(s) else "").split(','):
            a = a.strip()
            if a in ASPECT_LABELS:
                vec[ASPECT_LABELS.index(a)] = 1
        return vec

    pdf['aspect_vector'] = pdf['aspects'].apply(aspects_to_vector)
    return pdf

def create_dataloaders(pdf, tokenizer):
    print("\n[2/4] Creating DataLoaders (train / val / test 3-way split)...")
    texts         = pdf['comment_text'].tolist()
    sentiment_ids = pdf['sentiment_id'].tolist()
    aspect_vecs   = pdf['aspect_vector'].tolist()
    intent_ids    = pdf['intent_id'].tolist()
    use_sent      = pdf['use_for_sentiment'].tolist()
    use_int       = pdf['use_for_intent'].tolist()
    is_weak       = pdf['is_weak_label'].tolist()
    n             = len(texts)

    test_ratio = 1.0 - TRAIN_TEST_SPLIT - VAL_SPLIT
    idx_trainval, idx_test = train_test_split(
        range(n), test_size=test_ratio,
        random_state=42, stratify=sentiment_ids,
    )

    val_within = VAL_SPLIT / (TRAIN_TEST_SPLIT + VAL_SPLIT)
    idx_train, idx_val = train_test_split(
        list(idx_trainval), test_size=val_within,
        random_state=42, stratify=[sentiment_ids[i] for i in idx_trainval],
    )

    def make_ds(idx):
        return CommentDataset(
            [texts[i] for i in idx], [sentiment_ids[i] for i in idx],
            [aspect_vecs[i] for i in idx], [intent_ids[i] for i in idx],
            [use_sent[i] for i in idx], [use_int[i] for i in idx],
            [is_weak[i] for i in idx],
            tokenizer, MAX_SEQ_LENGTH,
        )

    train_loader     = DataLoader(make_ds(idx_train), batch_size=BATCH_SIZE, shuffle=True,  num_workers=2, pin_memory=USE_AMP)
    val_loader       = DataLoader(make_ds(idx_val),   batch_size=BATCH_SIZE, shuffle=False, num_workers=2, pin_memory=USE_AMP)
    test_loader      = DataLoader(make_ds(idx_test),  batch_size=BATCH_SIZE, shuffle=False, num_workers=2, pin_memory=USE_AMP)

    sample_n = min(TRAIN_EVAL_SAMPLES, len(idx_train))
    import random
    idx_train_eval    = random.sample(list(idx_train), sample_n)
    train_eval_loader = DataLoader(make_ds(idx_train_eval), batch_size=BATCH_SIZE, shuffle=False, num_workers=2, pin_memory=USE_AMP)

    print(f"  Train: {len(idx_train):,} | Val: {len(idx_val):,} | Test: {len(idx_test):,}")
    return train_loader, val_loader, test_loader, train_eval_loader

def evaluate(model, test_loader, device, verbose=False):
    model.eval()
    all_sent_true, all_sent_pred   = [], []
    all_intent_true, all_intent_pred = [], []

    loss_sent_fn = nn.CrossEntropyLoss()
    loss_asp_fn  = nn.BCEWithLogitsLoss()
    loss_int_fn  = nn.CrossEntropyLoss()
    
    with torch.no_grad():
        for batch in test_loader:
            ids  = batch['input_ids'].to(device)
            mask = batch['attention_mask'].to(device)

            with torch.autocast(device_type=device.type, enabled=USE_AMP):
                s_logits, _, i_logits = model(ids, mask)

            all_sent_true.extend(batch['sentiment'].numpy())
            all_sent_pred.extend(s_logits.argmax(dim=1).cpu().numpy())
            all_intent_true.extend(batch['intent'].numpy())
            all_intent_pred.extend(i_logits.argmax(dim=1).cpu().numpy())

    if verbose:
        print("\n  Sentiment Report:")
        print(classification_report(all_sent_true, all_sent_pred, target_names=SENTIMENT_LABELS))
        print("\n  Intent Report:")
        print(classification_report(all_intent_true, all_intent_pred, target_names=INTENT_LABELS))

    return {
        'sentiment_f1':       f1_score(all_sent_true,   all_sent_pred,   average='macro'),
        'sentiment_accuracy': accuracy_score(all_sent_true,   all_sent_pred),
        'intent_f1':          f1_score(all_intent_true, all_intent_pred, average='macro'),
        'intent_accuracy':    accuracy_score(all_intent_true, all_intent_pred),
    }

def train_model(model, train_loader, val_loader, test_loader, train_eval_loader, device):
    print("\n[3/4] Training PhoBERT (Google Colab)...")
    print(f"  FP16 AMP         : {'ON (GPU)' if USE_AMP else 'OFF (CPU)'}")
    print(f"  Grad accumulation: {GRAD_ACCUM_STEPS} steps")
    print(f"  Early stopping   : patience={EARLY_STOP_PATIENCE} (tracks Val F1)")

    backbone_params = [p for p in model.backbone.parameters() if p.requires_grad]
    head_params = (
        list(model.sentiment_head.parameters())
        + list(model.aspect_head.parameters())
        + list(model.intent_head.parameters())
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
    amp_scaler = torch.cuda.amp.GradScaler(enabled=USE_AMP)

    # local MLflow logging
    mlflow.set_experiment("tourism_nlp_phobert_colab")

    with mlflow.start_run(run_name=f"phobert_colab_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        best_composite   = 0.0
        best_state       = None
        no_improve       = 0
        train_losses     = []
        val_f1_history   = []
        train_f1_history = []

        for epoch in range(EPOCHS):
            model.train()
            epoch_loss = 0.0
            optimizer.zero_grad()

            for step, batch in enumerate(train_loader):
                ids   = batch['input_ids'].to(device)
                mask  = batch['attention_mask'].to(device)
                s_lbl = batch['sentiment'].to(device)
                a_lbl = batch['aspects'].to(device)
                i_lbl = batch['intent'].to(device)
                u_sent = batch['use_sent'].to(device)
                u_int  = batch['use_int'].to(device)
                is_weak = batch['is_weak'].to(device)

                with torch.autocast(device_type=device.type, enabled=USE_AMP):
                    s_logits, a_logits, i_logits = model(ids, mask)
                    
                    # FIX ISSUE-04: Apply label smoothing for weak labels
                    loss_s_unreduced = nn.CrossEntropyLoss(reduction='none')(s_logits, s_lbl)
                    loss_s_smooth = nn.CrossEntropyLoss(reduction='none', label_smoothing=0.1)(s_logits, s_lbl)
                    loss_s = torch.where(is_weak, loss_s_smooth, loss_s_unreduced)
                    
                    loss_i = nn.CrossEntropyLoss(reduction='none')(i_logits, i_lbl)
                    
                    # FIX ISSUE-02: Mask loss per-task
                    loss_s = (loss_s * u_sent).sum() / max(u_sent.sum(), 1)
                    loss_i = (loss_i * u_int).sum() / max(u_int.sum(), 1)

                    # FIX ISSUE-05: Lower aspect loss weight to reduce noise
                    loss = (
                        loss_s * 0.5
                        + loss_asp_fn(a_logits, a_lbl) * 0.1
                        + loss_i * 0.4
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

            val_metrics = evaluate(model, val_loader, device)
            val_f1      = val_metrics['sentiment_f1']
            val_f1_history.append(val_f1)
            
            # FIX ISSUE-06: Use composite metric for early stopping
            val_composite = 0.6 * val_metrics['sentiment_f1'] + 0.4 * val_metrics['intent_f1']

            train_metrics = evaluate(model, train_eval_loader, device)
            train_f1      = train_metrics['sentiment_f1']
            train_f1_history.append(train_f1)
            overfit_gap   = train_f1 - val_f1

            gap_flag = " [⚠️ GAP]" if overfit_gap > 0.10 else ""
            print(f"  Epoch {epoch+1}/{EPOCHS} | Loss: {avg_loss:.4f} | "
                  f"Train-F1: {train_f1:.4f} | Val-F1: {val_f1:.4f} | "
                  f"Gap: {overfit_gap:+.4f}{gap_flag} | Intent-F1: {val_metrics['intent_f1']:.4f}")

            mlflow.log_metrics({
                "train_loss":          avg_loss,
                "train_sentiment_f1":  train_f1,
                "val_sentiment_f1":    val_f1,
                "val_intent_f1":       val_metrics['intent_f1'],
                "val_sent_accuracy":   val_metrics['sentiment_accuracy'],
                "val_composite_f1":    val_composite,
                "overfit_gap":         overfit_gap,
            }, step=epoch + 1)

            # FIX ISSUE-06: Check composite score for early stopping
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

        # Save best model weight locally
        model.load_state_dict(best_state)
        torch.save(model.state_dict(), "phobert_multi_task.pt")
        print("\n  Saved best model weights to: phobert_multi_task.pt")

        # Plot curves
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        ax1.plot(range(1, len(train_losses) + 1), train_losses, marker='o', color='steelblue')
        ax1.set(xlabel='Epoch', ylabel='Loss', title='Training Loss')

        epochs_x = range(1, len(val_f1_history) + 1)
        ax2.plot(epochs_x, train_f1_history, marker='o', color='steelblue', label='Train F1 (subset)')
        ax2.plot(epochs_x, val_f1_history,   marker='s', color='green',     label='Val F1')
        if len(val_f1_history) > 1:
            ax2.fill_between(epochs_x, val_f1_history, train_f1_history, alpha=0.20, color='orange', label='Overfit Gap')
        ax2.axhline(best_composite, color='red', linestyle='--', label=f'Best Val Composite F1={best_composite:.4f}')
        ax2.set(xlabel='Epoch', ylabel='Sentiment F1', title='Train vs Val F1')
        ax2.legend()
        plt.tight_layout()
        plt.savefig("training_curves.png")
        print("  Saved training curves to: training_curves.png")
        plt.close()

        print("\n  === Final Evaluation on TEST SET (unbiased) ===")
        evaluate(model, test_loader, device, verbose=True)

def main():
    csv_path = "nlp_weak_labeled_81k.csv"
    if not os.path.exists(csv_path):
        print(f"ERROR: {csv_path} not found! Please upload the CSV first.")
        return

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Running on device: {device}")

    pdf = load_and_prepare_data(csv_path)
    tokenizer = AutoTokenizer.from_pretrained(PHOBERT_MODEL_NAME)
    train_loader, val_loader, test_loader, train_eval_loader = create_dataloaders(pdf, tokenizer)

    model = PhoBERTMultiTask(
        model_name=PHOBERT_MODEL_NAME,
        num_sentiments=len(SENTIMENT_LABELS),
        num_aspects=len(ASPECT_LABELS),
        num_intents=len(INTENT_LABELS),
    ).to(device)

    model = apply_layer_freezing(model)
    train_model(model, train_loader, val_loader, test_loader, train_eval_loader, device)

if __name__ == "__main__":
    main()
