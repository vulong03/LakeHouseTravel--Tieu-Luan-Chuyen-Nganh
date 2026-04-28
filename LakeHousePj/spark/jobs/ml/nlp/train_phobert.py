"""
NLP Pipeline Step 2: Fine-tune PhoBERT for Tourism Comments
============================================================
Multi-task model:
  Head 1: Sentiment classification (negative/neutral/positive)
  Head 2: Aspect detection (multi-label: scenery, food, price, service, transport, accommodation)
  Head 3: Intent classification (recommend/complain/question/share)

Input:  Weak-labeled Parquet from Step 1
Output: Fine-tuned PhoBERT model in MLflow registry
"""

import sys
import os
sys.path.append('/opt/spark/jobs')

import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.metrics import (
    accuracy_score, f1_score, classification_report
)
from sklearn.model_selection import train_test_split

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

from transformers import (
    AutoTokenizer, AutoModel,
    get_linear_schedule_with_warmup,
)

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
    MAX_SEQ_LENGTH, TRAIN_TEST_SPLIT, BATCH_SIZE,
    LEARNING_RATE, EPOCHS, WARMUP_RATIO,
    MLFLOW_TRACKING_URI, MLFLOW_EXPERIMENT,
    SENTIMENT_LABELS, SENTIMENT_TO_ID,
    ASPECT_LABELS,
    INTENT_LABELS, INTENT_TO_ID,
)


# ============================================================
# Multi-Task PhoBERT Model
# ============================================================

class PhoBERTMultiTask(nn.Module):
    """
    PhoBERT backbone with 3 classification heads:
    - Sentiment (3-class softmax)
    - Aspect (6-class sigmoid, multi-label)
    - Intent (4-class softmax)
    """
    def __init__(self, model_name, num_sentiments, num_aspects, num_intents, dropout=0.3):
        super().__init__()
        self.backbone = AutoModel.from_pretrained(model_name)
        hidden = self.backbone.config.hidden_size

        self.dropout = nn.Dropout(dropout)

        self.sentiment_head = nn.Sequential(
            nn.Linear(hidden, 128),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(128, num_sentiments),
        )

        self.aspect_head = nn.Sequential(
            nn.Linear(hidden, 128),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(128, num_aspects),
        )

        self.intent_head = nn.Sequential(
            nn.Linear(hidden, 64),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(64, num_intents),
        )

    def forward(self, input_ids, attention_mask):
        outputs = self.backbone(input_ids=input_ids, attention_mask=attention_mask)
        cls_output = self.dropout(outputs.last_hidden_state[:, 0, :])

        sentiment_logits = self.sentiment_head(cls_output)
        aspect_logits = self.aspect_head(cls_output)
        intent_logits = self.intent_head(cls_output)

        return sentiment_logits, aspect_logits, intent_logits


# ============================================================
# Dataset
# ============================================================

class CommentDataset(Dataset):
    def __init__(self, texts, sentiment_ids, aspect_vectors, intent_ids, tokenizer, max_len):
        self.texts = texts
        self.sentiment_ids = sentiment_ids
        self.aspect_vectors = aspect_vectors
        self.intent_ids = intent_ids
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        encoding = self.tokenizer(
            str(self.texts[idx]),
            max_length=self.max_len,
            padding='max_length',
            truncation=True,
            return_tensors='pt',
        )
        return {
            'input_ids': encoding['input_ids'].squeeze(),
            'attention_mask': encoding['attention_mask'].squeeze(),
            'sentiment': torch.tensor(self.sentiment_ids[idx], dtype=torch.long),
            'aspects': torch.tensor(self.aspect_vectors[idx], dtype=torch.float),
            'intent': torch.tensor(self.intent_ids[idx], dtype=torch.long),
        }


# ============================================================
# Data Preparation
# ============================================================

def load_and_prepare_data():
    """Load weak-labeled Parquet and prepare for training."""
    print("\n[1/4] Loading weak-labeled data...")

    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("NLP_Load_Data") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    df = spark.read.parquet(LABELED_PARQUET_PATH)
    pdf = df.toPandas()
    spark.stop()

    print(f"  Loaded {len(pdf):,} samples")

    # Encode sentiment
    pdf['sentiment_id'] = pdf['sentiment_label'].map(SENTIMENT_TO_ID).fillna(1).astype(int)

    # Encode aspects (multi-label binary vector)
    def aspects_to_vector(aspects_str):
        vec = [0] * len(ASPECT_LABELS)
        if aspects_str:
            for a in aspects_str.split(','):
                a = a.strip()
                if a in ASPECT_LABELS:
                    vec[ASPECT_LABELS.index(a)] = 1
        return vec

    pdf['aspect_vector'] = pdf['aspects'].apply(aspects_to_vector)

    # Encode intent
    pdf['intent_id'] = pdf['intent_label'].map(INTENT_TO_ID).fillna(3).astype(int)

    # Balance dataset: undersample majority class for sentiment
    print("\n  Class distribution (sentiment):")
    print(pdf['sentiment_label'].value_counts())

    min_class_size = pdf['sentiment_label'].value_counts().min()
    max_per_class = max(min_class_size, 5000)

    balanced_dfs = []
    for label in SENTIMENT_LABELS:
        subset = pdf[pdf['sentiment_label'] == label]
        if len(subset) > max_per_class:
            subset = subset.sample(n=max_per_class, random_state=42)
        balanced_dfs.append(subset)

    pdf_balanced = pd.concat(balanced_dfs, ignore_index=True).sample(frac=1, random_state=42)
    print(f"\n  Balanced to {len(pdf_balanced):,} samples")
    print(pdf_balanced['sentiment_label'].value_counts())

    return pdf_balanced


def create_dataloaders(pdf, tokenizer):
    """Split data and create DataLoaders."""
    print("\n[2/4] Creating DataLoaders...")

    texts = pdf['comment_text'].tolist()
    sentiment_ids = pdf['sentiment_id'].tolist()
    aspect_vectors = pdf['aspect_vector'].tolist()
    intent_ids = pdf['intent_id'].tolist()

    idx_train, idx_test = train_test_split(
        range(len(texts)), test_size=1 - TRAIN_TEST_SPLIT, random_state=42, stratify=sentiment_ids
    )

    train_ds = CommentDataset(
        [texts[i] for i in idx_train],
        [sentiment_ids[i] for i in idx_train],
        [aspect_vectors[i] for i in idx_train],
        [intent_ids[i] for i in idx_train],
        tokenizer, MAX_SEQ_LENGTH,
    )
    test_ds = CommentDataset(
        [texts[i] for i in idx_test],
        [sentiment_ids[i] for i in idx_test],
        [aspect_vectors[i] for i in idx_test],
        [intent_ids[i] for i in idx_test],
        tokenizer, MAX_SEQ_LENGTH,
    )

    train_loader = DataLoader(train_ds, batch_size=BATCH_SIZE, shuffle=True)
    test_loader = DataLoader(test_ds, batch_size=BATCH_SIZE, shuffle=False)

    print(f"  Train: {len(train_ds):,} samples, Test: {len(test_ds):,} samples")
    return train_loader, test_loader


# ============================================================
# Training
# ============================================================

def train_model(model, train_loader, test_loader, device):
    """Train multi-task PhoBERT with MLflow tracking."""
    print("\n[3/4] Training PhoBERT multi-task model...")

    optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE, weight_decay=0.01)
    total_steps = len(train_loader) * EPOCHS
    scheduler = get_linear_schedule_with_warmup(
        optimizer, num_warmup_steps=int(total_steps * WARMUP_RATIO),
        num_training_steps=total_steps,
    )

    sentiment_loss_fn = nn.CrossEntropyLoss()
    aspect_loss_fn = nn.BCEWithLogitsLoss()
    intent_loss_fn = nn.CrossEntropyLoss()

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name=f"phobert_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_params({
            "model": PHOBERT_MODEL_NAME,
            "max_seq_length": MAX_SEQ_LENGTH,
            "batch_size": BATCH_SIZE,
            "learning_rate": LEARNING_RATE,
            "epochs": EPOCHS,
            "warmup_ratio": WARMUP_RATIO,
            "tasks": "sentiment+aspect+intent",
            "sentiment_classes": len(SENTIMENT_LABELS),
            "aspect_classes": len(ASPECT_LABELS),
            "intent_classes": len(INTENT_LABELS),
            "train_size": len(train_loader.dataset),
            "test_size": len(test_loader.dataset),
        })

        best_f1 = 0
        best_state = None
        train_losses = []

        for epoch in range(EPOCHS):
            model.train()
            epoch_loss = 0

            for batch in train_loader:
                input_ids = batch['input_ids'].to(device)
                attention_mask = batch['attention_mask'].to(device)
                sent_labels = batch['sentiment'].to(device)
                aspect_labels = batch['aspects'].to(device)
                intent_labels = batch['intent'].to(device)

                optimizer.zero_grad()

                sent_logits, aspect_logits, intent_logits = model(input_ids, attention_mask)

                loss = (
                    sentiment_loss_fn(sent_logits, sent_labels) * 0.4
                    + aspect_loss_fn(aspect_logits, aspect_labels) * 0.3
                    + intent_loss_fn(intent_logits, intent_labels) * 0.3
                )

                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
                optimizer.step()
                scheduler.step()

                epoch_loss += loss.item()

            avg_loss = epoch_loss / len(train_loader)
            train_losses.append(avg_loss)

            # Evaluate
            metrics = evaluate(model, test_loader, device)

            print(f"  Epoch {epoch+1}/{EPOCHS} | Loss: {avg_loss:.4f} | "
                  f"Sent-F1: {metrics['sentiment_f1']:.4f} | "
                  f"Intent-F1: {metrics['intent_f1']:.4f}")

            mlflow.log_metrics({
                f"epoch_{epoch+1}_loss": avg_loss,
                f"epoch_{epoch+1}_sentiment_f1": metrics['sentiment_f1'],
                f"epoch_{epoch+1}_intent_f1": metrics['intent_f1'],
            })

            if metrics['sentiment_f1'] > best_f1:
                best_f1 = metrics['sentiment_f1']
                best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}

        model.load_state_dict(best_state)
        model.to(device)

        # Final evaluation
        final_metrics = evaluate(model, test_loader, device, verbose=True)

        mlflow.log_metrics({
            "best_sentiment_f1": final_metrics['sentiment_f1'],
            "sentiment_accuracy": final_metrics['sentiment_accuracy'],
            "intent_f1": final_metrics['intent_f1'],
            "intent_accuracy": final_metrics['intent_accuracy'],
        })

        # Loss plot
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(range(1, len(train_losses) + 1), train_losses, marker='o')
        ax.set_xlabel('Epoch'); ax.set_ylabel('Loss')
        ax.set_title('PhoBERT Multi-Task Training Loss')
        mlflow.log_figure(fig, "training_loss.png")
        plt.close()

        # Save model
        mlflow.pytorch.log_model(model, "model")
        model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
        mlflow.register_model(model_uri, FINE_TUNED_MODEL_NAME)

        print(f"\n  Model registered: {FINE_TUNED_MODEL_NAME}")
        print(f"  Best Sentiment F1: {best_f1:.4f}")

        return model


def evaluate(model, test_loader, device, verbose=False):
    """Evaluate all 3 tasks."""
    model.eval()

    all_sent_true, all_sent_pred = [], []
    all_intent_true, all_intent_pred = [], []

    with torch.no_grad():
        for batch in test_loader:
            input_ids = batch['input_ids'].to(device)
            attention_mask = batch['attention_mask'].to(device)

            sent_logits, aspect_logits, intent_logits = model(input_ids, attention_mask)

            all_sent_true.extend(batch['sentiment'].numpy())
            all_sent_pred.extend(sent_logits.argmax(dim=1).cpu().numpy())

            all_intent_true.extend(batch['intent'].numpy())
            all_intent_pred.extend(intent_logits.argmax(dim=1).cpu().numpy())

    sent_f1 = f1_score(all_sent_true, all_sent_pred, average='macro')
    sent_acc = accuracy_score(all_sent_true, all_sent_pred)
    intent_f1 = f1_score(all_intent_true, all_intent_pred, average='macro')
    intent_acc = accuracy_score(all_intent_true, all_intent_pred)

    if verbose:
        print("\n  Sentiment Classification Report:")
        print(classification_report(
            all_sent_true, all_sent_pred, target_names=SENTIMENT_LABELS
        ))
        print("\n  Intent Classification Report:")
        print(classification_report(
            all_intent_true, all_intent_pred, target_names=INTENT_LABELS
        ))

    return {
        'sentiment_f1': sent_f1,
        'sentiment_accuracy': sent_acc,
        'intent_f1': intent_f1,
        'intent_accuracy': intent_acc,
    }


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 70)
    print("NLP Pipeline Step 2: Fine-tune PhoBERT")
    print("=" * 70)
    print(f"Model: {PHOBERT_MODEL_NAME}")
    print(f"Tasks: Sentiment (3) + Aspect (6) + Intent (4)")
    print(f"Start: {datetime.now()}")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    pdf = load_and_prepare_data()

    print(f"\n  Loading tokenizer: {PHOBERT_MODEL_NAME}")
    tokenizer = AutoTokenizer.from_pretrained(PHOBERT_MODEL_NAME)

    train_loader, test_loader = create_dataloaders(pdf, tokenizer)

    model = PhoBERTMultiTask(
        model_name=PHOBERT_MODEL_NAME,
        num_sentiments=len(SENTIMENT_LABELS),
        num_aspects=len(ASPECT_LABELS),
        num_intents=len(INTENT_LABELS),
    ).to(device)

    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f"  Total params: {total_params:,}, Trainable: {trainable_params:,}")

    train_model(model, train_loader, test_loader, device)

    print(f"\nCompleted: {datetime.now()}")
    print(f"  Next: Run inference_phobert.py to re-score all 465K comments")


if __name__ == "__main__":
    main()
