# Hướng dẫn Huấn luyện PhoBERT trên Google Colab

Dưới đây là đoạn code bạn cần để chạy quá trình Fine-tune PhoBERT trên Google Colab bằng GPU T4. Bỏ qua các bước kết nối Spark và MLflow bị lỗi môi trường.

## Bước 1: Setup Môi trường trên Colab

Mở Colab, đảm bảo chọn **Runtime > Change runtime type > Hardware accelerator: T4 GPU**.

Chạy ô lệnh đầu tiên để cài đặt các thư viện cần thiết:

```python
!pip install transformers torch pandas scikit-learn underthesea
```

## Bước 2: Upload Dữ liệu
Tải 3 file parquet của bạn lên Colab (dùng thanh bên trái "Files" -> Upload). 
Và một thư mục chung ví dụ là `/content/data/`, đưa 3 file đó vào trong.

## Bước 3: Đoạn code chính

Tạo một Text Cell (ô mã) mới trong Colab, dán đoạn code sau và chạy. Đoạn code này được cải tiến để đọc một lúc cả 3 file data bằng pandas và chỉ dùng cấu hình nhẹ gọn, đầy đủ Multi-Task logic của bạn.

```python
import os
import glob
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.metrics import accuracy_score, f1_score, classification_report
from sklearn.model_selection import train_test_split

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

from transformers import AutoTokenizer, AutoModel, get_linear_schedule_with_warmup

import warnings
warnings.filterwarnings('ignore')

# Tùy chỉnh thông số để tiết kiệm RAM/GPU và rút ngắn thời gian.
# - Rút số mẫu (MAX_PER_CLASS) để train nhanh hơn, nâng lên nếu dư dả thời gian (Colab T4 tối đa được 12h/phiên).
MAX_PER_CLASS = 15000 
BATCH_SIZE = 32
MAX_SEQ_LENGTH = 128
EPOCHS = 4
LEARNING_RATE = 2e-5
WARMUP_RATIO = 0.1
PHOBERT_MODEL_NAME = "vinai/phobert-base-v2"
TRAIN_TEST_SPLIT = 0.85

# Các cấu hình của bạn
SENTIMENT_LABELS = ["negative", "neutral", "positive"]
SENTIMENT_TO_ID = {l: i for i, l in enumerate(SENTIMENT_LABELS)}
ASPECT_LABELS = ["scenery", "food", "price", "service", "transport", "accommodation"]
INTENT_LABELS = ["recommend", "complain", "question", "share"]
INTENT_TO_ID = {l: i for i, l in enumerate(INTENT_LABELS)}


# ============================================================
# 1. Multi-Task PhoBERT Model
# ============================================================
class PhoBERTMultiTask(nn.Module):
    def __init__(self, model_name, num_sentiments, num_aspects, num_intents, dropout=0.3):
        super().__init__()
        self.backbone = AutoModel.from_pretrained(model_name)
        hidden = self.backbone.config.hidden_size

        self.dropout = nn.Dropout(dropout)
        self.sentiment_head = nn.Sequential(nn.Linear(hidden, 128), nn.ReLU(), nn.Dropout(dropout), nn.Linear(128, num_sentiments))
        self.aspect_head = nn.Sequential(nn.Linear(hidden, 128), nn.ReLU(), nn.Dropout(dropout), nn.Linear(128, num_aspects))
        self.intent_head = nn.Sequential(nn.Linear(hidden, 64), nn.ReLU(), nn.Dropout(dropout), nn.Linear(64, num_intents))

    def forward(self, input_ids, attention_mask):
        outputs = self.backbone(input_ids=input_ids, attention_mask=attention_mask)
        cls_output = self.dropout(outputs.last_hidden_state[:, 0, :])
        return self.sentiment_head(cls_output), self.aspect_head(cls_output), self.intent_head(cls_output)


# ============================================================
# 2. Dataset Definition
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
            str(self.texts[idx]), max_length=self.max_len, padding='max_length',
            truncation=True, return_tensors='pt'
        )
        return {
            'input_ids': encoding['input_ids'].squeeze(),
            'attention_mask': encoding['attention_mask'].squeeze(),
            'sentiment': torch.tensor(self.sentiment_ids[idx], dtype=torch.long),
            'aspects': torch.tensor(self.aspect_vectors[idx], dtype=torch.float),
            'intent': torch.tensor(self.intent_ids[idx], dtype=torch.long),
        }


# ============================================================
# 3. Data Preparation
# ============================================================
def load_and_prepare_data(folder_path):
    print("\n[1/4] Loading weak-labeled data from parquet files...")
    # Đọc tất cả các file parquet trong thư mục
    files = glob.glob(os.path.join(folder_path, '*.parquet'))
    dfs = [pd.read_parquet(f) for f in files]
    pdf = pd.concat(dfs, ignore_index=True)
    
    print(f"  Loaded {len(pdf):,} samples from {len(files)} files")

    # Encode labels
    pdf['sentiment_id'] = pdf['sentiment_label'].map(SENTIMENT_TO_ID).fillna(1).astype(int)
    
    def aspects_to_vector(aspects_str):
        vec = [0] * len(ASPECT_LABELS)
        if aspects_str:
            for a in aspects_str.split(','):
                a = a.strip()
                if a in ASPECT_LABELS: vec[ASPECT_LABELS.index(a)] = 1
        return vec
    pdf['aspect_vector'] = pdf['aspects'].apply(aspects_to_vector)
    pdf['intent_id'] = pdf['intent_label'].map(INTENT_TO_ID).fillna(3).astype(int)

    # Balance data
    min_class_size = pdf['sentiment_label'].value_counts().min()
    max_per_class = max(min_class_size, MAX_PER_CLASS)

    balanced_dfs = []
    for label in SENTIMENT_LABELS:
        subset = pdf[pdf['sentiment_label'] == label]
        if len(subset) > max_per_class:
            subset = subset.sample(n=max_per_class, random_state=42)
        balanced_dfs.append(subset)

    pdf_balanced = pd.concat(balanced_dfs, ignore_index=True).sample(frac=1, random_state=42)
    print(f"\n  Balanced to {len(pdf_balanced):,} samples")
    return pdf_balanced

def create_dataloaders(pdf, tokenizer):
    print("\n[2/4] Creating DataLoaders...")
    texts = pdf['comment_text'].tolist()
    sentiment_ids = pdf['sentiment_id'].tolist()
    aspect_vectors = pdf['aspect_vector'].tolist()
    intent_ids = pdf['intent_id'].tolist()

    idx_train, idx_test = train_test_split(
        range(len(texts)), test_size=1 - TRAIN_TEST_SPLIT, random_state=42, stratify=sentiment_ids
    )

    train_ds = CommentDataset([texts[i] for i in idx_train], [sentiment_ids[i] for i in idx_train], [aspect_vectors[i] for i in idx_train], [intent_ids[i] for i in idx_train], tokenizer, MAX_SEQ_LENGTH)
    test_ds = CommentDataset([texts[i] for i in idx_test], [sentiment_ids[i] for i in idx_test], [aspect_vectors[i] for i in idx_test], [intent_ids[i] for i in idx_test], tokenizer, MAX_SEQ_LENGTH)

    train_loader = DataLoader(train_ds, batch_size=BATCH_SIZE, shuffle=True)
    test_loader = DataLoader(test_ds, batch_size=BATCH_SIZE, shuffle=False)

    print(f"  Train: {len(train_ds):,} samples, Test: {len(test_ds):,} samples")
    return train_loader, test_loader


# ============================================================
# 4. Training (Without MLflow)
# ============================================================
def train_model(model, train_loader, test_loader, device):
    print("\n[3/4] Training PhoBERT multi-task model...")

    optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE, weight_decay=0.01)
    total_steps = len(train_loader) * EPOCHS
    scheduler = get_linear_schedule_with_warmup(optimizer, num_warmup_steps=int(total_steps * WARMUP_RATIO), num_training_steps=total_steps)

    sentiment_loss_fn = nn.CrossEntropyLoss()
    aspect_loss_fn = nn.BCEWithLogitsLoss()
    intent_loss_fn = nn.CrossEntropyLoss()

    best_f1 = 0
    best_state = None

    for epoch in range(EPOCHS):
        model.train()
        epoch_loss = 0
        
        # Simple loading bar
        steps = len(train_loader)
        for i, batch in enumerate(train_loader):
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
            
            if i % 100 == 0:
                print(f"Epoch {epoch+1}/{EPOCHS} | Step {i}/{steps} | Loss: {loss.item():.4f}")

        # Evaluation (Test step)
        model.eval()
        all_sent_preds, all_sent_labels = [], []
        
        with torch.no_grad():
            for batch in test_loader:
                input_ids = batch['input_ids'].to(device)
                attention_mask = batch['attention_mask'].to(device)
                sent_labels = batch['sentiment'].to(device)
                
                sent_logits, _, _ = model(input_ids, attention_mask)
                _, sent_preds = torch.max(sent_logits, dim=1)
                
                all_sent_preds.extend(sent_preds.cpu().numpy())
                all_sent_labels.extend(sent_labels.cpu().numpy())

        sent_f1 = f1_score(all_sent_labels, all_sent_preds, average='macro')
        print(f"\n=> Epoch {epoch+1} Completed | Train Loss: {epoch_loss/steps:.4f} | Validation Sentiment F1 Macro: {sent_f1:.4f}\n")

        if sent_f1 > best_f1:
            best_f1 = sent_f1
            best_state = {k: v.cpu() for k, v in model.state_dict().items()}

    print(f"  Best Validation Sentiment F1: {best_f1:.4f}")
    if best_state is not None:
        model.load_state_dict(best_state)
    return model

# ============================================================
# Main Execution
# ============================================================
# TẠO THƯ MỤC LƯU MODEL
MODEL_SAVE_PATH = '/content/phobert_tourism_model'
os.makedirs(MODEL_SAVE_PATH, exist_ok=True)

# KIỂM TRA THIẾT BỊ
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Training using device: {device}")

# THỰC CÔNG QUY TRÌNH
pdf_balanced = load_and_prepare_data("/content/data") # <-- Chỉ đường dẫn data 
tokenizer = AutoTokenizer.from_pretrained(PHOBERT_MODEL_NAME)
train_loader, test_loader = create_dataloaders(pdf_balanced, tokenizer)

model = PhoBERTMultiTask(PHOBERT_MODEL_NAME, len(SENTIMENT_LABELS), len(ASPECT_LABELS), len(INTENT_LABELS))
model = model.to(device)

model = train_model(model, train_loader, test_loader, device)

# LƯU MODEL LẠI VÀO COLAB FILE SYSTEM
print(f"\n[4/4] Saving model to {MODEL_SAVE_PATH}...")
torch.save(model.state_dict(), os.path.join(MODEL_SAVE_PATH, "pytorch_model.bin"))
tokenizer.save_pretrained(MODEL_SAVE_PATH)
print("  Save complete! Download this folder's contents to your computer.")
```

## Bước 4: Tải Model về
Khi chạy xong ô trên, ở mục "Files" bên trái Colab, bạn sẽ thấy thư mục `phobert_tourism_model` vừa được tạo ra. Bên trong nó chứa file trọng số `pytorch_model.bin` cùng với các file config/vocab của tokenizer.

Bạn click chuột phải vào các file đó và bấm **Download** về máy của mình. Lưu vào chung một thư mục.
