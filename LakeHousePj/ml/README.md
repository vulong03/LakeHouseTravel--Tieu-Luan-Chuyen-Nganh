# Province Recommendation System - ML Pipeline

## 📋 Overview

Hệ thống recommendation tỉnh/thành phố du lịch dựa trên phân tích **465,707 bình luận TikTok** với NLP và sentiment analysis.

## 🏗️ Architecture

```
Data Pipeline:
Bronze → Silver → Gold → ML Training → MLflow → Gradio App

Components:
- Spark: Data processing & feature engineering
- MLflow: Experiment tracking & model registry
- XGBoost: Recommendation model
- Gradio: Web interface
```

## 🚀 Quick Start

### 1. Build & Start Services

```bash
# Build MLflow and Gradio images
docker-compose build mlflow gradio

# Start services
docker-compose up -d mlflow gradio

# Check logs
docker logs lakehouse_mlflow
docker logs lakehouse_gradio
```

### 2. Train Model

```bash
# Run training script
cd LakeHousePj/scripts
./train-province-model.ps1
```

**Training Output:**
- Experiment tracked in MLflow
- Model registered as `province_recommendation_model`
- Feature importance plots
- Performance metrics (RMSE, MAE, R²)

### 3. Access Applications

| Service | URL | Description |
|---------|-----|-------------|
| MLflow UI | http://localhost:5001 | Experiment tracking & model registry |
| Gradio App | http://localhost:7860 | Recommendation web interface |

### 4. Register Model to Production

1. Open MLflow UI: http://localhost:5001
2. Navigate to **Models** → `province_recommendation_model`
3. Select latest version
4. Click **Stage** → **Transition to Production**

### 5. Use Gradio App

1. Open: http://localhost:7860
2. Select **month** (1-12)
3. Choose **region** (Bắc/Trung/Nam/Tất cả)
4. Set **number of recommendations** (3-10)
5. Click **🚀 Tìm Điểm Đến**

## 📊 Model Details

### Features (28 total)

**Temporal Features:**
- `month`, `month_sin`, `month_cos`
- `is_tet`, `is_summer`, `is_holiday`

**Comment Metrics:**
- `comment_volume`, `avg_sentiment`
- `positive_ratio`, `negative_ratio`
- NLP features: word count, unique words, emojis

**Lag Features:**
- Previous month metrics (lag1)
- 3-month rolling averages

**Province Context:**
- `region` (Bắc/Trung/Nam)
- Post engagement metrics

**Target Variable:**
```python
engagement_score = (
    volume_norm * 0.3 +
    positive_ratio * 0.4 +
    sentiment_norm * 0.2 +
    unique_word_ratio * 0.1
)
```

### Model Hyperparameters

```python
n_estimators=200
max_depth=6
learning_rate=0.1
subsample=0.8
colsample_bytree=0.8
```

## 📁 Project Structure

```
ml/
├── mlflow/
│   ├── Dockerfile              # MLflow container
│   └── data/                   # MLflow data
├── scripts/
│   └── train_province_recommendation.py  # Training script
└── models/                     # Saved models

gradio/
├── Dockerfile                  # Gradio container
├── requirements.txt            # Python dependencies
├── app.py                      # Web interface
└── static/                     # Static assets

scripts/
└── train-province-model.ps1    # Training runner
```

## 🔧 Troubleshooting

### MLflow Connection Error

```bash
# Check MLflow service
docker logs lakehouse_mlflow

# Restart if needed
docker-compose restart mlflow
```

### Gradio Can't Load Model

```bash
# Verify model registered in MLflow UI
# Check model stage is "Production"

# Restart Gradio
docker-compose restart gradio
```

### Training Memory Error

```bash
# Increase executor memory in train-province-model.ps1
--executor-memory 4g  # (default: 2g)
```

## 📈 Performance Metrics

**Expected Results:**
- Train R²: ~0.75-0.85
- Test R²: ~0.65-0.75
- RMSE: ~0.08-0.12
- MAE: ~0.06-0.10

**Training Time:**
- ~3-5 minutes on 2 executors
- Dataset: 1,893 rows × 28 features

## 🎯 Use Cases

1. **Personal Travel Planning**
   - Choose month → Get top recommendations

2. **Tourism Marketing**
   - Identify best seasons for each province

3. **Data-Driven Insights**
   - Understand sentiment trends
   - Seasonal tourism patterns

## 🔄 Retraining

```bash
# Update model with new data (monthly)
cd scripts
./run-province-month-features.ps1  # Update aggregation
./train-province-model.ps1         # Retrain model

# Check MLflow for new version
# Update Production stage
```

## 📝 API Reference

### Recommendation Function

```python
def recommend(month: int, region: str, top_n: int = 5):
    """
    Args:
        month: 1-12 (tháng trong năm)
        region: "Tất cả" | "Bắc" | "Trung" | "Nam"
        top_n: Number of recommendations (3-10)
    
    Returns:
        DataFrame with columns:
        - Tỉnh/Thành phố
        - Miền
        - Điểm số (0-1, higher is better)
    """
```

## 🎨 Gradio Interface

**Input:**
- Month selector: Dropdown with Vietnamese month names
- Region filter: Radio buttons (Bắc/Trung/Nam/Tất cả)
- Top N slider: 3-10 recommendations

**Output:**
- Ranked list with emoji indicators (🥇🥈🥉)
- Engagement scores (0-1 scale)
- Interactive table with sorting

## 🐳 Docker Configuration

### MLflow Service

```yaml
ports:
  - "5001:5000"  # MLflow UI
environment:
  MLFLOW_BACKEND_STORE_URI: postgresql://...
  MLFLOW_DEFAULT_ARTIFACT_ROOT: s3://gold/mlflow/
```

### Gradio Service

```yaml
ports:
  - "7860:7860"  # Gradio UI
environment:
  MLFLOW_TRACKING_URI: http://mlflow:5000
volumes:
  - ./gradio:/app  # Hot reload for development
```

## 📞 Support

- Check logs: `docker logs lakehouse_mlflow` / `docker logs lakehouse_gradio`
- MLflow docs: https://mlflow.org/docs/latest/
- Gradio docs: https://gradio.app/docs/
