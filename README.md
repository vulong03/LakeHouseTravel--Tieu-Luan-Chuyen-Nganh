# Tourism Data Analytics Platform
## Overview
Tourism Data Analytics Platform là một nền tảng phân tích dữ liệu du lịch toàn diện, được xây dựng theo kiến trúc Lakehouse hiện đại. Dự án tập trung vào việc thu thập, xử lý và phân tích dữ liệu từ nhiều nguồn (Booking.com, TikTok) để cung cấp insights về xu hướng du lịch và dự báo mức độ "hot" của các tỉnh thành Việt Nam.
## Architecture


<img width="1316" height="604" alt="image" src="https://github.com/user-attachments/assets/b710f44d-ccd0-4044-b2b7-7ee4a9d49e62" />




### Medallion Architecture

Dự án tuân theo kiến trúc Medallion (Bronze → Silver → Gold):

- **Bronze Layer**: Dữ liệu thô (raw) từ các nguồn, lưu dưới dạng CSV gốc trên MinIO
- **Silver Layer**: Dữ liệu đã được làm sạch, chuẩn hóa và validate (Iceberg tables)
- **Gold Layer**: Dữ liệu đã được aggregate theo Star Schema, có sẵn dimensions và fact tables cho analytics

## Technology Stack
### Core Infrastructure

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Container Platform** | Docker & Docker Compose | - | Container orchestration |
| **Workflow Orchestration** | Apache Airflow | 2.10.2 | ETL pipeline scheduling |
| **Distributed Processing** | Apache Spark | 3.5.0 | Big data processing |
| **Table Format** | Apache Iceberg | 1.4.3 | ACID transactions, time travel |
| **Object Storage** | MinIO | Latest | S3-compatible storage |
| **Metadata Store** | Hive Metastore | 3.1.3 | Table metadata management |
| **Database** | PostgreSQL | 15 | Metadata & MLflow backend |

### Analytics & ML

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Query Engine** | Dremio | Latest | Interactive SQL analytics |
| **Visualization** | Apache Superset + Power BI | Latest | BI dashboards & data visualization |
| **DL Model** | PyTorch (LSTM + Temporal Attention) | - | Hotel volume forecasting |
| **ML Model** | XGBoost | - | Hotness forecasting (baseline) |
| **NLP Model** | PhoBERT (vinai/phobert-base-v2) | - | Vietnamese sentiment & aspect analysis |
| **NLP Library** | Underthesea | - | Vietnamese text preprocessing |
| **ML Ops** | MLflow | - | Experiment tracking, model registry |
| **Web Framework** | Gradio | - | Interactive forecasting dashboard |

### Programming Languages

- **Python 3.11** - Primary language cho Spark jobs, Airflow DAGs, ML
- **SQL** - Data queries và transformations
- **Bash** - Deployment scripts

## Data Pipeline

### Bronze Layer (Raw Ingestion)

**DAG**: `bronze_raw_ingestion`  
**Schedule**: Manual trigger  
**Purpose**: Thu thập dữ liệu thô từ CSV files và lưu vào MinIO

**Datasets**:
- `vietnam_hotels_list.csv` - Danh sách khách sạn 
- `vietnam_hotels_detail.csv` - Chi tiết khách sạn
- `vietnam_hotels_reviews.csv` - Reviews của khách hàng
- `tiktok_videos.csv` - TikTok videos về du lịch
- `tiktok_comments_*.csv` - Comments từ TikTok 

**Key Features**:
- File checksum tracking (tránh duplicate)
- UTF-8 encoding preservation
- Parallel ingestion cho datasets độc lập
- Logging vào PostgreSQL `file_ingestion_log` table

### Silver Layer (Data Transformation)

**DAG**: `silver_transformation`  
**Schedule**: Triggered sau Bronze layer  
**Purpose**: Làm sạch, validate và standardize data

**Transformations**:

1. **Hotels Detail**
   - Clean province names (loại bỏ ký tự đặc biệt cho S3 partition)
   - Extract latitude/longitude từ text
   - Parse JSON arrays (facilities, room_types)
   - Validate NOT NULL constraints

2. **Hotels List**
   - Standardize coordinates
   - Clean address fields
   - Map regions (Northeast, Southeast, v.v.)

3. **Hotels Reviews**
   - Parse review dates
   - Extract traveler types (Solo, Couple, Family, Business)
   - Clean review text
   - Calculate sentiment scores

4. **TikTok Videos**
   - Parse timestamps
   - Extract author info
   - Clean video descriptions
   - Map to provinces via destination matching

5. **TikTok Comments**
   - **NLP v1** với Underthesea: sentiment analysis, word count, unique word ratio, emoji detection
   - **NLP v2** với PhoBERT (`vinai/phobert-base-v2`): multi-task fine-tuning cho sentiment (3-class), 6-aspect ABSA và intent classification
   - Parent-child comment hierarchy (`level_comment`)
   - Deduplication theo composite key `(post_url, stt)`

**Resource Management**:
- **Phase 1** (Parallel): Light jobs (hotels_detail, hotels_list, tiktok_videos)
- **Phase 2** (Sequential): Heavy jobs (hotels_reviews → tiktok_comments)
- Executor memory: 2GB (light) → 4GB (heavy)

### Gold Layer (Analytics Ready)

**DAG**: `gold_aggregation`  
**Schedule**: Triggered sau Silver layer  
**Purpose**: Tạo star schema với dimensions và fact tables

**Dimension Tables**:
- `dim_date` - Date dimension (2015-2025)
- `dim_province` - 63 tỉnh/thành Việt Nam với region mapping
- `dim_destination` - Điểm đến du lịch
- `dim_author` - TikTok authors
- `dim_post` - TikTok videos/posts
- `dim_comment` - TikTok comments với NLP metrics
- `dim_hotel` - Hotels với location info
- `dim_room_type` - Room types (Suite, Deluxe, v.v.)
- `dim_travel_type` - Travel types (Solo, Couple, v.v.)
- `dim_country` - Countries (reviewer origins)

**Fact Tables**:
- `fact_province_content_engagement` - TikTok engagement metrics by province
- `fact_hotel_review_daily` - Hotel review analytics (grain: 1 review)
- `fact_comment_nlp_engagement` - NLP v1 analytics cho comments (Underthesea)
- `fact_comment_nlp_v2` - NLP v2 analytics cho comments (PhoBERT) — chạy riêng ngoài DAG chính
- `fact_province_month_dl_features` - ML feature table (grain: province × month, ~39 features)

**Key Features**:
- Surrogate keys (SK) cho tất cả dimensions
- Foreign key relationships
- Partition by province_sk cho performance
- ACID transactions với Iceberg MERGE operations

### ML Layer (Forecasting)

#### Model 1: LSTM v5 (Primary)

**Job**: `spark/jobs/dl/train_province_lstm_v5.py`  
**Purpose**: Train LSTM model để dự báo lượng đặt phòng khách sạn (`hotel_review_volume`) theo tỉnh

**Architecture**:
- 2-layer LSTM + LayerNorm + Temporal Attention
- FC Head: `Linear(hidden) → GELU → Dropout → Linear(16) → ReLU → Linear(1)`
- Loss: HybridLoss (70% Huber + 30% SMAPE)
- Scheduler: CosineAnnealingWarmRestarts
- Scaler: RobustScaler (fit trên train set only — no data leakage)

**Features**: 39 total (sequence length = 3 tháng)
- Temporal (2): `month_sin`, `month_cos`
- Hotel lags (3): `hotel_vol_lag_12`, `hotel_vol_rolling_3m`, `hotel_vol_momentum`
- Hotness lags (3): `hotness_lag_12`, `hotness_rolling_3m`, `hotness_momentum`
- TikTok Volume (3): `total_posts`, `total_comments`, `comments_per_post`
- Engagement (4): `avg_likes_per_post`, `avg_saves_per_post`, `viral_post_ratio`, `engagement_score`
- NLP/Sentiment (6): `avg_sentiment`, `sentiment_std`, `positive_ratio`, `negative_ratio`, `emoji_sentiment_ratio`, `reply_ratio`
- Aspect Scores (6): `avg_aspect_scenery/food/price/service/transport/accommodation`
- Hotel Quality (9): `avg_hotel_score`, `hotel_score_std`, `high_score_ratio`, `domestic_review_ratio`, `couple_ratio`, `family_ratio`, `business_ratio`, `solo_ratio`, `hotel_vol_growth`
- Custom (3): `social_to_booking_ratio`, `sentiment_polarity_change`, `hotel_vol_std_rolling_3m`

**Training Config**:
- Train/Val/Test Split: 70% / 15% / 15% (time-based)
- Epochs: 200, Early Stopping patience: 20
- Optuna hyperparameter tuning

**MLflow Tracking**:
- Experiment: `province_hotel_volume_forecasting_lstm_v5`
- Metrics: RMSE, MAE, R², MAPE
- Model registry: `province_hotel_volume_forecaster_lstm_v5`

**Forecasting**:
- Recursive autoregressive forecasting (12 tháng)
- Output table: `gold.gold.province_month_forecast_lstm_next12`
- Output path: `s3a://gold/dl_forecast/province_hotel_volume_forecast_lstm_v5/`

---

#### Model 2: XGBoost (Baseline)

**Job**: `spark/jobs/ml/train_and_forecast.py`  
**Purpose**: Train XGBoost model để dự báo `hotness_score` (composite index) theo tỉnh

**Features**: Temporal + lag features từ `fact_province_month_dl_features`
- Lag features: `hotness_lag_1/2/3/12`, `hotness_rolling_3m`, `hotness_momentum`
- Temporal: `month_sin`, `month_cos`

**Training Config**:
- Algorithm: XGBoost Regressor
- Train/Test Split: 70/30
- Hyperparameters: n_estimators=200, max_depth=4, learning_rate=0.01

**MLflow Tracking**:
- Experiment: `province_hotness_forecasting`
- Model registry: `province_hotness_forecaster`
- Output table: `gold.gold.province_month_forecast_next12`

## Prerequisites

### System Requirements

- **OS**: Windows 10/11, macOS, hoặc Linux
- **RAM**: Tối thiểu 16GB (khuyến nghị 32GB)
- **CPU**: 4+ cores (khuyến nghị 8+ cores)
- **Disk**: Tối thiểu 50GB free space
- **Docker**: Desktop 4.x hoặc cao hơn
- **Docker Compose**: v2.x

### Software Dependencies

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Git](https://git-scm.com/)
- PowerShell (Windows) hoặc Bash (Linux/macOS)

---

## Installation & Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd LakeHousePj
```

### 2. Configure Environment

Copy file `.env` và điều chỉnh nếu cần:

```bash
# File .env đã có sẵn với cấu hình mặc định
# Kiểm tra và thay đổi passwords nếu cần thiết
```

**Default Credentials**:
- **PostgreSQL**: `lakehouse_user` / `lakehouse_pass`
- **MinIO**: `minioadmin` / `minioadmin123`
- **Airflow**: `admin` / `admin`
- **pgAdmin**: `admin@admin.com` / `admin123`

### 3. Prepare Data Files

Đặt các file CSV vào thư mục `data/raw/`:

```
data/raw/
├── booking/
│   ├── vietnam_hotels_list.csv
│   ├── vietnam_hotels_detail.csv
│   └── vietnam_hotels_reviews.csv
└── tiktok/
    ├── links/
    │   └── *.csv (TikTok videos)
    └── comments/
        └── *.csv (TikTok comments)
```

### 4. Start Infrastructure

```bash
# Start all services
docker-compose up -d

# Check logs
docker-compose logs -f

# Verify services are healthy
docker-compose ps
```

**Startup Time**: Khoảng 2-3 phút cho tất cả services

### 5. Verify Services

Sau khi khởi động, truy cập các web interfaces:

| Service | URL | Purpose |
|---------|-----|---------|
| **Airflow** | http://localhost:8082 | Workflow orchestration |
| **Spark Master** | http://localhost:8080 | Spark cluster monitoring |
| **MinIO Console** | http://localhost:9001 | S3 storage management |
| **Dremio** | http://localhost:9047 | Query engine |
| **MLflow** | http://localhost:5001 | ML experiment tracking |
| **Gradio** | http://localhost:7860 | Forecasting web app |
| **pgAdmin** | http://localhost:5050 | Database management |

### 6. Initialize Dremio (First Time Only)

Truy cập http://localhost:9047 và setup:

1. Tạo admin account
2. Add source:
   - **Type**: Hive
   - **Name**: `hive_metastore`
   - **Host**: `hive-metastore`
   - **Port**: `9083`

Xem chi tiết: `dremio/SETUP.md`

---

## Usage

### Running ETL Pipeline

#### Option 1: Full Pipeline (Recommended)

1. Truy cập Airflow UI: http://localhost:8082
2. Login: `admin` / `admin`
3. Enable và trigger DAGs theo thứ tự:
   - `bronze_raw_ingestion` → Chờ complete
   - `silver_transformation` → Chờ complete
   - `gold_aggregation` → Chờ complete

#### Option 2: Individual DAG

```bash
# Trigger Bronze DAG
docker exec lakehouse_airflow airflow dags trigger bronze_raw_ingestion

# Check DAG status
docker exec lakehouse_airflow airflow dags list

# View logs
docker exec lakehouse_airflow airflow tasks logs bronze_raw_ingestion <task_id> <execution_date>
```

### Running ML Training & Forecasting

Sau khi Gold layer hoàn thành:

```bash
# LSTM v5 (Primary — hotel volume forecasting)
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 4g \
  --executor-memory 4g \
  --executor-cores 2 \
  /opt/spark/jobs/dl/train_province_lstm_v5.py

# XGBoost (Baseline — hotness forecasting)
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  /opt/spark/jobs/ml/train_and_forecast.py
```

Hoặc trigger từ Airflow (nếu đã thêm ML DAG).

### Viewing Forecast Results

#### Option 1: Gradio Web App (Recommended)

1. Truy cập http://localhost:7860
2. Tabs:
   - **🏆 Top Tỉnh Dự Báo**: Bảng xếp hạng tỉnh theo lượng đặt phòng
   - **📈 So sánh Tỉnh**: So sánh xu hướng giữa các tỉnh (tối đa 8 tỉnh)
   - **👥 Phân tích Du khách**: Tỷ lệ cặp đôi/gia đình/công tác/một mình theo tháng
   - **🧠 Thông tin Model**: Kết nối MLflow hiển thị metrics và hyperparameters

#### Option 2: Dremio SQL Query

```sql
-- LSTM v5: Hotel volume forecast
SELECT 
  province_name,
  forecast_month,
  predicted_hotel_volume,
  prediction_date
FROM gold.gold.province_month_forecast_lstm_next12
ORDER BY forecast_month, predicted_hotel_volume DESC
LIMIT 100;

-- XGBoost: Hotness forecast
SELECT 
  province_name,
  forecast_month,
  predicted_hotness,
  prediction_date
FROM gold.gold.province_month_forecast_next12
ORDER BY forecast_month, predicted_hotness DESC
LIMIT 100;
```

#### Option 3: MLflow UI

1. Truy cập http://localhost:5001
2. Experiments:
   - `province_hotel_volume_forecasting_lstm_v5` — LSTM v5 runs
   - `province_hotness_forecasting` — XGBoost runs
3. View: Run metrics (RMSE, MAE, R², MAPE), actual vs predicted charts
4. Model Registry: `province_hotel_volume_forecaster_lstm_v5` / `province_hotness_forecaster`
