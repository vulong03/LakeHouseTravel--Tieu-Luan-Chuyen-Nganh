# Tourism Data Analytics Platform
## Overview
Tourism Data Analytics Platform là một nền tảng phân tích dữ liệu du lịch toàn diện, được xây dựng theo kiến trúc Lakehouse hiện đại. Dự án tập trung vào việc thu thập, xử lý và phân tích dữ liệu từ nhiều nguồn (Booking.com, TikTok) để cung cấp insights về xu hướng du lịch và dự báo mức độ "hot" của các tỉnh thành Việt Nam.
## Architecture


<img width="1386" height="692" alt="image" src="https://github.com/user-attachments/assets/188de12f-5e62-4d43-bd48-3da7216fb721" />


### Medallion Architecture

Dự án tuân theo kiến trúc Medallion (Bronze → Silver → Gold):

- **Bronze Layer**: Dữ liệu thô (raw) từ các nguồn, lưu dưới dạng Parquet
- **Silver Layer**: Dữ liệu đã được làm sạch, chuẩn hóa và validate
- **Gold Layer**: Dữ liệu đã được aggregate, có sẵn dimensions và fact tables cho analytics

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
| **Visualization** | PowerBI | Latest  | Data visualization |
| **ML** | XGBoost | - | Time-series forecasting |
| **ML Ops** | MLflow | - | Experiment tracking, model registry |
| **Web Framework** | Gradio | - | Interactive web application |
| **NLP Library** | Underthesea | - | Vietnamese text processing |

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
   - **NLP Processing** với Underthesea:
     - Sentiment analysis (positive/negative)
     - Word count & unique word ratio
     - Emoji detection (positive/negative emojis)
   - Parent-child comment hierarchy
   - Deduplication

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
- `fact_hotel_review` - Hotel review analytics
- `fact_comment_nlp` - NLP analytics cho comments

**Key Features**:
- Surrogate keys (SK) cho tất cả dimensions
- Foreign key relationships
- Partition by province_sk cho performance
- ACID transactions với Iceberg MERGE operations

### ML Layer (Forecasting)

**Job**: `train_and_forecast.py`  
**Schedule**: Triggered sau Gold layer aggregation  
**Purpose**: Train XGBoost model để dự báo province hotness

**Pipeline**:

1. **Feature Engineering**
   - Calculate `hotness_score` từ 11 metrics:
     - Volume: total_comments, total_posts
     - Post Engagement: total_post_likes, total_post_saves
     - Sentiment: positive_ratio, avg_sentiment_score
     - NLP Richness: avg_words_per_comment, avg_unique_word_ratio, total_positive_emojis, total_emojis, total_negative_emojis
   
   - Temporal features:
     - Month (1-12)
     - Month sin/cos (cyclical encoding)
   
   - Lag features:
     - hotness_lag_1, lag_2, lag_3, lag_12 (1, 2, 3, 12 tháng trước)
     - hotness_rolling_avg_3m (rolling average 3 tháng)

2. **Model Training**
   - Algorithm: XGBoost Regressor
   - Train/Test Split: 70/30
   - Cross-validation với time series split
   - Hyperparameters:
     ```python
     {
       "n_estimators": 200,
       "max_depth": 4,
       "learning_rate": 0.01,
       "min_child_weight": 5,
       "subsample": 0.7,
       "colsample_bytree": 0.7,
       "gamma": 0.1,
       "reg_alpha": 0.1,
       "reg_lambda": 1.0
     }


3. **MLflow Tracking**
   - Experiment name: `province_hotness_forecasting`
   - Logged metrics: RMSE, MAE, R², MAPE
   - Artifacts: Feature importance plots, actual vs predicted charts
   - Model registry: `province_hotness_forecaster`

4. **Forecasting**
   - Recursive autoregressive forecasting (12 tháng)
   - Output table: `gold.gold.province_month_forecast_next12`
   - Columns: province_sk, province_name, forecast_month, predicted_hotness, prediction_date

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
# Trigger ML job từ Spark
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
2. Select:
   - **Start Month**: Tháng bắt đầu dự báo
   - **Forecast Months**: Số tháng dự báo (1-12)
   - **Region Filter**: Vùng miền (optional)
   - **Top N Provinces**: Số lượng tỉnh hiển thị
3. Click **"Forecast Province Hotness"**
4. Xem:
   - Line chart: Hotness trend theo thời gian
   - Table: Top provinces với predicted hotness scores

#### Option 2: Dremio SQL Query

```sql
-- View forecast results
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
2. Click experiment: `province_hotness_forecasting`
3. View:
   - Run metrics (RMSE, MAE, R²)
   - Feature importance plots
   - Actual vs Predicted charts
4. Download model từ Models tab
</div>
