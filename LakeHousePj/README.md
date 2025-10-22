# Lakehouse Tourism Analytics Platform

Modern data lakehouse architecture for analyzing tourism trends using social media and online reviews.

## 🏗️ Architecture

### Foundation Layer ✅
- **PostgreSQL**: Metadata database for Hive Metastore and Airflow
- **MinIO**: S3-compatible object storage (Bronze/Silver/Gold layers)
- **Hive Metastore**: Centralized metadata management

### Processing Layer ✅
- **Apache Spark 3.5.0**: ETL/ELT processing engine with PySpark
- **Apache Iceberg 1.4.3**: Table format for ACID transactions & time travel
- **Hadoop AWS 3.3.4**: S3A filesystem for MinIO integration

### Orchestration Layer (Coming Soon)
- **Apache Airflow**: Workflow orchestration and scheduling

### Analytics & ML Layer (Coming Soon)
- **Dremio**: SQL query engine
- **MLflow**: ML experiment tracking
- **PyTorch/Scikit-learn**: ML models

### Application Layer (Coming Soon)
- **Power BI**: Business intelligence dashboards
- **Gradio**: Web application for predictions

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- At least 8GB RAM
- PowerShell (Windows) or Bash (Linux/Mac)

### Step 1: Clone and Setup
```powershell
cd LakeHousePj
```

### Step 2: Start Foundation Services
```powershell
docker-compose up -d
```

### Step 3: Verify Services
```powershell
.\scripts\test-foundation.ps1
```

### Step 4: Access Services
- **MinIO Console**: http://localhost:9001
  - Username: `minioadmin`
  - Password: `minioadmin123`
- **Spark UI**: http://localhost:4040 (when jobs are running)
- **Spark Master UI**: http://localhost:8080
- **PostgreSQL**: localhost:5432
  - Username: `lakehouse_user`
  - Password: `lakehouse_pass`
- **Hive Metastore**: localhost:9083

## 📁 Project Structure

```
LakeHousePj/
├── docker-compose.yml          # Main orchestration file
├── .env                        # Environment variables
├── postgres/                   # PostgreSQL configuration
│   └── init/                   # Initialization scripts
├── minio/                      # MinIO storage
│   ├── data/                   # Data storage
│   └── config/                 # MinIO config
├── hive/                       # Hive Metastore
│   ├── Dockerfile              # Custom Hive image
│   ├── init/                   # Hive configuration
│   └── data/                   # Hive data
├── data/                       # Source data
│   └── raw/                    # Raw CSV files
│       ├── booking/            # Booking.com data
│       └── tiktok/             # TikTok data
├── scripts/                    # Utility scripts
└── docs/                       # Documentation
```

## 🔧 Common Commands

### Start all services
```powershell
docker-compose up -d
```

### View logs
```powershell
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f hive-metastore
```

### Stop services
```powershell
docker-compose down
```

### Full reset (including data)
```powershell
docker-compose down -v
```

### Check service health
```powershell
docker-compose ps
```

## 🐛 Troubleshooting

### PostgreSQL not connecting
```powershell
docker-compose logs postgres
docker exec lakehouse_postgres pg_isready -U lakehouse_user
```

### MinIO buckets not created
```powershell
docker-compose logs minio-init
docker exec lakehouse_minio mc ls myminio
```

### Hive Metastore connection issues
```powershell
docker-compose logs hive-metastore
# Check if PostgreSQL is healthy first
```

## 📊 Data Flow

```
Raw Data (CSV) 
    ↓
MinIO Bronze Bucket (s3a://bronze/)
    ↓
Spark Transformation
    ↓
MinIO Silver Bucket (s3a://silver/)
    ↓
Spark Aggregation
    ↓
MinIO Gold Bucket (s3a://gold/)
    ↓
Analytics (Dremio, Power BI)
```

## 🎯 Project Status

- [x] Foundation Layer (PostgreSQL, MinIO, Hive)
- [x] Processing Layer (Spark 3.5 + Iceberg 1.4.3)
- [ ] Orchestration Layer (Airflow)
- [ ] Analytics Layer (Dremio, MLflow)
- [ ] Application Layer (Gradio, Power BI)

## 📝 License

This is an academic project for research purposes.

## 👥 Contributors

- Your Name - Tourism Analytics Research

## 📚 References

- Apache Iceberg Documentation
- Databricks Lakehouse Architecture
- MinIO S3 Compatible Storage
