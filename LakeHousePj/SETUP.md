# 🚀 Lakehouse Setup Guide

## 📋 Prerequisites

- Docker Desktop installed and running
- Git
- PowerShell (Windows) or Bash (Linux/Mac)

## 🔧 Quick Start

### 1. Clone Repository

```bash
git clone <repository-url>
cd LakeHousePj
```

### 2. Start All Services

```bash
docker compose up -d
```

**That's it!** 🎉 All data will be managed by Docker volumes automatically.

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin123 |
| **Spark Master UI** | http://localhost:8080 | - |
| **Spark History** | http://localhost:18080 | - |
| **Airflow** | http://localhost:8082 | admin / admin |
| **pgAdmin** | http://localhost:5050 | admin@admin.com / admin123 |

## 📦 Data Storage

### Bind Mounts (Direct access to files)

All data is stored in **local folders** for easy access and inspection:

```yaml
# Metadata (can view with database tools)
./postgres/data/           # ✅ Hive metadata + file_ingestion_log
./airflow/postgres-data/   # ✅ Airflow metadata (DAGs, runs, connections)
./pgadmin/data/            # ✅ pgAdmin sessions and preferences

# Lakehouse Data (can browse with Windows Explorer)
./minio/data/              # ✅ Bronze/Silver/Gold lakehouse data
  ├── bronze/              # Raw ingested data
  ├── silver/              # Cleaned, transformed data
  ├── gold/                # Aggregated, analytics-ready data
  └── scratch/             # Temporary Hive scratch space

# Source & Code
./data/raw/                # ✅ Source CSV files
./airflow/dags/            # ✅ Airflow DAG definitions
./spark/jobs/              # ✅ Spark job scripts
./postgres/init/           # ✅ PostgreSQL init scripts
```

**Benefits:**
- ✅ **Easy inspection**: Open with Windows Explorer / Finder
- ✅ **Easy backup**: Copy folders to backup location
- ✅ **Easy migration**: Copy entire project to another machine
- ✅ **Database tools**: Connect pgAdmin/DBeaver to view PostgreSQL files
- ✅ **MinIO Console**: Browse lakehouse data visually

**Important Folders:**
| Folder | Purpose | Can Delete? |
|--------|---------|-------------|
| `postgres/data/` | Hive metadata (CRITICAL) | ❌ NO - Will lose all table schemas |
| `airflow/postgres-data/` | Airflow history | ⚠️ Optional - Will lose DAG run history |
| `pgadmin/data/` | pgAdmin preferences | ✅ YES - Can recreate |
| `minio/data/` | All lakehouse data | ❌ NO - Your actual data! |
| `data/raw/` | Source files | ⚠️ Keep - Can re-download if needed |

## 🧹 Maintenance

### Backup Data

```powershell
# Create backup folder
New-Item -Path "backup" -ItemType Directory -Force

# Backup metadata
Copy-Item -Path "postgres\data\" -Destination "backup\postgres_data_$(Get-Date -Format 'yyyyMMdd_HHmmss')\" -Recurse
Copy-Item -Path "airflow\postgres-data\" -Destination "backup\airflow_data_$(Get-Date -Format 'yyyyMMdd_HHmmss')\" -Recurse

# Backup lakehouse data
Copy-Item -Path "minio\data\" -Destination "backup\minio_data_$(Get-Date -Format 'yyyyMMdd_HHmmss')\" -Recurse
```

### View Data

```powershell
# Open folders in Windows Explorer
explorer postgres\data
explorer minio\data\bronze
explorer minio\data\silver

# Or use database tools to inspect PostgreSQL
# Connect to: localhost:5432
# Database: metastore_db
# User/Password: (from .env file)
```

### Clean Reset

```powershell
# Stop all containers
docker compose down

# Delete data (CAUTION!)
Remove-Item -Path "postgres\data\*" -Recurse -Force
Remove-Item -Path "airflow\postgres-data\*" -Recurse -Force
Remove-Item -Path "pgadmin\data\*" -Recurse -Force

# Start fresh
docker compose up -d
```

## 🔍 Troubleshooting

### Services not starting

```bash
# Check logs
docker compose logs <service-name>

# Common services to check:
docker compose logs postgres
docker compose logs airflow-postgres
docker compose logs hive-metastore
```

### Permission errors (rare with volumes)

```bash
# If you still encounter permissions, try:
docker compose down
docker volume rm lakehousepj_postgres_data lakehousepj_airflow_postgres_data lakehousepj_pgadmin_data
docker compose up -d
```

### Port conflicts

```bash
# Check what's using ports
netstat -ano | findstr :5432
netstat -ano | findstr :9000
netstat -ano | findstr :8080
```

## 📝 Development Workflow

### 1. First Time Setup

```bash
docker compose up -d
# Wait for all services to be healthy (~2 minutes)
docker compose ps
```

### 2. Run Bronze Ingestion

```bash
# Access Airflow UI: http://localhost:8082
# Trigger DAG: bronze_layer_ingestion
```

### 3. Run Silver Transformation

```bash
# Trigger DAG: silver_layer_transformation
```

### 4. View Data

```bash
# MinIO Console: http://localhost:9001
# Browse buckets: bronze, silver, gold
```

## 🎯 Key Advantages of This Setup

1. **✅ Reproducible**: Clone → `docker compose up` → Works!
2. **✅ Cross-platform**: Same commands on Windows/Linux/Mac
3. **✅ Isolated**: Data managed by Docker, not affected by host OS
4. **✅ Easy cleanup**: One command to reset everything
5. **✅ No manual folder creation**: Docker creates volumes automatically
6. **✅ No permission issues**: Docker handles uid/gid mapping

## 📚 Next Steps

- Read [HIVE_SETUP_SUMMARY.md](docs/HIVE_SETUP_SUMMARY.md) for Hive details
- Read [BRONZE_VERIFICATION_SUMMARY.md](docs/BRONZE_VERIFICATION_SUMMARY.md) for data ingestion
- Check [scripts/](scripts/) for utility scripts

## 💡 Tips

- Use `docker compose logs -f <service>` to follow logs in real-time
- Use `docker compose restart <service>` to restart specific service
- Use `docker compose down && docker compose up -d` for full restart
- Keep `.env` file for environment variables (not tracked in git)

---

**Happy Data Engineering! 🚀**

