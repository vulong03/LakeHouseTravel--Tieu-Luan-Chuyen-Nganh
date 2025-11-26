# DataLakehouse
## Introduce

## System Architecture
<img width="618" height="301" alt="image" src="https://github.com/user-attachments/assets/3f4208e7-597d-4741-9304-28dba639dc7b" />

## Components & Naming Standards
### Foundation Layer 
- **PostgreSQL**: Metadata database for Hive Metastore and Airflow
- **MinIO**: S3-compatible object storage (Bronze/Silver/Gold layers)
- **Hive Metastore**: Centralized metadata management
### Processing Layer 
- **Apache Spark 3.5.0**: ETL/ELT processing engine with PySpark
- **Apache Iceberg 1.4.3**: Table format for ACID transactions & time travel
- **Hadoop AWS 3.3.4**: S3A filesystem for MinIO integration
### Orchestration Layer 

### Analytics & ML Layer 

### Application Layer 

## Pipelines
### Ingestion

### Transformation 

### Training Model

### Serving & Visualization

## Quick Start
