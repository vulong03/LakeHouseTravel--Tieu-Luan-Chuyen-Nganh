FROM python:3.10-slim

WORKDIR /mlflow

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install MLflow and dependencies
RUN pip install --no-cache-dir \
    mlflow==2.17.2 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.34 \
    pymysql==1.1.0

# Create directories
RUN mkdir -p /mlflow/artifacts /mlflow/data

# Expose MLflow UI port
EXPOSE 5000

# Default command - will be overridden in docker-compose
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
