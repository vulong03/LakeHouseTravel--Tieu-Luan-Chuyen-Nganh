-- Create MLflow database for experiment tracking
CREATE DATABASE mlflow_db;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE mlflow_db TO lakehouse_user;

\c mlflow_db

-- Create schema
CREATE SCHEMA IF NOT EXISTS mlflow;
GRANT ALL PRIVILEGES ON SCHEMA mlflow TO lakehouse_user;
