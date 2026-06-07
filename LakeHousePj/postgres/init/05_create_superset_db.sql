-- Create Superset database
-- Create database superset_db if it does not exist
SELECT 'CREATE DATABASE "superset_db"' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset_db')\gexec

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE "superset_db" TO lakehouse_user;
