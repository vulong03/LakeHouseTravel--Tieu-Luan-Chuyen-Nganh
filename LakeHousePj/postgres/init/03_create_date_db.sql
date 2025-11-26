-- Create Date_DB database
-- Note: This script only creates the database
-- The actual dim_date table creation and data population is done by:
--   - Manual: scripts/setup-date-db.ps1 (uses data/DateDimension/Date_Dimension_2015_2025.sql)
--   - Or run the SQL file directly after database is created

-- Create database Date_DB (if not exists)
-- Note: PostgreSQL doesn't support CREATE DATABASE IF NOT EXISTS
-- This will fail if database already exists, which is OK
SELECT 'CREATE DATABASE "date_db"' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'date_db')\gexec

-- Grant permissions to lakehouse_user
GRANT ALL PRIVILEGES ON DATABASE "date_db" TO lakehouse_user;
