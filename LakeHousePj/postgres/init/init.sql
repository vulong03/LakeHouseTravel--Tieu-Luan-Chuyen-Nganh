-- ============================================
-- PostgreSQL Initialization Script
-- For Hive Metastore Database
-- ============================================

-- Create metastore database (if not exists via env var)
-- This is just a backup script, database should be created by POSTGRES_DB env

-- Grant necessary privileges
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO lakehouse_user;

-- Create schema if needed
\c metastore_db

-- Log completion
SELECT 'PostgreSQL initialization completed for Hive Metastore' AS status;
