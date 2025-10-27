-- File Tracking Table for Bronze Layer Incremental Ingestion
-- Purpose: Track which files have been ingested to avoid duplicates

CREATE TABLE IF NOT EXISTS file_ingestion_log (
    id SERIAL PRIMARY KEY,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_size_bytes BIGINT,
    file_checksum TEXT NOT NULL UNIQUE,  -- MD5 hash for deduplication
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_ingested INTEGER,
    table_name TEXT,  -- Which Bronze table this file was ingested into
    status TEXT CHECK (status IN ('success', 'failed', 'in_progress')),
    error_message TEXT,
    ingestion_details JSONB,  -- Optional: breakdown for multi-table ingestion (e.g., {"tables": [{"name": "raw_tiktok_post_metadata", "records": 1}, {"name": "raw_tiktok_post_comments", "records": 150}]})
    CONSTRAINT unique_file_checksum UNIQUE(file_checksum)
);

-- Index for fast lookup
CREATE INDEX IF NOT EXISTS idx_file_checksum ON file_ingestion_log(file_checksum);
CREATE INDEX IF NOT EXISTS idx_file_name ON file_ingestion_log(file_name);
CREATE INDEX IF NOT EXISTS idx_table_name ON file_ingestion_log(table_name);
CREATE INDEX IF NOT EXISTS idx_ingestion_timestamp ON file_ingestion_log(ingestion_timestamp);

-- Comment
COMMENT ON TABLE file_ingestion_log IS 'Tracks ingested files for incremental loading';
COMMENT ON COLUMN file_ingestion_log.file_checksum IS 'MD5 hash to detect duplicate/changed files';
COMMENT ON COLUMN file_ingestion_log.table_name IS 'Target Bronze table (e.g., tiktok_videos_metadata, tiktok_posts_raw)';
COMMENT ON COLUMN file_ingestion_log.ingestion_details IS 'Optional JSONB for multi-table ingestion breakdown';
