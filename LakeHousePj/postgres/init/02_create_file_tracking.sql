-- File Tracking Table for Multi-Layer Incremental Ingestion
-- Purpose: Track which files have been ingested across Bronze/Silver/Gold layers

CREATE TABLE IF NOT EXISTS file_ingestion_log (
    id SERIAL PRIMARY KEY,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_size_bytes BIGINT,
    file_checksum TEXT NOT NULL,  -- MD5 hash for deduplication
    layer TEXT NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),  -- Data layer tracking (must be explicitly specified)
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_ingested INTEGER,
    table_name TEXT,  -- Which table this file was ingested into (e.g., 'booking_hotels_list_raw', 'silver.booking_hotels_list')
    status TEXT CHECK (status IN ('success', 'failed', 'in_progress')),
    error_message TEXT,
    ingestion_details JSONB,  -- Optional: breakdown for multi-table ingestion or processing details
    CONSTRAINT unique_file_checksum_layer UNIQUE(file_checksum, layer)  -- Same file can exist in different layers
);

-- Indexes for fast lookup
CREATE INDEX IF NOT EXISTS idx_file_checksum ON file_ingestion_log(file_checksum);
CREATE INDEX IF NOT EXISTS idx_file_name ON file_ingestion_log(file_name);
CREATE INDEX IF NOT EXISTS idx_table_name ON file_ingestion_log(table_name);
CREATE INDEX IF NOT EXISTS idx_layer ON file_ingestion_log(layer);
CREATE INDEX IF NOT EXISTS idx_status ON file_ingestion_log(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_timestamp ON file_ingestion_log(ingestion_timestamp);

-- Comments
COMMENT ON TABLE file_ingestion_log IS 'Tracks ingested files across all layers (Bronze/Silver/Gold) for incremental loading and data lineage';
COMMENT ON COLUMN file_ingestion_log.file_checksum IS 'MD5 hash to detect duplicate/changed files';
COMMENT ON COLUMN file_ingestion_log.layer IS 'Data layer: bronze (raw copy), silver (cleaned/validated), gold (aggregated)';
COMMENT ON COLUMN file_ingestion_log.table_name IS 'Target table (e.g., booking_hotels_list_raw for bronze, silver.booking_hotels_list for silver)';
COMMENT ON COLUMN file_ingestion_log.ingestion_details IS 'Optional JSONB for multi-table ingestion breakdown or layer-specific processing details';
