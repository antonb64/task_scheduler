ALTER TABLE attempts ADD COLUMN diagnostic_json TEXT;
ALTER TABLE attempts ADD COLUMN output_metadata_json TEXT;
ALTER TABLE attempts ADD COLUMN exit_code INTEGER;
ALTER TABLE attempts ADD COLUMN signal TEXT;
ALTER TABLE attempts ADD COLUMN duration_ms INTEGER;
