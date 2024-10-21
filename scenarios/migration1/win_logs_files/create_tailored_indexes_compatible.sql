-- Function to check if an index exists
CREATE OR REPLACE FUNCTION index_exists(idx_name text) RETURNS boolean AS $$
DECLARE
  exists boolean;
BEGIN
  SELECT INTO exists COUNT(*) > 0 FROM pg_class WHERE relname = idx_name;
  RETURN exists;
END;
$$ LANGUAGE plpgsql;

-- Timestamp Index (already part of the primary key, but adding for completeness)
DO $$
BEGIN
  IF NOT index_exists('idx_win_logs_timestamp') THEN
    CREATE INDEX idx_win_logs_timestamp ON win_logs (timestamp_col1);
  END IF;
END $$;

-- Composite Index on timestamp and text_col1
DO $$
BEGIN
  IF NOT index_exists('idx_win_logs_timestamp_text') THEN
    CREATE INDEX idx_win_logs_timestamp_text ON win_logs (timestamp_col1, text_col1);
  END IF;
END $$;

-- Bitmap Indexes on boolean columns
DO $$
BEGIN
  IF NOT index_exists('idx_win_logs_boolean1') THEN
    CREATE INDEX idx_win_logs_boolean1 ON win_logs USING bitmap (boolean_col1);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT index_exists('idx_win_logs_boolean2') THEN
    CREATE INDEX idx_win_logs_boolean2 ON win_logs USING bitmap (boolean_col2);
  END IF;
END $$;

-- GIN Index for Text Search on text_col1
DO $$
BEGIN
  IF NOT index_exists('idx_win_logs_text_gin') THEN
    CREATE INDEX idx_win_logs_text_gin ON win_logs USING gin (to_tsvector('english', text_col1));
  END IF;
END $$;

-- Partial Index on int_col1 for values > 1000
DO $$
BEGIN
  IF NOT index_exists('idx_win_logs_partial') THEN
    CREATE INDEX idx_win_logs_partial ON win_logs (int_col1) WHERE int_col1 > 1000;
  END IF;
END $$;

-- Expression Index for extracting year from timestamp_col1
DO $$
BEGIN
  IF NOT index_exists('idx_win_logs_extract_year') THEN
    CREATE INDEX idx_win_logs_extract_year ON win_logs ((EXTRACT(YEAR FROM timestamp_col1)));
  END IF;
END $$;

-- B-tree Index on text_col1 for equality searches
DO $$
BEGIN
  IF NOT index_exists('idx_win_logs_text1_btree') THEN
    CREATE INDEX idx_win_logs_text1_btree ON win_logs (text_col1);
  END IF;
END $$;

-- Analyze the table to update statistics
ANALYZE win_logs;

-- Drop the helper function
DROP FUNCTION IF EXISTS index_exists(text);
