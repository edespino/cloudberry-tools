-- ===================================================================

CREATE DATABASE climate_analysis;
\c climate_analysis

-- ===================================================================

DROP TABLE IF EXISTS ghcn_daily;

-- ===================================================================

SHOW config_file;
SELECT * FROM pg_file_settings;

SELECT * FROM pg_extension;


CREATE EXTERNAL TABLE ext_ghcn_daily (
    station_id VARCHAR(20),
    date DATE,
    element VARCHAR(10),
    value NUMERIC
) LOCATION ('gpfdist://mdw:8081/*.csv')
FORMAT 'CSV' (DELIMITER ',' NULL '' HEADER);

CREATE TABLE IF NOT EXISTS ghcn_daily_test (
    station_id VARCHAR(20),
    date DATE,
    element VARCHAR(10),
    value NUMERIC
) DISTRIBUTED BY (station_id);

CREATE TABLE IF NOT EXISTS ghcn_load_control_test (
    file_name VARCHAR(255),
    status VARCHAR(20),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate with file names from our test subset
INSERT INTO ghcn_load_control_test (file_name, status)
SELECT unnest(string_to_array(pg_read_file('/tmp/ghcn_test_files.txt'), E'\n')), 'PENDING'
WHERE length(trim(unnest)) > 0;

SELECT * FROM ghcn_load_control_test;



CREATE TABLE IF NOT EXISTS ghcn_daily_test (
    station_id VARCHAR(20),
    date DATE,
    element VARCHAR(10),
    value NUMERIC
) DISTRIBUTED BY (station_id);




DROP TABLE ghcn_daily_test;
CREATE TABLE ghcn_daily_test (
    station_id VARCHAR(20),
    date DATE,
    element VARCHAR(10),
    value NUMERIC
) DISTRIBUTED BY (station_id)  -- choose an appropriate distribution key
  PARTITION BY RANGE (measurement_date) (
    START ('2022-01-01') END ('2024-01-01') EVERY (1 YEAR);
);

SELECT count(*) from ghcn_daily_test;


DROP TABLE IF EXISTS ghcn_load_control_test;

CREATE TABLE ghcn_load_control_test (
    file_name VARCHAR(255) PRIMARY KEY,  -- The primary key ensures each file name is unique
    status VARCHAR(20),                  -- Status of the file (e.g., 'PENDING', 'COMPLETED', 'FAILED')
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Timestamp of the last update
    csv_record_count INT,                -- The number of records in the CSV file (excluding the header)
    inserted_row_count INT,              -- The number of rows inserted into the database
    error_condition VARCHAR(255)         -- Stores error information, if any (e.g., 'Row mismatch')
);

SELECT * FROM ghcn_load_control_test;
SELECT count(*) FROM ghcn_load_control_test;
