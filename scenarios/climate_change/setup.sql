-- ===================================================================

CREATE DATABASE climate_analysis;
\c climate_analysis

-- ===================================================================

DROP TABLE IF EXISTS gistemp;
CREATE TABLE gistemp (
    Year INT,
    Jan NUMERIC,
    Feb NUMERIC,
    Mar NUMERIC,
    Apr NUMERIC,
    May NUMERIC,
    Jun NUMERIC,
    Jul NUMERIC,
    Aug NUMERIC,
    Sep NUMERIC,
    Oct NUMERIC,
    Nov NUMERIC,
    Dec NUMERIC,
    J_D NUMERIC,
    D_N NUMERIC,
    DJF NUMERIC,
    MAM NUMERIC,
    JJA NUMERIC,
    SON NUMERIC
);

COPY gistemp FROM PROGRAM 'tail -n +3 /home/gpadmin/data/GLB.Ts+dSST.csv' WITH (FORMAT CSV, DELIMITER ',', NULL '***');

SELECT * FROM gistemp LIMIT 10;

CREATE OR REPLACE VIEW gistemp_monthly AS
SELECT year,
       unnest(ARRAY['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']) AS month,
       unnest(ARRAY[jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec]) AS temperature_anomaly
FROM gistemp;

SELECT * FROM gistemp_monthly LIMIT 12;

-- ===================================================================

DROP TABLE IF EXISTS sea_ice_extent;
CREATE TABLE sea_ice_extent (
    Year INT,
    Month INT,
    Day INT,
    Extent NUMERIC,
    Missing NUMERIC,
    Source_Data TEXT
);

COPY sea_ice_extent FROM PROGRAM 'tail -n +3 /home/gpadmin/data/N_seaice_extent_daily_v3.0.csv' WITH (
    FORMAT CSV,
    DELIMITER ',',
    NULL '',
    QUOTE '"'
);

SELECT * FROM sea_ice_extent ORDER BY Year, Month, Day LIMIT 10;

CREATE OR REPLACE VIEW sea_ice_extent_view AS
SELECT
    (Year || '-' || LPAD(Month::text, 2, '0') || '-' || LPAD(Day::text, 2, '0'))::DATE AS Date,
    Extent,
    Missing,
    Source_Data
FROM sea_ice_extent;

SELECT * FROM sea_ice_extent_view ORDER BY Date LIMIT 10;

-- ===================================================================

DROP TABLE IF EXISTS ghcn_daily;

-- ===================================================================

SHOW max_wal_size;
ALTER SYSTEM SET max_wal_size = '8GB';

SHOW checkpoint_timeout;
ALTER SYSTEM SET checkpoint_timeout = '30min';

SHOW maintenance_work_mem;
ALTER SYSTEM SET maintenance_work_mem = '1GB';

SELECT pg_reload_conf();


ALTER SYSTEM SET log_checkpoints = on;
SELECT pg_reload_conf();

ALTER SYSTEM SET max_wal_size = '16GB';
SELECT pg_reload_conf();


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




CREATE TABLE ghcn_daily_test (
    station_id VARCHAR(20),
    date DATE,
    element VARCHAR(10),
    value NUMERIC
) DISTRIBUTED BY (station_id)  -- choose an appropriate distribution key
  PARTITION BY RANGE (measurement_date) (
    START ('2022-01-01') END ('2024-01-01') EVERY (1 YEAR);
);
