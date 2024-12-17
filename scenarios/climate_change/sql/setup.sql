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


SELECT year, AVG(temperature_anomaly) AS avg_anomaly
FROM gistemp
GROUP BY year
ORDER BY year;

CREATE MATERIALIZED VIEW monthly_temp_anomalies AS
SELECT date_trunc('month', measure_date) AS month,
       AVG(value) AS avg_temp
FROM ghcn_daily_test
WHERE element = 'TAVG'
GROUP BY 1;


CREATE INDEX idx_gistemp_year ON gistemp(year);
CREATE INDEX idx_sea_ice_date ON sea_ice_extent(date);



SELECT
    MIN(measurement_date) AS earliest_date,
    MAX(measurement_date) AS latest_date
FROM ghcn_daily_test;


SELECT
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE station_id IS NULL) AS null_station_ids,
    COUNT(*) FILTER (WHERE measurement_date IS NULL) AS null_dates,
    COUNT(*) FILTER (WHERE element IS NULL) AS null_elements,
    COUNT(*) FILTER (WHERE value IS NULL) AS null_values
FROM ghcn_daily_test;

climate_analysis-#     COUNT(*) AS total_records,
climate_analysis-#     COUNT(*) FILTER (WHERE station_id IS NULL) AS null_station_ids,
climate_analysis-#     COUNT(*) FILTER (WHERE measurement_date IS NULL) AS null_dates,
climate_analysis-#     COUNT(*) FILTER (WHERE element IS NULL) AS null_elements,
climate_analysis-#     COUNT(*) FILTER (WHERE value IS NULL) AS null_values
climate_analysis-# FROM ghcn_daily_test;
 total_records | null_station_ids | null_dates | null_elements | null_values
---------------+------------------+------------+---------------+-------------
    3123643189 |                0 |          0 |             0 |           0
(1 row)



-- Index on station_id
CREATE INDEX idx_ghcn_station_id ON ghcn_daily_test(station_id);

-- Index on measurement_date
CREATE INDEX idx_ghcn_measurement_date ON ghcn_daily_test(measurement_date);

-- Composite index on station_id and measurement_date
CREATE INDEX idx_ghcn_station_date ON ghcn_daily_test(station_id, measurement_date);

-- Index on element
CREATE INDEX idx_ghcn_element ON ghcn_daily_test(element);

CREATE INDEX idx_ghcn_year ON ghcn_daily_test(EXTRACT(YEAR FROM measurement_date));

EXPLAIN ANALYZE
SELECT DISTINCT EXTRACT(YEAR FROM measurement_date) AS year
FROM ghcn_daily_test
ORDER BY year;


EXPLAIN ANALYZE
WITH yearly_data AS (
  SELECT DISTINCT ON (EXTRACT(YEAR FROM measurement_date))
    EXTRACT(YEAR FROM measurement_date) AS year
  FROM ghcn_daily_test
)
SELECT year FROM yearly_data ORDER BY year;

CREATE MATERIALIZED VIEW mv_ghcn_years AS
SELECT DISTINCT EXTRACT(YEAR FROM measurement_date) AS year
FROM ghcn_daily_test
ORDER BY year;

ANALYZE ghcn_daily_test;

-- ======================================================================

SELECT gp_segment_id, COUNT(*) as row_count
FROM ghcn_daily_test
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

NOTICE:  One or more columns in the following table(s) do not have statistics: ghcn_daily_test
HINT:  For non-partitioned tables, run analyze <table_name>(<column_list>). For partitioned tables, run analyze rootpartition <table_name>(<column_list>). See log for columns missing statistics.
 gp_segment_id | row_count
---------------+-----------
             0 | 194249223
             1 | 200599130
             2 | 193214033
             3 | 196103132
             4 | 191817257
             5 | 185704810
             6 | 197945977
             7 | 202155193
             8 | 201295451
             9 | 188853017
            10 | 199570572
            11 | 189709694
            12 | 188508852
            13 | 199890850
            14 | 197122925
            15 | 196982895
(16 rows)

-- ======================================================================

SELECT
    substring(c.relname from 'ghcn_daily_test_1_prt_(.*)') AS partition_name,
    COUNT(*) as row_count
FROM ghcn_daily_test t
JOIN pg_class c ON c.oid = t.tableoid
GROUP BY c.relname
ORDER BY c.relname;



SELECT schemaname, partition_name,
       pg_size_pretty(pg_total_relation_size(partitionschemaname||'.'||partitiontablename)) as total_size
FROM pg_partitions
WHERE tablename = 'ghcn_daily_test'
ORDER BY partitiontablename;


SELECT partition_name, COUNT(*) as row_count
FROM ghcn_daily_test
GROUP BY partition_name
ORDER BY partition_name;

SELECT MIN(measurement_date), MAX(measurement_date), COUNT(*)
FROM ghcn_daily_test_1_prt_p_default;


SELECT
    substring(c.relname from 'ghcn_daily_test_1_prt_(.*)') AS partition_name,
    COUNT(*) as row_count
FROM ghcn_daily_test t
JOIN pg_class c ON c.oid = t.tableoid
GROUP BY c.relname
ORDER BY c.relname;


-- To verify the new partition structure after running the SQL:

SELECT nspname AS schemaname,
       relname AS partition_name,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE relkind = 'r'
  AND relname LIKE 'ghcn_daily_test%'
ORDER BY c.relname;


-- And to check the data distribution:

SELECT
    substring(c.relname from 'ghcn_daily_test_1_prt_(.*)') AS partition_name,
    COUNT(*) as row_count
FROM ghcn_daily_test t
JOIN pg_class c ON c.oid = t.tableoid
GROUP BY c.relname
ORDER BY c.relname;

CREATE INDEX idx_ghcn_measurement_date ON ghcn_daily_test (measurement_date);
CREATE INDEX idx_ghcn_measurement_date_value ON ghcn_daily_test (measurement_date, value);

SELECT COUNT(*) FROM ghcn_daily_test_1_prt_p1950;
SELECT COUNT(*) FROM ghcn_daily_test_1_prt_p2000;

ANALYZE ghcn_daily_test;

EXPLAIN ANALYZE
SELECT EXTRACT(YEAR FROM measurement_date) as year, AVG(value)
FROM ghcn_daily_test
WHERE measurement_date BETWEEN '1800-01-01' AND '1850-12-31'
GROUP BY year
ORDER BY year;

EXPLAIN ANALYZE
SELECT EXTRACT(YEAR FROM measurement_date) as year, AVG(value)
FROM ghcn_daily_test
WHERE measurement_date BETWEEN '1950-01-01' AND '2000-12-31'
GROUP BY year
ORDER BY year;

SET enable_seqscan = off;
EXPLAIN ANALYZE
SELECT EXTRACT(YEAR FROM measurement_date) as year, AVG(value)
FROM ghcn_daily_test
WHERE measurement_date BETWEEN '1950-01-01' AND '2000-12-31'
GROUP BY year
ORDER BY year;
SET enable_seqscan = on;


SELECT
    c.relname AS table_name,
    a.blocksize,
    CASE
        WHEN a.columnstore THEN 'column'
        ELSE 'row'
    END AS storage_orientation,
    a.compresstype AS compression_type,
    a.compresslevel AS compression_level,
    a.checksum AS checksum_enabled
FROM
    pg_class c
JOIN
    pg_appendonly a
ON
    c.oid = a.relid
WHERE
    c.oid IN (
        SELECT inhrelid
        FROM pg_inherits
        WHERE inhparent = 'ghcn_daily_test'::regclass
    );
           table_name            | blocksize | storage_orientation | compression_type | compression_level | checksum_enabled
---------------------------------+-----------+---------------------+------------------+-------------------+------------------
 ghcn_daily_test_1_prt_p1766     |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p1767     |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p1768     |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p1769     |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p1770     |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p1764     |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p1765     |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p1771     |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p_default |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p1750     |     32768 | column              |                  |                 0 | t
 ghcn_daily_test_1_prt_p1751     |     32768 | column              |                  |                 0 | t
