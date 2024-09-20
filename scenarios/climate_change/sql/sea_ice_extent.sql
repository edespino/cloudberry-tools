\c climate_analysis

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
