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
