SHOW max_wal_size;
ALTER SYSTEM SET max_wal_size = '16GB';

SHOW maintenance_work_mem;
ALTER SYSTEM SET maintenance_work_mem = '1GB';

SELECT pg_reload_conf();

CREATE DATABASE climate_analysis;
