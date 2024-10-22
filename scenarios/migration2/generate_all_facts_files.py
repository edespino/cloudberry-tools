#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse
import random
import sys
import os
import multiprocessing
from functools import partial

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

def generate_random_text_array():
    array_size = random.randint(1, 5)
    texts = ["text_{0}_{1}".format(random.randint(1, 100), random.randint(1, 1000))
            for _ in range(array_size)]
    return "{" + ",".join('"{0}"'.format(t) for t in texts) + "}"

def generate_random_hll():
    # Create a properly formatted empty HLL
    # Format: \x11 (header byte) followed by the correct number of zero bytes for specified parameters
    header = "11"  # Header byte indicating empty HLL
    body = "00" * 16384  # 2^14 bytes for log2m=14 (specified in hll(15,5,-1,1))
    return "\\x" + header + body

def create_table_sql():
    return """
DROP TABLE IF EXISTS all_facts CASCADE;

CREATE TABLE all_facts (
    -- Integer columns
    {0},
    -- Numeric columns
    {1},
    -- Text array columns
    {2},
    -- HLL columns
    {3},
    -- Text columns
    {4},
    -- Timestamp column
    event_time TIMESTAMP WITHOUT TIME ZONE,
    PRIMARY KEY (event_time)
) DISTRIBUTED BY (event_time)
PARTITION BY RANGE (event_time)
(
    PARTITION y2024m01 START ('2024-01-01'::timestamp) END ('2024-02-01'::timestamp),
    PARTITION y2024m02 START ('2024-02-01'::timestamp) END ('2024-03-01'::timestamp),
    PARTITION y2024m03 START ('2024-03-01'::timestamp) END ('2024-04-01'::timestamp),
    PARTITION y2024m04 START ('2024-04-01'::timestamp) END ('2024-05-01'::timestamp),
    PARTITION y2024m05 START ('2024-05-01'::timestamp) END ('2024-06-01'::timestamp),
    PARTITION y2024m06 START ('2024-06-01'::timestamp) END ('2024-07-01'::timestamp),
    PARTITION y2024m07 START ('2024-07-01'::timestamp) END ('2024-08-01'::timestamp),
    PARTITION y2024m08 START ('2024-08-01'::timestamp) END ('2024-09-01'::timestamp),
    PARTITION y2024m09 START ('2024-09-01'::timestamp) END ('2024-10-01'::timestamp),
    PARTITION y2024m10 START ('2024-10-01'::timestamp) END ('2024-11-01'::timestamp),
    PARTITION y2024m11 START ('2024-11-01'::timestamp) END ('2024-12-01'::timestamp),
    PARTITION y2024m12 START ('2024-12-01'::timestamp) END ('2025-01-01'::timestamp)
);
""".format(
    ",\n    ".join("int_col{} INTEGER".format(i) for i in range(1, 50)),
    ",\n    ".join("numeric_col{} NUMERIC".format(i) for i in range(1, 23)),
    ",\n    ".join("text_array_col{} TEXT[]".format(i) for i in range(1, 13)),
    ",\n    ".join("hll_col{} hll(15,5,-1,1)".format(i) for i in range(1, 13)),
    ",\n    ".join("text_col{} TEXT".format(i) for i in range(1, 10))
)

def create_indexes_sql():
    return """
-- Drop existing indexes if they exist
DROP INDEX IF EXISTS idx_all_facts_event_time;
DROP INDEX IF EXISTS idx_all_facts_text_cols;
DROP INDEX IF EXISTS idx_all_facts_int_cols;
DROP INDEX IF EXISTS idx_all_facts_numeric_cols;
DROP INDEX IF EXISTS idx_all_facts_text_array_gin;
DROP INDEX IF EXISTS idx_all_facts_recent_data;

-- Basic btree index for timestamp
CREATE INDEX idx_all_facts_event_time ON all_facts USING btree (event_time);

-- Partial index for 2024 data (using fixed date)
CREATE INDEX idx_all_facts_recent_data ON all_facts (event_time)
WHERE event_time >= '2024-01-01'::timestamp;

-- Multi-column index for frequently queried integer columns
CREATE INDEX idx_all_facts_int_cols ON all_facts (int_col1, int_col2, int_col3);

-- GIN indexes for array columns that need exact matches
CREATE INDEX idx_all_facts_array_gin_1 ON all_facts USING gin (text_array_col1);
CREATE INDEX idx_all_facts_array_gin_2 ON all_facts USING gin (text_array_col2);

-- Specialized index for numeric range queries
CREATE INDEX idx_all_facts_numeric_cols ON all_facts (numeric_col1, numeric_col2);

-- Partial composite index for active integer columns
CREATE INDEX idx_all_facts_int_composite ON all_facts (int_col1, int_col2)
WHERE int_col1 IS NOT NULL AND int_col2 IS NOT NULL;

-- Add text column index for pattern matching
CREATE INDEX idx_all_facts_text ON all_facts USING btree (text_col1, text_col2);

-- Analyze table statistics
ANALYZE all_facts;

-- Note: Monitor index usage with:
-- SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
-- FROM pg_stat_user_indexes
-- WHERE tablename = 'all_facts'
-- ORDER BY idx_scan DESC;
"""

def create_copy_command(output_dir):
    # Create lists for generating the SELECT statement
    int_cols = ["int_col{}".format(i) for i in range(1, 50)]
    numeric_cols = ["numeric_col{}".format(i) for i in range(1, 23)]
    text_array_cols = ["text_array_col{}".format(i) for i in range(1, 13)]
    text_cols = ["text_col{}".format(i) for i in range(1, 10)]

    # Create HLL initialization and update commands with balanced distribution
    hll_updates = []
    for i in range(1, 13):
        current_month = "2024-{:02d}-01".format(i)
        if i == 12:
            next_month = "2025-01-01"
        else:
            next_month = "2024-{:02d}-01".format(i + 1)

        hll_updates.append("""
        -- Create staging table for HLL values with explicit distribution
        CREATE TEMP TABLE hll_staging_{0} AS
        SELECT
            event_time,
            hll_empty(15,5,-1,1) as hll_value,
            width_bucket(random(), 0, 1, 10) as size_bucket  -- Create 10 buckets for better distribution
        FROM all_facts
        DISTRIBUTED BY (event_time);

        -- Add values to HLLs with balanced distribution
        UPDATE hll_staging_{0}
        SET hll_value = (
            SELECT hll_union_agg(hll_add(hll_empty(15,5,-1,1), hll_hash_any(value)))
            FROM (
                SELECT s.num || '_hll{0}_' ||
                    extract(epoch from hs.event_time)::text || '_' ||
                    s.num as value
                FROM hll_staging_{0} hs,
                LATERAL (
                    SELECT generate_series(1,
                        CASE hs.size_bucket
                            WHEN 1 THEN 10  -- 10% tiny sets
                            WHEN 2 THEN 25 + ({0} * 2)  -- 10% small sets
                            WHEN 3 THEN 25 + ({0} * 2)  -- 10% small sets
                            WHEN 4 THEN 50 + ({0} * 3)  -- 10% medium sets
                            WHEN 5 THEN 50 + ({0} * 3)  -- 10% medium sets
                            WHEN 6 THEN 50 + ({0} * 3)  -- 10% medium sets
                            WHEN 7 THEN 100 + ({0} * 5)  -- 10% large sets
                            WHEN 8 THEN 100 + ({0} * 5)  -- 10% large sets
                            WHEN 9 THEN 200 + ({0} * 10)  -- 10% very large sets
                            ELSE 200 + ({0} * 10)  -- 10% very large sets
                        END
                    ) as num
                ) s
                WHERE hs.event_time >= '{1}'::timestamp
                  AND hs.event_time < '{2}'::timestamp
            ) v
        )
        WHERE event_time >= '{1}'::timestamp
          AND event_time < '{2}'::timestamp;

        -- Update main table with the new HLL values
        UPDATE all_facts SET
            hll_col{0} = s.hll_value
        FROM hll_staging_{0} s
        WHERE all_facts.event_time = s.event_time;

        DROP TABLE hll_staging_{0};
        """.format(i, current_month, next_month))

    return """
SET gp_enable_segment_copy_checking=off;
SET bytea_output = 'hex';

DROP TABLE IF EXISTS temp_all_facts;
CREATE TEMP TABLE temp_all_facts (
    -- Integer columns
    {0},
    -- Numeric columns
    {1},
    -- Text array columns
    {2},
    -- HLL columns (as BYTEA for initial load)
    {3},
    -- Text columns
    {4},
    -- Timestamp column
    event_time TIMESTAMP WITHOUT TIME ZONE
) DISTRIBUTED BY (event_time);

COPY temp_all_facts FROM '{5}' WITH DELIMITER AS '|';

-- Insert data with empty HLLs
INSERT INTO all_facts
SELECT
    {6}
FROM temp_all_facts;

-- Populate HLLs with values
{7}

DROP TABLE temp_all_facts;

-- Analyze table for better query planning
ANALYZE all_facts;
""".format(
    ",\n    ".join("int_col{} INTEGER".format(i) for i in range(1, 50)),
    ",\n    ".join("numeric_col{} NUMERIC".format(i) for i in range(1, 23)),
    ",\n    ".join("text_array_col{} TEXT[]".format(i) for i in range(1, 13)),
    ",\n    ".join("hll_col{} BYTEA".format(i) for i in range(1, 13)),
    ",\n    ".join("text_col{} TEXT".format(i) for i in range(1, 10)),
    os.path.abspath(os.path.join(output_dir, 'test_data.tsv')),
    ",\n    ".join(
        int_cols +
        numeric_cols +
        text_array_cols +
        ["hll_empty(15,5,-1,1)".format(i) for i in range(1, 13)] +
        text_cols +
        ["event_time"]
    ),
    "\n".join(hll_updates)
)

def create_cardinality_analysis_sql():
    array_analysis = ""
    for i in range(1, 13):
        if i > 1:
            array_analysis += "    UNION ALL\n"
        array_analysis += """    SELECT
        'text_array_col{0}' as column_name,
        COUNT(*) as total_rows,
        COUNT(DISTINCT text_array_col{0}) as distinct_arrays,
        AVG(array_length(text_array_col{0}, 1))::numeric(10,2) as avg_array_length
    FROM all_facts
    WHERE text_array_col{0} IS NOT NULL\n""".format(i)

    return """
-- Function to get column cardinality (excluding HLL columns)
CREATE OR REPLACE FUNCTION get_column_cardinality(table_name text, column_name text)
RETURNS TABLE(total_rows bigint, distinct_values bigint, cardinality_ratio numeric) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        WITH stats AS (
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT %I) AS distinct_values
            FROM %I
        )
        SELECT total_rows,
               distinct_values,
               (distinct_values::numeric / total_rows::numeric) AS cardinality_ratio
        FROM stats
    ', column_name, table_name);
END;
$$ LANGUAGE plpgsql;

-- Analyze cardinality for regular columns (excluding HLL and arrays)
SELECT 'Regular Columns Cardinality' as analysis_type;
SELECT column_name, *
FROM (
    SELECT column_name::text,
           (get_column_cardinality('all_facts', column_name::text)).*
    FROM information_schema.columns
    WHERE table_name = 'all_facts'
    AND data_type NOT IN ('text[]', 'hll')
    AND column_name NOT LIKE 'hll_col%'
) subquery
ORDER BY cardinality_ratio ASC
LIMIT 10;

-- Analyze array column statistics
SELECT 'Array Columns Statistics' as analysis_type;
SELECT *
FROM (
{0}
) array_stats
ORDER BY column_name;

-- Array element frequency analysis
SELECT 'Array Element Frequency Analysis' as analysis_type;
WITH RECURSIVE array_elements AS (
    SELECT
        'text_array_col1' as column_name,
        unnest(text_array_col1) as element
    FROM all_facts
    UNION ALL
    SELECT
        'text_array_col2',
        unnest(text_array_col2)
    FROM all_facts
    UNION ALL
    SELECT
        'text_array_col3',
        unnest(text_array_col3)
    FROM all_facts
),
element_stats AS (
    SELECT
        column_name,
        element,
        COUNT(*) as frequency
    FROM array_elements
    GROUP BY column_name, element
)
SELECT
    column_name,
    COUNT(DISTINCT element) as unique_elements,
    MIN(frequency) as min_frequency,
    MAX(frequency) as max_frequency,
    AVG(frequency)::numeric(10,2) as avg_frequency
FROM element_stats
GROUP BY column_name
ORDER BY column_name;

-- HLL column analysis with detailed metrics
SELECT 'HLL Columns Detailed Analysis' as analysis_type;
WITH hll_stats AS (
    SELECT
        hll_column,
        hll_cardinality(hll_value) as cardinality,
        pg_column_size(hll_value) as col_size
    FROM (
        SELECT
            'hll_col1' as hll_column, hll_col1 as hll_value FROM all_facts
        UNION ALL
        SELECT 'hll_col2', hll_col2 FROM all_facts
        UNION ALL
        SELECT 'hll_col3', hll_col3 FROM all_facts
        UNION ALL
        SELECT 'hll_col4', hll_col4 FROM all_facts
        UNION ALL
        SELECT 'hll_col5', hll_col5 FROM all_facts
        UNION ALL
        SELECT 'hll_col6', hll_col6 FROM all_facts
        UNION ALL
        SELECT 'hll_col7', hll_col7 FROM all_facts
        UNION ALL
        SELECT 'hll_col8', hll_col8 FROM all_facts
        UNION ALL
        SELECT 'hll_col9', hll_col9 FROM all_facts
        UNION ALL
        SELECT 'hll_col10', hll_col10 FROM all_facts
        UNION ALL
        SELECT 'hll_col11', hll_col11 FROM all_facts
        UNION ALL
        SELECT 'hll_col12', hll_col12 FROM all_facts
    ) t
)
SELECT
    hll_column,
    COUNT(*) as row_count,
    MIN(cardinality) as min_cardinality,
    MAX(cardinality) as max_cardinality,
    AVG(cardinality)::numeric(10,2) as avg_cardinality,
    MIN(col_size) as min_size_bytes,
    MAX(col_size) as max_size_bytes,
    AVG(col_size)::numeric(10,2) as avg_size_bytes,
    SUM(cardinality) as total_unique_elements
FROM hll_stats
GROUP BY hll_column
ORDER BY hll_column;

-- HLL cardinality distribution
SELECT 'HLL Cardinality Distribution' as analysis_type;
WITH hll_ranges AS (
    SELECT
        hll_column,
        CASE
            WHEN cardinality = 0 THEN 'Empty'
            WHEN cardinality < 50 THEN 'Very Low (1-49)'
            WHEN cardinality < 100 THEN 'Low (50-99)'
            WHEN cardinality < 500 THEN 'Medium (100-499)'
            ELSE 'High (500+)'
        END as cardinality_range,
        COUNT(*) as count
    FROM (
        SELECT
            'hll_col1' as hll_column, hll_cardinality(hll_col1) as cardinality FROM all_facts
        UNION ALL
        SELECT 'hll_col2', hll_cardinality(hll_col2) FROM all_facts
        UNION ALL
        SELECT 'hll_col3', hll_cardinality(hll_col3) FROM all_facts
        UNION ALL
        SELECT 'hll_col4', hll_cardinality(hll_col4) FROM all_facts
        UNION ALL
        SELECT 'hll_col5', hll_cardinality(hll_col5) FROM all_facts
        UNION ALL
        SELECT 'hll_col6', hll_cardinality(hll_col6) FROM all_facts
        UNION ALL
        SELECT 'hll_col7', hll_cardinality(hll_col7) FROM all_facts
        UNION ALL
        SELECT 'hll_col8', hll_cardinality(hll_col8) FROM all_facts
        UNION ALL
        SELECT 'hll_col9', hll_cardinality(hll_col9) FROM all_facts
        UNION ALL
        SELECT 'hll_col10', hll_cardinality(hll_col10) FROM all_facts
        UNION ALL
        SELECT 'hll_col11', hll_cardinality(hll_col11) FROM all_facts
        UNION ALL
        SELECT 'hll_col12', hll_cardinality(hll_col12) FROM all_facts
    ) t
    GROUP BY hll_column,
        CASE
            WHEN cardinality = 0 THEN 'Empty'
            WHEN cardinality < 50 THEN 'Very Low (1-49)'
            WHEN cardinality < 100 THEN 'Low (50-99)'
            WHEN cardinality < 500 THEN 'Medium (100-499)'
            ELSE 'High (500+)'
        END
)
SELECT
    hll_column,
    cardinality_range,
    count,
    (count * 100.0 / SUM(count) OVER (PARTITION BY hll_column))::numeric(5,2) as percentage
FROM hll_ranges
ORDER BY hll_column, cardinality_range;

-- Additional size analysis
SELECT 'Column Size Analysis' as analysis_type;
SELECT
    column_name,
    pg_column_size(column_name::text::regclass) as column_definition_size,
    pg_total_relation_size('all_facts'::regclass) as total_table_size
FROM information_schema.columns
WHERE table_name = 'all_facts'
ORDER BY column_name;

-- Drop helper functions
DROP FUNCTION IF EXISTS get_column_cardinality(text, text);
""".format(array_analysis)

def create_partition_count_query():
    return """
WITH partition_counts AS (
    SELECT 'all_facts_1_prt_y2024m01'::TEXT as partition_name, COUNT(*) as row_count FROM ONLY all_facts_1_prt_y2024m01
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m02'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m02
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m03'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m03
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m04'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m04
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m05'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m05
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m06'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m06
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m07'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m07
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m08'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m08
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m09'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m09
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m10'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m10
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m11'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m11
    UNION ALL
    SELECT 'all_facts_1_prt_y2024m12'::TEXT, COUNT(*) FROM ONLY all_facts_1_prt_y2024m12
),
total_count AS (
    SELECT 'TOTAL'::TEXT as partition_name, SUM(row_count) as row_count
    FROM partition_counts
)
SELECT partition_name, row_count
FROM (
    SELECT partition_name, row_count, 1 as sort_order
    FROM partition_counts
    UNION ALL
    SELECT partition_name, row_count, 2 as sort_order
    FROM total_count
) subquery
ORDER BY sort_order, partition_name;
"""

def generate_test_data_chunk(chunk_size, chunk_number=None):
    data = []
    for _ in range(chunk_size):
        row = []

        # Generate integer columns (49)
        for _ in range(49):
            row.append(str(random.randint(1, 1000000)))

        # Generate numeric columns (22)
        for _ in range(22):
            row.append(str(round(random.uniform(0, 1000), 2)))

        # Generate TEXT[] columns (12)
        for _ in range(12):
            row.append(generate_random_text_array())

        # Generate HLL columns (12)
        for _ in range(12):
            row.append(generate_random_hll())

        # Generate text columns (9)
        for _ in range(9):
            row.append("text_{0}".format(random.randint(1, 1000)))

        # Generate timestamp
        row.append("2024-{0:02d}-{1:02d} {2:02d}:{3:02d}:{4:02d}".format(
            random.randint(1, 12), random.randint(1, 28),
            random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
        ))

        data.append("|".join(row))
    return "\n".join(data)

def main():
    parser = argparse.ArgumentParser(description="Generate SQL files for Greenplum all_facts table creation, data insertion, and partition counting")
    parser.add_argument("--output-dir", required=True, help="Output directory for SQL files")
    parser.add_argument("--test-data", type=int, default=1000, help="Number of test data rows to generate")
    parser.add_argument("--cores", type=int, default=multiprocessing.cpu_count(), help="Number of CPU cores to use")

    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Create table SQL
    with open(os.path.join(args.output_dir, 'create_table.sql'), 'w') as f:
        f.write(create_table_sql())

    # Create indexes SQL
    with open(os.path.join(args.output_dir, 'create_tailored_indexes_compatible.sql'), 'w') as f:
        f.write(create_indexes_sql())

    # Create cardinality analysis SQL
    with open(os.path.join(args.output_dir, 'analyze_cardinality.sql'), 'w') as f:
        f.write(create_cardinality_analysis_sql())

    # Generate test data
    chunk_size = args.test_data // args.cores
    remaining_rows = args.test_data % args.cores

    print("Generating test data...")
    pool = multiprocessing.Pool(processes=args.cores)

    if TQDM_AVAILABLE:
        results = list(tqdm(pool.imap(partial(generate_test_data_chunk, chunk_size), range(args.cores)), total=args.cores))
    else:
        results = []
        for i, result in enumerate(pool.imap(partial(generate_test_data_chunk, chunk_size), range(args.cores)), 1):
            results.append(result)
            sys.stdout.write('\rProgress: {0:.1f}%'.format(100 * i / args.cores))
            sys.stdout.flush()
        print()

    if remaining_rows > 0:
        results.append(generate_test_data_chunk(remaining_rows))

    print("Writing test data to file...")
    with open(os.path.join(args.output_dir, 'test_data.tsv'), 'w') as f:
        for result in results:
            f.write(result)
            f.write('\n')

    # Create COPY command SQL
    with open(os.path.join(args.output_dir, 'copy_data.sql'), 'w') as f:
        f.write(create_copy_command(args.output_dir))

    # Partition count query
    with open(os.path.join(args.output_dir, 'partition_count.sql'), 'w') as f:
        f.write(create_partition_count_query())

    print("SQL files have been generated in the directory: {0}".format(args.output_dir))
    print("To create the table and load data:")
    print("1. Run: psql -f {0}/create_table.sql -d your_database_name".format(args.output_dir))
    print("2. Run: psql -f {0}/copy_data.sql -d your_database_name".format(args.output_dir))
    print("3. Run: psql -f {0}/create_tailored_indexes_compatible.sql -d your_database_name".format(args.output_dir))
    print("4. To analyze cardinality: psql -f {0}/analyze_cardinality.sql -d your_database_name".format(args.output_dir))
    print("5. To check partition counts: psql -f {0}/partition_count.sql -d your_database_name".format(args.output_dir))

if __name__ == "__main__":
    main()
