#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import argparse
import random
import sys
import os
import multiprocessing
import json
from functools import partial

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

def create_table_sql():
    columns = []

    # Add 7 text columns
    for i in range(1, 8):
        columns.append("text_col{0} TEXT".format(i))

    # Add 6 integer columns
    for i in range(1, 7):
        columns.append("int_col{0} INTEGER".format(i))

    # Add 3 bigint columns
    for i in range(1, 4):
        columns.append("bigint_col{0} BIGINT".format(i))

    # Add 2 jsonb columns
    columns.append("config_json JSONB")
    columns.append("metadata_json JSONB")

    # Add 2 timestamp columns
    columns.append("created_at TIMESTAMP WITHOUT TIME ZONE")
    columns.append("updated_at TIMESTAMP WITHOUT TIME ZONE")

    # Add 1 boolean column
    columns.append("is_active BOOLEAN")

    column_definitions = ",\n    ".join(columns)

    # Create table SQL without indexes (they'll be in a separate file)
    return """
DROP TABLE IF EXISTS beeswax CASCADE;

CREATE TABLE beeswax (
    id SERIAL,
    {0},
    PRIMARY KEY (id, created_at)
) DISTRIBUTED BY (id)
PARTITION BY RANGE (created_at)
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
""".format(column_definitions)

def create_indexes_sql():
    return """
-- Drop existing indexes if they exist
DROP INDEX IF EXISTS idx_beeswax_created_at;
DROP INDEX IF EXISTS idx_beeswax_updated_at;
DROP INDEX IF EXISTS idx_beeswax_is_active;
DROP INDEX IF EXISTS idx_beeswax_config_json;
DROP INDEX IF EXISTS idx_beeswax_metadata_json;
DROP INDEX IF EXISTS idx_beeswax_active_created_at;
DROP INDEX IF EXISTS idx_beeswax_text_cols;
DROP INDEX IF EXISTS idx_beeswax_int_cols;
DROP INDEX IF EXISTS idx_beeswax_json_paths;
DROP INDEX IF EXISTS idx_beeswax_composite_timestamp;

-- Basic indexes for timestamp columns
CREATE INDEX idx_beeswax_created_at ON beeswax USING btree (created_at);
CREATE INDEX idx_beeswax_updated_at ON beeswax USING btree (updated_at);

-- Index for boolean column
CREATE INDEX idx_beeswax_is_active ON beeswax USING btree (is_active);

-- GIN indexes for JSONB columns with specific operators
CREATE INDEX idx_beeswax_config_json ON beeswax USING gin (config_json jsonb_path_ops);
CREATE INDEX idx_beeswax_metadata_json ON beeswax USING gin (metadata_json jsonb_path_ops);

-- Partial indexes for active records
CREATE INDEX idx_beeswax_active_created_at ON beeswax (created_at)
WHERE is_active = true;

-- Multi-column index for text columns that are frequently searched together
CREATE INDEX idx_beeswax_text_cols ON beeswax (text_col1, text_col2, text_col3);

-- Multi-column index for integer columns commonly used in range queries
CREATE INDEX idx_beeswax_int_cols ON beeswax (int_col1, int_col2, int_col3);

-- Specialized JSON path indexes for commonly accessed JSON fields
CREATE INDEX idx_beeswax_json_paths ON beeswax ((config_json->'version'), (metadata_json->'environment'));

-- Composite index for timestamp range queries with is_active
CREATE INDEX idx_beeswax_composite_timestamp ON beeswax (created_at, updated_at)
WHERE is_active = true;

-- Add statistics for better query planning
ANALYZE beeswax;
"""

def generate_sample_json():
    config = {
        "version": random.randint(1, 5),
        "settings": {
            "timeout": random.randint(1000, 5000),
            "retry_count": random.randint(1, 5),
            "enabled_features": random.sample(["feature1", "feature2", "feature3", "feature4"], random.randint(1, 3))
        }
    }

    metadata = {
        "environment": random.choice(["prod", "stage", "dev"]),
        "region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
        "tags": random.sample(["tag1", "tag2", "tag3", "tag4", "tag5"], random.randint(1, 4))
    }

    return json.dumps(config), json.dumps(metadata)

def generate_test_data_chunk(chunk_size, chunk_number=None):
    data = []
    for _ in range(chunk_size):
        row = []
        # Generate text columns
        for i in range(7):
            row.append("text_{0}_{1}".format(i, random.randint(1, 1000)))

        # Generate integer columns
        for _ in range(6):
            row.append(str(random.randint(1, 1000000)))

        # Generate bigint columns
        for _ in range(3):
            row.append(str(random.randint(1, 1000000000)))

        # Generate JSONB columns
        config_json, metadata_json = generate_sample_json()
        row.append(config_json)
        row.append(metadata_json)

        # Generate timestamp columns
        for _ in range(2):
            row.append("2024-{0:02d}-{1:02d} {2:02d}:{3:02d}:{4:02d}".format(
                random.randint(1, 12), random.randint(1, 28),
                random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
            ))

        # Generate boolean column
        row.append("true" if random.choice([True, False]) else "false")

        data.append("|".join(row))
    return "\n".join(data)

def create_copy_command(output_dir):
    columns = (
        ["text_col{0}".format(i) for i in range(1, 8)] +
        ["int_col{0}".format(i) for i in range(1, 7)] +
        ["bigint_col{0}".format(i) for i in range(1, 4)] +
        ["config_json", "metadata_json"] +
        ["created_at", "updated_at"] +
        ["is_active"]
    )

    column_list = ", ".join(columns)

    return "COPY beeswax({0}) FROM '{1}' WITH DELIMITER AS '|';\n".format(
        column_list,
        os.path.abspath(os.path.join(output_dir, 'test_data.tsv'))
    )

def create_partition_count_query():
    return """
WITH partition_counts AS (
    SELECT 'beeswax_1_prt_y2024m01'::TEXT as partition_name, COUNT(*) as row_count FROM ONLY beeswax_1_prt_y2024m01
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m02'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m02
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m03'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m03
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m04'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m04
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m05'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m05
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m06'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m06
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m07'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m07
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m08'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m08
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m09'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m09
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m10'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m10
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m11'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m11
    UNION ALL
    SELECT 'beeswax_1_prt_y2024m12'::TEXT, COUNT(*) FROM ONLY beeswax_1_prt_y2024m12
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

def create_cardinality_analysis_sql():
    return """
-- Function to get column cardinality
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

-- Function to analyze JSONB field cardinality
CREATE OR REPLACE FUNCTION get_jsonb_field_cardinality(
    table_name text,
    column_name text,
    json_path text
)
RETURNS TABLE(total_rows bigint, distinct_values bigint, cardinality_ratio numeric) AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        WITH stats AS (
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT %s->>%L) AS distinct_values
            FROM %I
        )
        SELECT total_rows,
               distinct_values,
               (distinct_values::numeric / total_rows::numeric) AS cardinality_ratio
        FROM stats
    ', column_name, json_path, table_name);
END;
$$ LANGUAGE plpgsql;

-- Analyze cardinality for specific columns
SELECT 'is_active' AS column_name, * FROM get_column_cardinality('beeswax', 'is_active');
SELECT 'text_col1' AS column_name, * FROM get_column_cardinality('beeswax', 'text_col1');
SELECT 'int_col1' AS column_name, * FROM get_column_cardinality('beeswax', 'int_col1');
SELECT 'bigint_col1' AS column_name, * FROM get_column_cardinality('beeswax', 'bigint_col1');

-- Analyze cardinality for specific JSONB fields
SELECT 'config_json.version' AS json_field, *
FROM get_jsonb_field_cardinality('beeswax', 'config_json', 'version');
SELECT 'metadata_json.environment' AS json_field, *
FROM get_jsonb_field_cardinality('beeswax', 'metadata_json', 'environment');

-- Analyze cardinality for all non-JSONB columns
SELECT column_name, *
FROM (
    SELECT column_name::text,
           (get_column_cardinality('beeswax', column_name::text)).*
    FROM information_schema.columns
    WHERE table_name = 'beeswax'
    AND data_type NOT IN ('jsonb')  -- Exclude JSONB columns as they need special handling
) subquery
ORDER BY cardinality_ratio ASC
LIMIT 10;  -- Show top 10 lowest cardinality columns

-- Analyze cardinality for common JSONB fields
WITH json_fields AS (
    SELECT 'config_json' AS column_name, 'version' AS field_path
    UNION ALL SELECT 'config_json', 'settings.timeout'
    UNION ALL SELECT 'config_json', 'settings.retry_count'
    UNION ALL SELECT 'metadata_json', 'environment'
    UNION ALL SELECT 'metadata_json', 'region'
)
SELECT
    column_name || '.' || field_path AS json_field,
    (get_jsonb_field_cardinality('beeswax', column_name, field_path)).*
FROM json_fields
ORDER BY cardinality_ratio ASC;

-- Get distribution of values for low-cardinality columns
SELECT 'is_active' AS column_name, is_active AS value, COUNT(*) AS frequency,
       (COUNT(*)::float / SUM(COUNT(*)) OVER ()) AS percentage
FROM beeswax
GROUP BY is_active
ORDER BY frequency DESC;

SELECT 'metadata_json.environment' AS column_name,
       metadata_json->>'environment' AS value,
       COUNT(*) AS frequency,
       (COUNT(*)::float / SUM(COUNT(*)) OVER ()) AS percentage
FROM beeswax
GROUP BY metadata_json->>'environment'
ORDER BY frequency DESC;

-- Drop the helper functions
DROP FUNCTION IF EXISTS get_column_cardinality(text, text);
DROP FUNCTION IF EXISTS get_jsonb_field_cardinality(text, text, text);
"""

def main():
    parser = argparse.ArgumentParser(description="Generate SQL files for Greenplum beeswax table creation, data insertion, and partition counting")
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
    print("4. To check partition counts, run: psql -f {0}/partition_count.sql -d your_database_name".format(args.output_dir))
    print("5. To check cardinality counts, run: psql -f {0}/analyze_cardinality.sql -d your_database_name".format(args.output_dir))

if __name__ == "__main__":
    main()
