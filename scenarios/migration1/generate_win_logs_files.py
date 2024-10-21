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

def create_table_sql():
    columns = []

    # Add 130 text columns
    for i in range(1, 131):
        columns.append(u"text_col{0} TEXT".format(i))

    # Add 59 integer columns
    for i in range(1, 60):
        columns.append(u"int_col{0} INTEGER".format(i))

    # Add 20 bigint columns
    for i in range(1, 21):
        columns.append(u"bigint_col{0} BIGINT".format(i))

    # Add 6 timestamp without time zone columns
    for i in range(1, 7):
        columns.append(u"timestamp_col{0} TIMESTAMP WITHOUT TIME ZONE".format(i))

    # Add 5 numeric columns
    for i in range(1, 6):
        columns.append(u"numeric_col{0} NUMERIC".format(i))

    # Add 2 boolean columns
    columns.append(u"boolean_col1 BOOLEAN")
    columns.append(u"boolean_col2 BOOLEAN")

    # Add 2 inet columns
    columns.append(u"inet_col1 INET")
    columns.append(u"inet_col2 INET")

    # Add 2 double precision columns
    columns.append(u"double_col1 DOUBLE PRECISION")
    columns.append(u"double_col2 DOUBLE PRECISION")

    # Add 1 character varying column
    columns.append(u"varchar_col CHARACTER VARYING")

    column_definitions = u",\n    ".join(columns)

    return u"""
DROP TABLE IF EXISTS win_logs CASCADE;

CREATE TABLE win_logs (
    id SERIAL,
    {0},
    PRIMARY KEY (id, timestamp_col1)
) DISTRIBUTED BY (id)
PARTITION BY RANGE (timestamp_col1)
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

def generate_test_data_chunk(chunk_size, chunk_number=None):
    data = []
    for _ in range(chunk_size):
        row = []
        for i in range(130):
            row.append(u"text_{0}_{1}".format(i, random.randint(1, 1000)))
        for _ in range(59):
            row.append(str(random.randint(1, 1000000)))
        for _ in range(20):
            row.append(str(random.randint(1, 1000000000)))
        for _ in range(6):
            row.append(u"2024-{0:02d}-{1:02d} {2:02d}:{3:02d}:{4:02d}".format(
                random.randint(1, 12), random.randint(1, 28),
                random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
            ))
        for _ in range(5):
            row.append(str(round(random.uniform(0, 1000), 2)))
        for _ in range(2):
            row.append(u"true" if random.choice([True, False]) else u"false")
        for _ in range(2):
            row.append(u"{0}.{1}.{2}.{3}".format(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
        for _ in range(2):
            row.append(str(random.uniform(0, 1000)))
        row.append(u"varchar_{0}".format(random.randint(1, 1000)))

        data.append(u"|".join(row))
    return u"\n".join(data)

def create_copy_command(output_dir):
    columns = (
        [u"text_col{0}".format(i) for i in range(1, 131)] +
        [u"int_col{0}".format(i) for i in range(1, 60)] +
        [u"bigint_col{0}".format(i) for i in range(1, 21)] +
        [u"timestamp_col{0}".format(i) for i in range(1, 7)] +
        [u"numeric_col{0}".format(i) for i in range(1, 6)] +
        [u"boolean_col1", u"boolean_col2"] +
        [u"inet_col1", u"inet_col2"] +
        [u"double_col1", u"double_col2"] +
        [u"varchar_col"]
    )

    column_list = u", ".join(columns)

    return u"COPY win_logs({0}) FROM '{1}' WITH DELIMITER AS '|';\n".format(
        column_list,
        os.path.abspath(os.path.join(output_dir, 'test_data.tsv'))
    )

def create_partition_count_query():
    return u"""
WITH partition_counts AS (
    SELECT 'win_logs_1_prt_y2024m01'::TEXT as partition_name, COUNT(*) as row_count FROM ONLY win_logs_1_prt_y2024m01
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m02'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m02
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m03'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m03
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m04'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m04
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m05'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m05
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m06'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m06
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m07'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m07
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m08'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m08
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m09'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m09
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m10'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m10
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m11'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m11
    UNION ALL
    SELECT 'win_logs_1_prt_y2024m12'::TEXT, COUNT(*) FROM ONLY win_logs_1_prt_y2024m12
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

def main():
    parser = argparse.ArgumentParser(description=u"Generate SQL files for Greenplum win_logs table creation, data insertion, and partition counting")
    parser.add_argument(u"--output-dir", required=True, help=u"Output directory for SQL files")
    parser.add_argument(u"--test-data", type=int, default=1000, help=u"Number of test data rows to generate")
    parser.add_argument(u"--cores", type=int, default=multiprocessing.cpu_count(), help=u"Number of CPU cores to use")

    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Create table SQL
    with open(os.path.join(args.output_dir, 'create_table.sql'), 'w') as f:
        f.write(create_table_sql())

    # Generate test data for COPY using multiprocessing
    chunk_size = args.test_data // args.cores
    remaining_rows = args.test_data % args.cores

    print(u"Generating test data...")
    pool = multiprocessing.Pool(processes=args.cores)

    if TQDM_AVAILABLE:
        results = list(tqdm(pool.imap(partial(generate_test_data_chunk, chunk_size), range(args.cores)), total=args.cores))
    else:
        results = []
        for i, result in enumerate(pool.imap(partial(generate_test_data_chunk, chunk_size), range(args.cores)), 1):
            results.append(result)
            sys.stdout.write(u'\rProgress: {0:.1f}%'.format(100 * i / args.cores))
            sys.stdout.flush()
        print()  # New line after progress indicator

    if remaining_rows > 0:
        results.append(generate_test_data_chunk(remaining_rows))

    print(u"Writing test data to file...")
    with open(os.path.join(args.output_dir, 'test_data.tsv'), 'w') as f:
        for result in results:
            f.write(result)
            f.write(u'\n')

    # Create COPY command SQL
    with open(os.path.join(args.output_dir, 'copy_data.sql'), 'w') as f:
        f.write(create_copy_command(args.output_dir))

    # Partition count query
    with open(os.path.join(args.output_dir, 'partition_count.sql'), 'w') as f:
        f.write(create_partition_count_query())

    print(u"SQL files have been generated in the directory: {0}".format(args.output_dir))
    print(u"To create the table and load data:")
    print(u"1. Run: psql -f {0}/create_table.sql -d your_database_name".format(args.output_dir))
    print(u"2. Run: psql -f {0}/copy_data.sql -d your_database_name".format(args.output_dir))
    print(u"3. To check partition counts, run: psql -f {0}/partition_count.sql -d your_database_name".format(args.output_dir))

if __name__ == u"__main__":
    main()
