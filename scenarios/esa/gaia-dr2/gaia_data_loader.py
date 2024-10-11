#!/usr/bin/env python3

import os
import argparse
import psycopg2
from psycopg2 import sql
from tqdm import tqdm

def load_gaia_data(directory, db_params):
    if not os.path.isdir(directory):
        raise ValueError(f"The directory {directory} does not exist.")

    files = [f for f in os.listdir(directory) if f.endswith('.csv.gz')]
    if not files:
        raise ValueError(f"No .csv.gz files found in {directory}")

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    total_rows = 0

    for file in tqdm(files, desc="Processing files"):
        file_path = os.path.join(directory, file)

        copy_sql = sql.SQL("""
            COPY gaia_stars_full FROM PROGRAM {program}
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
        """).format(
            program=sql.Literal(f"zcat {file_path}")
        )

        try:
            cur.execute(copy_sql)
            rows_affected = cur.rowcount
            total_rows += rows_affected
            conn.commit()
            tqdm.write(f"Loaded {rows_affected} rows from {file}")
        except Exception as e:
            conn.rollback()
            tqdm.write(f"Error loading {file}: {str(e)}")

    cur.close()
    conn.close()

    print(f"\nTotal rows added: {total_rows}")
    return total_rows

def get_segment_distribution(db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Get the distribution policy (distribution key)
    cur.execute("""
        SELECT attrnums
        FROM gp_distribution_policy
        WHERE localoid = 'gaia_stars_full'::regclass;
    """)
    dist_key = cur.fetchone()[0]

    # If dist_key is empty, the table is randomly distributed
    if not dist_key:
        dist_clause = "random()"
    else:
        # Get the column name for the distribution key
        cur.execute(f"""
            SELECT attname
            FROM pg_attribute
            WHERE attrelid = 'gaia_stars_full'::regclass AND attnum = {dist_key[0]};
        """)
        dist_column = cur.fetchone()[0]
        dist_clause = dist_column

    # Query to get row count per segment
    cur.execute(f"""
        SELECT gp_segment_id, count(*) FROM gaia_stars_full GROUP BY gp_segment_id;
    """)

    segment_counts = cur.fetchall()

    cur.close()
    conn.close()

    return segment_counts, dist_clause

def main():
    parser = argparse.ArgumentParser(description="Load Gaia DR2 data into PostgreSQL")
    parser.add_argument("directory", help="Directory containing Gaia DR2 .csv.gz files")
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", default="5432", help="Database port")
    parser.add_argument("--dbname", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--password", required=True, help="Database password")
    args = parser.parse_args()

    db_params = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password
    }

    try:
        total_rows = load_gaia_data(args.directory, db_params)
        segment_counts, dist_clause = get_segment_distribution(db_params)

        print("\nData distribution across segments:")
        print(f"Distribution key: {dist_clause}")
        for segment_id, count in segment_counts:
            print(f"Segment {segment_id}: {count} rows ({count/total_rows*100:.2f}%)")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
