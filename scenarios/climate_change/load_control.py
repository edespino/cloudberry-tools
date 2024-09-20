#!/usr/bin/env python3
import psycopg2
import os
import argparse

def get_csv_files(directory):
    """
    Get a list of all CSV files in the specified directory.
    """
    csv_files = []
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            csv_files.append(os.path.join(directory, filename))
    return csv_files

def setup_control_table(conn):
    with conn.cursor() as cur:
        # Check if the table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'ghcn_load_control_test'
            );
        """)
        table_exists = cur.fetchone()[0]

        if table_exists:
            # If the table exists, truncate it
            cur.execute("TRUNCATE TABLE ghcn_load_control_test;")
            print("Truncated existing ghcn_load_control_test table.")
        else:
            # If the table doesn't exist, create it
            cur.execute("""
                CREATE TABLE ghcn_load_control_test (
                    file_name VARCHAR(255) PRIMARY KEY,
                    status VARCHAR(20),
                    csv_record_count INTEGER,
                    inserted_row_count INTEGER,
                    error_condition TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            print("Created new ghcn_load_control_test table.")

        conn.commit()

def insert_csv_files(conn, csv_files):
    with conn.cursor() as cur:
        for file_name in csv_files:
            cur.execute("""
                INSERT INTO ghcn_load_control_test (file_name, status)
                VALUES (%s, 'PENDING');
            """, (file_name,))
        conn.commit()

def reset_file_statuses(conn):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ghcn_load_control_test
            SET status = 'PENDING',
                csv_record_count = NULL,
                inserted_row_count = NULL,
                error_condition = NULL,
                last_updated = CURRENT_TIMESTAMP
        """)
        rows_updated = cur.rowcount
        conn.commit()
    print(f"Reset {rows_updated} files to 'PENDING' state.")

def main():
    parser = argparse.ArgumentParser(description='Setup or reset GHCN control table.')
    parser.add_argument('-r', '--reset', action='store_true', help='Reset all files to PENDING state')
    args = parser.parse_args()

    # Connect to the database
    conn = psycopg2.connect("dbname=climate_analysis user=gpadmin host=mdw")

    try:
        if args.reset:
            # Reset all files to PENDING state
            reset_file_statuses(conn)
        else:
            # Setup (truncate or create) the control table
            setup_control_table(conn)

            # Get the list of CSV files from the directory
            csv_directory = '/home/gpadmin/data/ghcnd_all/processed_ghcn'
            csv_files = get_csv_files(csv_directory)

            # Insert CSV files into the control table
            insert_csv_files(conn, csv_files)
            print(f"Inserted {len(csv_files)} CSV files into ghcn_load_control_test.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
