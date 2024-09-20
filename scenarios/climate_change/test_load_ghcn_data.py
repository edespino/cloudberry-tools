#!/usr/bin/env python3
import psycopg2
import os
import csv
from io import StringIO
import argparse
import subprocess
import time
from datetime import datetime, timedelta
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

def execute_query(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()

def get_next_files(conn, limit):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT file_name
            FROM ghcn_load_control_test
            WHERE status = 'PENDING'
            ORDER BY file_name
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        """, (limit,))
        return [row[0] for row in cur.fetchall()]

def update_file_status(conn, file_name, status, csv_record_count=None, inserted_row_count=None, error_condition=None):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ghcn_load_control_test
            SET status = %s, csv_record_count = %s, inserted_row_count = %s, error_condition = %s, last_updated = CURRENT_TIMESTAMP
            WHERE file_name = %s
        """, (status, csv_record_count, inserted_row_count, error_condition, file_name))
        conn.commit()

def count_csv_records(file_path):
    with open(file_path, 'r') as file:
        return sum(1 for _ in csv.reader(file)) - 1  # Subtract 1 for header

def get_first_n_rows(file_name, n):
    buffer = StringIO()
    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Read the header
        buffer.write(','.join(header) + '\n')  # Write the header to buffer
        if n == -1:
            for row in reader:
                buffer.write(','.join(row) + '\n')
        else:
            for i, row in enumerate(reader):
                if i >= n:
                    break
                buffer.write(','.join(row) + '\n')
    buffer.seek(0)  # Reset the buffer position to the start
    return buffer

def start_gpfdist(data_dir, port, verbose):
    log_file = 'gpfdist.log' if not verbose else None
    with open(log_file, 'a') if log_file else subprocess.DEVNULL as log:
        process = subprocess.Popen(
            ['gpfdist', '-d', data_dir, '-p', str(port)],
            stdout=log,
            stderr=subprocess.STDOUT
        )
    time.sleep(2)  # Give gpfdist some time to start
    return process

def stop_gpfdist(processes):
    for process in processes:
        process.terminate()
        process.wait()

def process_file(file_name, conn, gpfdist_ports):
    try:
        print(f"Processing file: {file_name}")
        csv_record_count = count_csv_records(file_name)
        print(f"Number of records in the file (excluding header): {csv_record_count}")

        # Create a temporary external table for this file
        table_name = f"temp_ext_{os.path.splitext(os.path.basename(file_name))[0]}"
        gpfdist_locations = ", ".join([f"'gpfdist://mdw:{port}/{os.path.basename(file_name)}'" for port in gpfdist_ports])

        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE EXTERNAL TABLE {table_name} (
                    station_id VARCHAR(20),
                    measurement_date DATE,
                    element VARCHAR(10),
                    value NUMERIC
                )
                LOCATION ({gpfdist_locations})
                FORMAT 'CSV' (HEADER);
            """)

        # Insert data using the external table
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO ghcn_daily_test
                SELECT * FROM {table_name};
            """)
        conn.commit()

        # Get the number of inserted rows
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM ghcn_daily_test WHERE station_id = %s", (os.path.splitext(os.path.basename(file_name))[0],))
            inserted_row_count = cur.fetchone()[0]

        error_condition = None
        if inserted_row_count != csv_record_count:
            error_condition = f"Rows inserted: {inserted_row_count} (expected {csv_record_count})"

        update_file_status(conn, file_name, 'COMPLETED', csv_record_count, inserted_row_count, error_condition)
        print(f"Processed {inserted_row_count} rows for file: {file_name}")

        # Drop the temporary external table
        with conn.cursor() as cur:
            cur.execute(f"DROP EXTERNAL TABLE IF EXISTS {table_name};")
        conn.commit()

    except Exception as e:
        print(f"Error processing file {file_name}: {str(e)}")
        update_file_status(conn, file_name, 'FAILED', error_condition=str(e))

def format_time(seconds):
    return str(timedelta(seconds=int(seconds)))

def main():
    parser = argparse.ArgumentParser(description='Process GHCN data files.')
    parser.add_argument('-n', type=int, default=1, help='Number of files to process')
    parser.add_argument('-g', type=int, default=1, help='Number of gpfdist processes to start')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output for gpfdist')
    args = parser.parse_args()

    conn = psycopg2.connect("dbname=climate_analysis user=gpadmin host=mdw")
    data_dir = '/home/gpadmin/data/ghcnd_all/processed_ghcn'

    gpfdist_processes = []
    gpfdist_ports = []
    try:
        # Start gpfdist processes
        for i in range(args.g):
            port = 8081 + i
            gpfdist_processes.append(start_gpfdist(data_dir, port, args.verbose))
            gpfdist_ports.append(port)

        start_time = time.time()
        files_processed = 0

        with ThreadPoolExecutor(max_workers=args.g) as executor:
            while files_processed < args.n:
                files = get_next_files(conn, min(args.g, args.n - files_processed))
                if not files:
                    print("No more files to process.")
                    break

                futures = []
                for file_name in files:
                    futures.append(executor.submit(process_file, file_name, conn, gpfdist_ports))

                for future in as_completed(futures):
                    future.result()  # This will raise any exceptions that occurred
                    files_processed += 1

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"All requested files processed. Total files: {files_processed}")
        print(f"Elapsed time: {format_time(elapsed_time)}")

    finally:
        stop_gpfdist(gpfdist_processes)
        conn.close()

if __name__ == "__main__":
    main()
