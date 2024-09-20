#!/usr/bin/env python3
import psycopg2
import os
import csv
from io import StringIO
import argparse
import subprocess
import time
from datetime import datetime, timedelta
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

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
    if log_file:
        log = open(log_file, 'a')
    else:
        log = subprocess.DEVNULL

    try:
        process = subprocess.Popen(
            ['gpfdist', '-d', data_dir, '-p', str(port)],
            stdout=log,
            stderr=subprocess.STDOUT
        )
        time.sleep(2)  # Give gpfdist some time to start
        return process
    except Exception as e:
        print(f"Error starting gpfdist on port {port}: {str(e)}")
        if log_file:
            log.close()
        return None

def stop_gpfdist(processes):
    for process in processes:
        try:
            process.terminate()
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        except Exception as e:
            print(f"Error stopping gpfdist process: {str(e)}")

def process_file(file_name, conn, gpfdist_ports, verbose, display_definition, debug):
    try:
        if debug:
            print(f"Started processing file: {file_name}")

        if not gpfdist_ports:
            raise ValueError("No gpfdist processes available")

        csv_record_count = count_csv_records(file_name)
        if verbose:
            print(f"Number of records in the file (excluding header): {csv_record_count}")

        table_name = f"temp_ext_{os.path.splitext(os.path.basename(file_name))[0]}"
        gpfdist_locations = ", ".join([f"'gpfdist://mdw:{port}/{os.path.basename(file_name)}'" for port in gpfdist_ports])

        external_table_definition = f"""
        CREATE EXTERNAL TABLE {table_name} (
            station_id VARCHAR(20),
            measurement_date DATE,
            element VARCHAR(10),
            value NUMERIC
        )
        LOCATION ({gpfdist_locations})
        FORMAT 'CSV' (HEADER)
        SEGMENT REJECT LIMIT 1 PERCENT;
        """

        with conn.cursor() as cur:
            cur.execute(external_table_definition)

        if display_definition:
            print("\nExternal Table Definition:")
            print(textwrap.dedent(external_table_definition).strip())

        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO ghcn_daily_test
                SELECT DISTINCT * FROM {table_name};
            """)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM ghcn_daily_test WHERE station_id = %s", (os.path.splitext(os.path.basename(file_name))[0],))
            inserted_row_count = cur.fetchone()[0]

        error_condition = None
        if inserted_row_count != csv_record_count:
            error_condition = f"Rows inserted: {inserted_row_count} (expected {csv_record_count})"

        update_file_status(conn, file_name, 'COMPLETED', csv_record_count, inserted_row_count, error_condition)
        if verbose:
            print(f"Processed {inserted_row_count} rows for file: {file_name}")

        with conn.cursor() as cur:
            cur.execute(f"DROP EXTERNAL TABLE IF EXISTS {table_name};")
        conn.commit()

        if debug:
            print(f"Finished processing file: {file_name}")

    except Exception as e:
        print(f"Error processing file {file_name}: {str(e)}")
        conn.rollback()  # Rollback the transaction in case of error
        update_file_status(conn, file_name, 'FAILED', error_condition=str(e))

def format_time(seconds):
    return str(timedelta(seconds=int(seconds)))


def optimized_load(conn, file_list, gpfdist_ports):
    # Create a single external table for all files
    ext_table_name = "ext_ghcn_all"
    gpfdist_locations = ", ".join([f"'gpfdist://mdw:{port}/*.csv'" for port in gpfdist_ports])

    create_ext_table_sql = f"""
    CREATE EXTERNAL TABLE {ext_table_name} (
        station_id VARCHAR(20),
        measurement_date DATE,
        element VARCHAR(10),
        value NUMERIC
    )
    LOCATION ({gpfdist_locations})
    FORMAT 'CSV' (HEADER)
    SEGMENT REJECT LIMIT 1 PERCENT;
    """

    with conn.cursor() as cur:
        cur.execute(create_ext_table_sql)

    # Load data in batches
    batch_size = 1000000  # Adjust based on your system's capabilities
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO ghcn_daily_test SELECT * FROM {ext_table_name}")

    # Drop the external table
    with conn.cursor() as cur:
        cur.execute(f"DROP EXTERNAL TABLE {ext_table_name}")

def main():
    parser = argparse.ArgumentParser(description='Process GHCN data files.')
    parser.add_argument('-n', type=int, default=1, help='Number of files to process')
    parser.add_argument('-g', type=int, default=1, help='Number of gpfdist processes to start')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output')
    parser.add_argument('--display-definition', action='store_true', help='Display external table definition')
    parser.add_argument('-p', '--progress', action='store_true', help='Display progress bar')
    args = parser.parse_args()

    conn = psycopg2.connect("dbname=climate_analysis user=gpadmin host=mdw")
    data_dir = '/home/gpadmin/data/ghcnd_all/processed_ghcn'

    gpfdist_processes = []
    gpfdist_ports = []
    try:
        for i in range(args.g):
            port = 8081 + i
            process = start_gpfdist(data_dir, port, args.verbose)
            if process:
                gpfdist_processes.append(process)
                gpfdist_ports.append(port)
            else:
                print(f"Failed to start gpfdist on port {port}")

        if not gpfdist_ports:
            print("No gpfdist processes started successfully. Exiting.")
            return

        start_time = time.time()
        files_processed = 0

        with ThreadPoolExecutor(max_workers=args.g) as executor:
            futures = []

            pbar = tqdm(total=args.n, disable=not args.progress)

            while files_processed < args.n:
                files = get_next_files(conn, min(args.g, args.n - files_processed))
                if not files:
                    print("No more files to process.")
                    break

                for file_name in files:
                    if files_processed >= args.n:
                        break
                    futures.append(executor.submit(process_file, file_name, conn, gpfdist_ports, args.verbose, args.display_definition, args.debug))
                    files_processed += 1

                # Wait for all futures to complete
                for future in as_completed(futures):
                    future.result()
                    if args.progress:
                        pbar.update(1)

            if args.progress:
                pbar.close()

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Get total record count in ghcn_daily_test
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ghcn_daily_test")
            total_records = cur.fetchone()[0]

        print(f"All requested files processed. Total files: {files_processed}")
        print(f"Total records in ghcn_daily_test: {total_records}")
        print(f"Elapsed time: {format_time(elapsed_time)}")
        if files_processed > 0:
            avg_time_per_file = elapsed_time / files_processed
            print(f"Average time per file: {format_time(avg_time_per_file)}")
        else:
            print("Average time per file: N/A (no files processed)")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        stop_gpfdist(gpfdist_processes)
        conn.close()

if __name__ == "__main__":
    main()
