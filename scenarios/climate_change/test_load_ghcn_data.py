#!/usr/bin/env python3

import psycopg2
import os
import csv
import shutil
import argparse
import subprocess
import time
from datetime import timedelta
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

def create_gpfdist_directories(base_dir, num_servers):
    dirs = []
    for i in range(num_servers):
        dir_path = os.path.join(base_dir, f'gpfdist_{i}')
        os.makedirs(dir_path, exist_ok=True)
        dirs.append(dir_path)
    return dirs

def distribute_files(file_list, gpfdist_dirs):
    for i, file_path in enumerate(file_list):
        target_dir = gpfdist_dirs[i % len(gpfdist_dirs)]
        symlink_path = os.path.join(target_dir, os.path.basename(file_path))
        os.symlink(file_path, symlink_path)

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

        csv_record_count = count_csv_records(file_name)
        if verbose:
            print(f"Number of records in the file (excluding header): {csv_record_count}")

        table_name = f"temp_ext_{os.path.splitext(os.path.basename(file_name))[0]}"
        gpfdist_locations = ", ".join([f"'gpfdist://mdw:{port}/{os.path.basename(file_name)}'" for port in gpfdist_ports])

        external_table_definition = f"""
        CREATE EXTERNAL TABLE {table_name} (
            station_id VARCHAR(20),
            observation_date DATE,
            element CHAR(4),
            value NUMERIC,
            mflag VARCHAR(1),
            qflag VARCHAR(1),
            sflag VARCHAR(1)
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
        conn.rollback()
        update_file_status(conn, file_name, 'FAILED', error_condition=str(e))

def batch_process(conn, gpfdist_dirs, gpfdist_ports, batch_size, verbose, display_definition, debug):
    ext_table_name = "ext_ghcn_batch"
    gpfdist_locations = [f"'gpfdist://mdw:{port}/*.csv'" for port in gpfdist_ports]
    location_clause = ", ".join(gpfdist_locations)

    create_ext_table_sql = f"""
    CREATE EXTERNAL TABLE {ext_table_name} (
        station_id VARCHAR(20),
        observation_date DATE,
        element CHAR(4),
        value NUMERIC,
        mflag VARCHAR(1),
        qflag VARCHAR(1),
        sflag VARCHAR(1)
    )
    LOCATION ({location_clause})
    FORMAT 'CSV' (HEADER)
    SEGMENT REJECT LIMIT 1 PERCENT;
    """

    if display_definition:
        print("\nExternal Table Definition:")
        print(textwrap.dedent(create_ext_table_sql).strip())

    with conn.cursor() as cur:
        cur.execute(create_ext_table_sql)

    if debug:
        print(f"Created external table: {ext_table_name}")
        print(f"Location clause: {location_clause}")

    try:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO ghcn_daily_test SELECT * FROM {ext_table_name}")

        if verbose:
            print(f"Inserted data from external table into ghcn_daily_test")

    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP EXTERNAL TABLE IF EXISTS {ext_table_name}")

        if debug:
            print(f"Dropped external table: {ext_table_name}")

def format_time(seconds):
    return str(timedelta(seconds=int(seconds)))

def main():
    parser = argparse.ArgumentParser(description='Process GHCN data files.')
    parser.add_argument('-n', type=int, default=1, help='Number of files to process')
    parser.add_argument('-g', type=int, default=1, help='Number of gpfdist processes to start')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug output')
    parser.add_argument('--display-definition', action='store_true', help='Display external table definition')
    parser.add_argument('-p', '--progress', action='store_true', help='Display progress bar')
    parser.add_argument('-b', '--batch', action='store_true', help='Enable batch processing')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing when using batch mode')
    args = parser.parse_args()

    conn = psycopg2.connect("dbname=climate_analysis user=gpadmin host=mdw")
    base_data_dir = '/home/gpadmin/data/ghcnd_all/processed_ghcn'
    work_dir = '/home/gpadmin/data/ghcnd_all/work_dir'

    gpfdist_processes = []
    gpfdist_ports = []
    gpfdist_dirs = []

    try:
        # Progress: Starting gpfdist processes
        print("Starting gpfdist processes...")
        if args.batch:
            gpfdist_dirs = create_gpfdist_directories(work_dir, args.g)

        for i in range(args.g):
            port = 8081 + i
            dir_path = gpfdist_dirs[i] if args.batch else base_data_dir
            process = start_gpfdist(dir_path, port, args.verbose)
            if process:
                gpfdist_processes.append(process)
                gpfdist_ports.append(port)
                print(f"Started gpfdist on port {port}")
            else:
                print(f"Failed to start gpfdist on port {port}")

        if not gpfdist_ports:
            print("No gpfdist processes started successfully. Exiting.")
            return

        start_time = time.time()
        files_processed = 0
        total_records = 0

        # Progress: Main processing loop
        print(f"Beginning to process {args.n} files...")
        if args.batch:
            with tqdm(total=args.n, disable=not args.progress, desc="Batch Progress") as pbar:
                while files_processed < args.n:
                    files = get_next_files(conn, min(args.batch_size, args.n - files_processed))
                    if not files:
                        print("No more files to process.")
                        break

                    print(f"Processing batch of {len(files)} files...")
                    distribute_files(files, gpfdist_dirs)
                    batch_process(conn, gpfdist_dirs, gpfdist_ports, args.batch_size, args.verbose, args.display_definition, args.debug)

                    for file_name in files:
                        update_file_status(conn, file_name, 'COMPLETED')

                    files_processed += len(files)
                    pbar.update(len(files))

                    for dir_path in gpfdist_dirs:
                        for file_name in os.listdir(dir_path):
                            os.unlink(os.path.join(dir_path, file_name))

                    if args.verbose:
                        print(f"Processed {files_processed} files so far.")
        else:
            with ThreadPoolExecutor(max_workers=args.g) as executor:
                futures = []
                with tqdm(total=args.n, disable=not args.progress, desc="File Progress") as pbar:
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

                        for future in as_completed(futures):
                            future.result()
                            pbar.update(1)

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Progress: Final statistics
        print("\nCalculating final statistics...")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ghcn_daily_test")
            total_records = cur.fetchone()[0]

        print(f"\nProcessing complete!")
        print(f"Total files processed: {files_processed}")
        print(f"Total records in ghcn_daily_test: {total_records}")
        print(f"Total elapsed time: {format_time(elapsed_time)}")
        if files_processed > 0:
            avg_time_per_file = elapsed_time / files_processed
            print(f"Average time per file: {format_time(avg_time_per_file)}")
        else:
            print("Average time per file: N/A (no files processed)")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Progress: Cleanup
        print("\nCleaning up...")
        stop_gpfdist(gpfdist_processes)

        if args.batch:
            for dir_path in gpfdist_dirs:
                shutil.rmtree(dir_path, ignore_errors=True)

        conn.close()
        print("Cleanup complete.")

if __name__ == "__main__":
    main()
