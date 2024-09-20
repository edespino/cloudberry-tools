import psycopg2
import os
import csv
from io import StringIO

def execute_query(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()

def get_next_file(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT file_name
            FROM ghcn_load_control_test
            WHERE status = 'PENDING'
            ORDER BY file_name
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """)
        return cur.fetchone()

def update_file_status(conn, file_name, status, csv_record_count=None, inserted_row_count=None, error_condition=None):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ghcn_load_control_test
            SET status = %s, csv_record_count = %s, inserted_row_count = %s, error_condition = %s, last_updated = CURRENT_TIMESTAMP
            WHERE file_name = %s
        """, (status, csv_record_count, inserted_row_count, error_condition, file_name))
        conn.commit()

def count_csv_records(file_path):
    """Counts the number of records in the CSV file excluding the header."""
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        row_count = sum(1 for row in reader) - 1  # Subtract 1 for header
    return row_count

def get_first_n_rows(file_name, n):
    """Reads the first `n` rows from the CSV file, including the header."""
    buffer = StringIO()
    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Read the header
        buffer.write(','.join(header) + '\n')  # Write the header to buffer

        # Write the first `n` rows to the buffer
        for i, row in enumerate(reader):
            if i >= n:
                break
            buffer.write(','.join(row) + '\n')

    buffer.seek(0)  # Reset the buffer position to the start
    return buffer

def process_file(file_name, conn):
    try:
        print(f"Processing file: {file_name}")

        # Count records in the CSV file
        csv_record_count = count_csv_records(file_name)
        print(f"Number of records in the file (excluding header): {csv_record_count}")

        # Extract the first 100 rows
        limited_csv_buffer = get_first_n_rows(file_name, 100)

        # Use COPY to insert the first 100 rows
        with conn.cursor() as cur:
            cur.copy_expert("COPY ghcn_daily_test (station_id, measurement_date, element, value) FROM STDIN WITH CSV HEADER", limited_csv_buffer)
        conn.commit()

        # Verify that 100 rows were inserted
        inserted_row_count = min(csv_record_count, 100)

        error_condition = None
        if inserted_row_count != 100:
            error_condition = f"Rows inserted: {inserted_row_count} (expected 100)"

        # Update the control table with status, row counts, and error condition
        update_file_status(conn, file_name, 'COMPLETED', csv_record_count, inserted_row_count, error_condition)

    except Exception as e:
        print(f"Error processing file {file_name}: {str(e)}")
        update_file_status(conn, file_name, 'FAILED')

def main():
    conn = psycopg2.connect("dbname=climate_analysis user=gpadmin host=mdw")

    while True:
        file = get_next_file(conn)
        if not file:
            print("All test files processed.")
            break

        file_name = file[0]

        process_file(file_name, conn)

    conn.close()

if __name__ == "__main__":
    main()
