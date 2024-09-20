#!/usr/bin/env python3

import psycopg2
import os

# List of 10 CSV files for testing purposes
csv_files = [
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/USW00003950.csv",
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/HRE00155176.csv",
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/US1FLMA0050.csv",
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/ASN00091068.csv",
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/US1COAR0066.csv",
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/US1MDCL0011.csv",
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/USC00237967.csv",
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/US1NCWT0001.csv",
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/USC00412794.csv",
    "/home/gpadmin/data/ghcnd_all/processed_ghcn/US1TXKF0003.csv"
]

def insert_csv_files(conn, csv_files):
    with conn.cursor() as cur:
        for file_name in csv_files:
            cur.execute("""
                INSERT INTO ghcn_load_control_test (file_name, status)
                VALUES (%s, 'PENDING')
                ON CONFLICT (file_name) DO NOTHING;
            """, (file_name,))
        conn.commit()

def main():
    # Connect to the database
    conn = psycopg2.connect("dbname=climate_analysis user=gpadmin host=mdw")

    try:
        # Insert 10 CSV files into the control table
        insert_csv_files(conn, csv_files)
        print("Inserted 10 CSV files into ghcn_load_control_test.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
