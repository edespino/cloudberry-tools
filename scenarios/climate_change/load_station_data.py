#!/usr/bin/env python3

import requests
import argparse
from tqdm import tqdm  # For progress indicator

# Function to create the weather_stations table if it doesn't exist
def create_table_if_not_exists(db_conn):
    cursor = db_conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_stations (
        station_id CHAR(11) PRIMARY KEY,
        latitude DECIMAL(8, 4),
        longitude DECIMAL(9, 4),
        elevation DECIMAL(5, 1),
        state VARCHAR(2),
        station_name VARCHAR(30),
        gsn_flag VARCHAR(3),
        hcn_crn_flag VARCHAR(3),
        wmo_id VARCHAR(5)
    );
    """
    cursor.execute(create_table_query)
    db_conn.commit()
    cursor.close()

# Function to fetch and parse the data
def fetch_and_parse_data(output=None, to_stdout=False, to_db=False, db_conn=None):
    # URL of the station text file
    url = 'https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'

    # Fetch the file from the internet
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve the file. Status code: {response.status_code}")
        exit()

    # Parse the fetched data
    lines = response.text.splitlines()  # Splitting the file content into lines
    if to_db:
        # Initialize tqdm progress bar if writing to DB
        for line in tqdm(lines, desc="Processing stations", unit="station"):
            process_line(line, output, to_stdout, to_db, db_conn)
    else:
        for line in lines:
            process_line(line, output, to_stdout, to_db, db_conn)

# Function to process and write a single line of data
def process_line(line, output, to_stdout, to_db, db_conn):
    if line.strip():  # Only process non-empty lines
        station_id = line[0:11].strip()  # ID
        latitude = float(line[12:20].strip())  # LATITUDE
        longitude = float(line[21:30].strip())  # LONGITUDE
        elevation = float(line[31:37].strip())  # ELEVATION

        # Convert elevation of -999.9 to NULL
        elevation = None if elevation == -999.9 else elevation

        state = line[38:40].strip() if line[38:40].strip() else None  # STATE (can be NULL)
        station_name = line[41:71].strip()  # NAME (fixed width CHAR(30))
        gsn_flag = line[72:75].strip() if line[72:75].strip() else None  # GSN FLAG (can be NULL)
        hcn_crn_flag = line[76:79].strip() if line[76:79].strip() else None  # HCN/CRN FLAG (can be NULL)
        wmo_id = line[80:85].strip() if line[80:85].strip() else None  # WMO ID (can be NULL)

        # Format the parsed data
        parsed_data = f"Station ID: {station_id}, Latitude: {latitude}, Longitude: {longitude}, " \
                      f"Elevation: {elevation}, State: {state}, Name: {station_name}, " \
                      f"GSN Flag: {gsn_flag}, HCN/CRN Flag: {hcn_crn_flag}, WMO ID: {wmo_id}\n"

        # Write to stdout if specified
        if to_stdout:
            output.write(parsed_data)

        # Write to Postgres database
        if to_db and db_conn:
            cursor = db_conn.cursor()
            cursor.execute("""
                INSERT INTO weather_stations (
                    station_id, latitude, longitude, elevation, state, station_name,
                    gsn_flag, hcn_crn_flag, wmo_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (station_id) DO NOTHING;
            """, (station_id, latitude, longitude, elevation, state, station_name, gsn_flag, hcn_crn_flag, wmo_id))
            db_conn.commit()

# Main function to handle arguments and logic
def main():
    parser = argparse.ArgumentParser(description="Parse weather station data and output to file, stdout, or Postgres DB.")
    parser.add_argument('--to-file', metavar='filename', type=str, help='Write output to the specified file.')
    parser.add_argument('--to-stdout', action='store_true', help='Write output to stdout.')
    parser.add_argument('--to-db', action='store_true', help='Write output to a Postgres database.')

    # Database connection arguments
    parser.add_argument('--db-name', type=str, help='Postgres database name.')
    parser.add_argument('--db-user', type=str, help='Postgres database user.')
    parser.add_argument('--db-password', type=str, help='Postgres database password.')
    parser.add_argument('--db-host', type=str, default='localhost', help='Postgres database host (default: localhost).')
    parser.add_argument('--db-port', type=str, default='5432', help='Postgres database port (default: 5432).')

    args = parser.parse_args()

    db_conn = None

    # Handle Postgres connection if --to-db is specified
    if args.to_db:
        try:
            # Only import psycopg2 if --to-db is used
            import psycopg2

            if not all([args.db_name, args.db_user, args.db_password]):
                print("Database credentials are required when using --to-db.")
                exit()

            db_conn = psycopg2.connect(
                dbname=args.db_name,
                user=args.db_user,
                password=args.db_password,
                host=args.db_host,
                port=args.db_port
            )

            # Create the table if it doesn't exist
            create_table_if_not_exists(db_conn)

        except ImportError:
            print("psycopg2 is not installed. Install it with 'pip install psycopg2'.")
            exit()
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            exit()

    if args.to_file:
        # Write output to the specified file
        with open(args.to_file, 'w') as f:
            fetch_and_parse_data(f, to_stdout=args.to_stdout, to_db=args.to_db, db_conn=db_conn)
    elif args.to_stdout:
        # Write output to stdout
        fetch_and_parse_data(sys.stdout, to_stdout=True, to_db=args.to_db, db_conn=db_conn)
    elif args.to_db:
        # Write output to the database
        fetch_and_parse_data(None, to_stdout=False, to_db=args.to_db, db_conn=db_conn)
    else:
        print("Please specify an output option: --to-file, --to-stdout, or --to-db.")

    if db_conn:
        db_conn.close()

# Entry point
if __name__ == '__main__':
    main()
