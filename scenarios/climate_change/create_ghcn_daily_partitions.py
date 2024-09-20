#!/usr/bin/env python3
import psycopg2

def drop_table_if_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS ghcn_daily_test CASCADE;
        """)
        conn.commit()
    print("Dropped existing ghcn_daily_test table (if it existed).")

def create_table_with_partitions(start_year, end_year, conn):
    # SQL to create the base table with all partitions
    partition_definitions = []
    # Loop to create partition definitions for each year
    for year in range(start_year, end_year + 1):
        partition_name = f"p{year}"
        partition_start = f"'{year}-01-01'"
        partition_end = f"'{year + 1}-01-01'"
        partition_definitions.append(
            f"PARTITION {partition_name} START ({partition_start}) END ({partition_end})"
        )
    # Add default partition to handle out-of-range data
    partition_definitions.append("DEFAULT PARTITION p_default")
    # Join all partition definitions into a single string
    partitions_sql = ",\n".join(partition_definitions)
    # SQL to create the table with all partitions, including the default partition
    create_table_sql = f"""
    CREATE TABLE ghcn_daily_test (
        station_id VARCHAR(20),
        measurement_date DATE,
        element VARCHAR(10),
        value NUMERIC
    )
    DISTRIBUTED BY (station_id)  -- Choose an appropriate distribution key
    PARTITION BY RANGE (measurement_date) (
        {partitions_sql}
    );
    """
    # Execute the base table creation with all partitions
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
        conn.commit()
    print("Table and partitions (including default) created successfully.")

def main():
    # Connection to the database (adjust as necessary)
    conn = psycopg2.connect("dbname=climate_analysis user=gpadmin host=mdw")
    try:
        # Drop the existing table if it exists
        drop_table_if_exists(conn)

        # Generate the table and partitions from 1954 to 2024, with a default partition
        create_table_with_partitions(1954, 2024, conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
