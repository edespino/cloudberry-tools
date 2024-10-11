#!/usr/bin/env python3

import os
import argparse
import psycopg2
from psycopg2 import sql
from tqdm import tqdm

def drop_table_and_partitions(cur):
    print("Dropping existing table and partitions...")
    cur.execute("""
    DROP TABLE IF EXISTS gaia_stars_full CASCADE;
    """)
    print("Table and partitions dropped.")

def create_table(cur):
    print("Creating new gaia_stars_full table...")
    cur.execute("""
    CREATE TABLE gaia_stars_full (
        solution_id BIGINT,
        designation TEXT,
        source_id BIGINT,
        random_index BIGINT,
        ref_epoch DOUBLE PRECISION,
        ra DOUBLE PRECISION,
        ra_error DOUBLE PRECISION,
        dec DOUBLE PRECISION,
        dec_error DOUBLE PRECISION,
        parallax DOUBLE PRECISION,
        parallax_error DOUBLE PRECISION,
        parallax_over_error DOUBLE PRECISION,
        pmra DOUBLE PRECISION,
        pmra_error DOUBLE PRECISION,
        pmdec DOUBLE PRECISION,
        pmdec_error DOUBLE PRECISION,
        ra_dec_corr DOUBLE PRECISION,
        ra_parallax_corr DOUBLE PRECISION,
        ra_pmra_corr DOUBLE PRECISION,
        ra_pmdec_corr DOUBLE PRECISION,
        dec_parallax_corr DOUBLE PRECISION,
        dec_pmra_corr DOUBLE PRECISION,
        dec_pmdec_corr DOUBLE PRECISION,
        parallax_pmra_corr DOUBLE PRECISION,
        parallax_pmdec_corr DOUBLE PRECISION,
        pmra_pmdec_corr DOUBLE PRECISION,
        astrometric_n_obs_al INT,
        astrometric_n_obs_ac INT,
        astrometric_n_good_obs_al INT,
        astrometric_n_bad_obs_al INT,
        astrometric_gof_al DOUBLE PRECISION,
        astrometric_chi2_al DOUBLE PRECISION,
        astrometric_excess_noise DOUBLE PRECISION,
        astrometric_excess_noise_sig DOUBLE PRECISION,
        astrometric_params_solved INT,
        astrometric_primary_flag BOOLEAN,
        astrometric_weight_al DOUBLE PRECISION,
        astrometric_pseudo_colour DOUBLE PRECISION,
        astrometric_pseudo_colour_error DOUBLE PRECISION,
        mean_varpi_factor_al DOUBLE PRECISION,
        astrometric_matched_observations INT,
        visibility_periods_used INT,
        astrometric_sigma5d_max DOUBLE PRECISION,
        frame_rotator_object_type INT,
        matched_observations INT,
        duplicated_source BOOLEAN,
        phot_g_n_obs INT,
        phot_g_mean_flux DOUBLE PRECISION,
        phot_g_mean_flux_error DOUBLE PRECISION,
        phot_g_mean_flux_over_error DOUBLE PRECISION,
        phot_g_mean_mag DOUBLE PRECISION,
        phot_bp_n_obs INT,
        phot_bp_mean_flux DOUBLE PRECISION,
        phot_bp_mean_flux_error DOUBLE PRECISION,
        phot_bp_mean_flux_over_error DOUBLE PRECISION,
        phot_bp_mean_mag DOUBLE PRECISION,
        phot_rp_n_obs INT,
        phot_rp_mean_flux DOUBLE PRECISION,
        phot_rp_mean_flux_error DOUBLE PRECISION,
        phot_rp_mean_flux_over_error DOUBLE PRECISION,
        phot_rp_mean_mag DOUBLE PRECISION,
        phot_bp_rp_excess_factor DOUBLE PRECISION,
        phot_proc_mode INT,
        bp_rp DOUBLE PRECISION,
        bp_g DOUBLE PRECISION,
        g_rp DOUBLE PRECISION,
        radial_velocity DOUBLE PRECISION,
        radial_velocity_error DOUBLE PRECISION,
        rv_nb_transits INT,
        rv_template_teff DOUBLE PRECISION,
        rv_template_logg DOUBLE PRECISION,
        rv_template_fe_h DOUBLE PRECISION,
        phot_variable_flag TEXT,
        l DOUBLE PRECISION,
        b DOUBLE PRECISION,
        ecl_lon DOUBLE PRECISION,
        ecl_lat DOUBLE PRECISION,
        priam_flags BIGINT,
        teff_val DOUBLE PRECISION,
        teff_percentile_lower DOUBLE PRECISION,
        teff_percentile_upper DOUBLE PRECISION,
        a_g_val DOUBLE PRECISION,
        a_g_percentile_lower DOUBLE PRECISION,
        a_g_percentile_upper DOUBLE PRECISION,
        e_bp_min_rp_val DOUBLE PRECISION,
        e_bp_min_rp_percentile_lower DOUBLE PRECISION,
        e_bp_min_rp_percentile_upper DOUBLE PRECISION,
        flame_flags INT,
        radius_val DOUBLE PRECISION,
        radius_percentile_lower DOUBLE PRECISION,
        radius_percentile_upper DOUBLE PRECISION,
        lum_val DOUBLE PRECISION,
        lum_percentile_lower DOUBLE PRECISION,
        lum_percentile_upper DOUBLE PRECISION
    )
    WITH (
        APPENDONLY=true,
        ORIENTATION=column,
        COMPRESSTYPE=zstd,
        COMPRESSLEVEL=1
    )
    DISTRIBUTED BY (source_id);
    """)
    print("Table created.")

def load_gaia_data(directory, db_params):
    if not os.path.isdir(directory):
        raise ValueError(f"The directory {directory} does not exist.")

    files = [f for f in os.listdir(directory) if f.endswith('.csv.gz')]
    if not files:
        raise ValueError(f"No .csv.gz files found in {directory}")

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Ensure Q3C extension is created
    cur.execute("CREATE EXTENSION IF NOT EXISTS q3c;")

    drop_table_and_partitions(cur)
    create_table(cur)

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

def create_indexes(db_params):
    print("\nCreating indexes...")
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    index_commands = [
        "CREATE INDEX ON gaia_stars_full (ra, dec)",
        "CREATE INDEX ON gaia_stars_full (source_id)",
        "CREATE INDEX ON gaia_stars_full (phot_g_mean_mag)",
        "CREATE INDEX ON gaia_stars_full (parallax)",
        "CREATE INDEX ON gaia_stars_full (pmra, pmdec)",
        "CREATE INDEX ON gaia_stars_full (q3c_ang2ipix(ra, dec))",
        "CREATE INDEX ON gaia_stars_full USING BRIN (ra, dec)",
        "CREATE INDEX ON gaia_stars_full USING BRIN (phot_g_mean_mag)"
    ]

    for command in tqdm(index_commands, desc="Creating indexes"):
        cur.execute(command)
        conn.commit()

    cur.close()
    conn.close()
    print("Indexes created.")

def main():
    parser = argparse.ArgumentParser(description="Load Gaia DR2 data into a non-partitioned Greenplum table")
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
        create_indexes(db_params)
        print(f"\nData loading and indexing complete. Total rows: {total_rows}")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
