import psycopg2
import math

def analyze_ra_distribution(dbname, user, password, host, port):
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    # Get min and max RA
    cur.execute("SELECT MIN(ra) as min_ra, MAX(ra) as max_ra FROM gaia_stars_full;")
    min_ra, max_ra = cur.fetchone()

    # Get total count
    cur.execute("SELECT COUNT(*) FROM gaia_stars_full;")
    total_count = cur.fetchone()[0]

    # Analyze distribution
    num_buckets = 20
    cur.execute(f"""
    SELECT
        WIDTH_BUCKET(ra, {min_ra}, {max_ra}, {num_buckets}) as bucket,
        MIN(ra) as min_ra,
        MAX(ra) as max_ra,
        COUNT(*) as star_count
    FROM gaia_stars_full
    GROUP BY bucket
    ORDER BY bucket;
    """)
    distribution = cur.fetchall()

    conn.close()

    # Suggest partitioning
    ra_range = max_ra - min_ra
    suggested_partitions = math.ceil(total_count / 2000)  # Aim for at least 2000 stars per partition
    partition_size = ra_range / suggested_partitions

    print(f"Data Analysis:")
    print(f"Total stars: {total_count}")
    print(f"RA range: {min_ra:.2f} to {max_ra:.2f}")
    print(f"\nSuggested Partitioning:")
    print(f"Number of partitions: {suggested_partitions}")
    print(f"Partition size: {partition_size:.2f} degrees")
    print(f"\nSuggested values for partition script:")
    print(f"ra_min = {min_ra:.2f}")
    print(f"ra_max = {max_ra:.2f}")
    print(f"num_partitions = {suggested_partitions}")

    print("\nRA Distribution:")
    for bucket, bucket_min, bucket_max, count in distribution:
        print(f"RA {bucket_min:.2f} to {bucket_max:.2f}: {count} stars")

# Usage
analyze_ra_distribution(
    dbname="gpadmin",
    user="gpadmin",
    password="gpadmin",
    host="localhost",
    port="5432"
)
