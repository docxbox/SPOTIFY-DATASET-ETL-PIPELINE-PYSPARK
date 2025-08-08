import sys
import os
import psycopg2
from pyspark.sql import SparkSession

def create_spark_session():
    """Initialize Spark session."""
    return SparkSession.builder \
        .appName("SpotifyDataLoad") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars", "/home/likhil/Workspace/pyspark/venv/lib/python3.12/site-packages/pyspark/jars/postgresql-42.7.7.jar") \
        .getOrCreate()

def create_postgres_tables():
    """Create necessary tables in PostgreSQL if they don't exist."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="docx12345",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Drop master_table if it exists
        cursor.execute("DROP TABLE IF EXISTS master_table;")
        conn.commit()
        print("[PostgreSQL] Dropped master_table if it existed.")

        create_table_queries = [
            # artist_track: link table for tracks and artists
            """
            CREATE TABLE IF NOT EXISTS artist_track (
                track_id VARCHAR(50),
                artist_id VARCHAR(50)
            );
            """,
            # track_metadata: metadata for tracks
            """
            CREATE TABLE IF NOT EXISTS track_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                popularity INTEGER,
                duration_ms INTEGER,
                danceability FLOAT,
                energy FLOAT,
                tempo FLOAT
            );
            """,
            # artist_metadata: metadata for artists
            """
            CREATE TABLE IF NOT EXISTS artist_metadata (
                id VARCHAR(50) PRIMARY KEY,
                name TEXT,
                followers FLOAT,
                popularity INTEGER
            );
            """,
            # master_table: combined track-artist info and recommendations
            """
            CREATE TABLE IF NOT EXISTS master_table (
                track_id VARCHAR(50),
                track_name TEXT,
                track_popularity INTEGER,
                artist_id TEXT,
                artist_name TEXT,
                followers FLOAT,
                artist_popularity INTEGER,
                related_ids TEXT[]
            );
            """,
            # recommendations_exploded: exploded recommendation pairs
            """
            CREATE TABLE IF NOT EXISTS recommendations_exploded (
                id VARCHAR(50),
                related_id VARCHAR(50)
            );
            """
        ]

        for query in create_table_queries:
            cursor.execute(query)
            conn.commit()
            print("[PostgreSQL] Table created or already exists")

    except Exception as e:
        print(f"[ERROR] Creating tables: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_to_postgres(spark, input_dir):
    """Load transformed Parquet data into PostgreSQL tables via JDBC."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": "postgres",
        "password": "docx12345",
        "driver": "org.postgresql.Driver"
    }

    # Mapping parquet relative paths to target Postgres tables
    tables = [
        ("stage2/master_table", "master_table"),
        ("stage3/recommendations_exploded", "recommendations_exploded"),
        ("stage3/artist_track", "artist_track"),
        ("stage3/track_metadata", "track_metadata"),
        ("stage3/artist_metadata", "artist_metadata")
    ]

    for parquet_path, table_name in tables:
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))

            # Rename columns if needed
            if table_name == "master_table":
                df = df.withColumnRenamed("related-ids", "related_ids")
            # Use append mode for master_table to accumulate, overwrite otherwise
            write_mode = "append" if table_name == "master_table" else "overwrite"

            df.write \
                .mode(write_mode) \
                .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

            print(f"[PostgreSQL] Loaded {table_name} successfully")

        except Exception as e:
            print(f"[ERROR] Loading {table_name}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python execute.py <input_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]

    spark = create_spark_session()

    print("[INFO] Creating PostgreSQL tables...")
    create_postgres_tables()

    print("[INFO] Loading Parquet files to PostgreSQL...")
    load_to_postgres(spark, input_dir)

    print("[SUCCESS] Data loading pipeline completed.")
