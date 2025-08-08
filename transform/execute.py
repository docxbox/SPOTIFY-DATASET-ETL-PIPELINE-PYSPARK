import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

def create_spark_session():
    """Initialize Spark session."""
    return (SparkSession.builder
            .appName("SpotifyDataTransform")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "4g")
            .getOrCreate())

def load_and_clean(spark, input_dir, output_dir):
    artists_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("followers", T.FloatType(), True),
        T.StructField("genres", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("popularity", T.IntegerType(), True)
    ])

    recommendations_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("related-ids", T.ArrayType(T.StringType()), True)
    ])

    tracks_schema = T.StructType([
        T.StructField("id", T.StringType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("popularity", T.IntegerType(), True),
        T.StructField("duration_ms", T.IntegerType(), True),
        T.StructField("explicit", T.IntegerType(), True),
        T.StructField("artists", T.StringType(), True),
        T.StructField("id_artists", T.StringType(), True),
        T.StructField("release_date", T.StringType(), True),
        T.StructField("danceability", T.FloatType(), True),
        T.StructField("energy", T.FloatType(), True),
        T.StructField("key", T.IntegerType(), True),
        T.StructField("loudness", T.FloatType(), True),
        T.StructField("mode", T.IntegerType(), True),
        T.StructField("speechiness", T.FloatType(), True),
        T.StructField("acousticness", T.FloatType(), True),
        T.StructField("instrumentalness", T.FloatType(), True),
        T.StructField("liveness", T.FloatType(), True),
        T.StructField("valence", T.FloatType(), True),
        T.StructField("tempo", T.FloatType(), True),
        T.StructField("time_signature", T.IntegerType(), True)
    ])

    # Load raw data with schema
    artists_df = spark.read.schema(artists_schema).csv(os.path.join(input_dir, "artists.csv"), header=True)
    recommendations_df = spark.read.schema(recommendations_schema).json(os.path.join(input_dir, "fixed_da.json"))
    tracks_df = spark.read.schema(tracks_schema).csv(os.path.join(input_dir, "tracks.csv"), header=True)

    # Clean: remove duplicates and null IDs
    artists_df = artists_df.dropDuplicates(['id']).filter(F.col("id").isNotNull())
    recommendations_df = recommendations_df.dropDuplicates(['id']).filter(F.col("id").isNotNull())
    tracks_df = tracks_df.dropDuplicates(['id']).filter(F.col("id").isNotNull())

    # Save cleaned data as parquet
    artists_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "artists"))
    recommendations_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "recommendations"))
    tracks_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "tracks"))

    print("Stage 1: Cleaned the data")
    return artists_df, recommendations_df, tracks_df

def create_master_table(output_dir, artists_df, recommendations_df, tracks_df):
    # Convert 'id_artists' string to array of artist IDs
    tracks_df = tracks_df.withColumn(
        "id_artists_array",
        F.from_json(F.regexp_replace(F.col("id_artists"), "'", '"'), T.ArrayType(T.StringType()))
    )

    # Explode to get one row per artist-track pair
    tracks_exploded = tracks_df.select(
        F.col("id").alias("track_id"),
        F.col("name").alias("track_name"),
        F.col("popularity").alias("track_popularity"),
        F.explode(F.col("id_artists_array")).alias("artist_id")
    )

    # Rename artist columns to avoid ambiguity
    artists_renamed_df = artists_df.withColumnRenamed("name", "artist_name_col").withColumnRenamed("popularity", "artist_popularity_col")

    # Join tracks with artists on artist_id
    tracks_with_artists = tracks_exploded.join(
        artists_renamed_df,
        tracks_exploded.artist_id == artists_renamed_df.id,
        "left"
    ).select(
        "track_id",
        "track_name",
        "track_popularity",
        "artist_id",
        F.col("artist_name_col").alias("artist_name"),
        "followers",
        F.col("artist_popularity_col").alias("artist_popularity")
    )

    # Join with recommendations on artist_id
    master_df = tracks_with_artists.join(
        recommendations_df.withColumnRenamed("id", "rec_artist_id"),
        tracks_with_artists.artist_id == F.col("rec_artist_id"),
        "left"
    ).select(
        "track_id",
        "track_name",
        "track_popularity",
        "artist_id",
        "artist_name",
        "followers",
        "genres"
        "artist_popularity",
        F.col("related-ids")
    )

    # Save master table parquet
    master_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage2", "master_table"))
    print("Stage 2: Master table saved")

def create_query_tables(output_dir, artists_df, recommendations_df, tracks_df):
    """Stage 3: Create query-optimized tables."""

    # Explode recommendations related-ids
    recommendations_exploded = recommendations_df.withColumn("related_id", F.explode("related-ids"))
    recommendations_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "recommendations_exploded"))

    # Convert 'artists' JSON string to array and explode tracks with artist pairs
    tracks_exploded = tracks_df.withColumn("id_artists_array", F.from_json(F.col("artists"), T.ArrayType(T.StringType())))
    tracks_exploded = tracks_exploded.select(
        F.col("id").alias("track_id"),
        F.explode(F.col("id_artists_array")).alias("artist_id")
    )
    tracks_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artist_track"))

    # Select subset of track metadata fields present in schema
    tracks_metadata = tracks_df.select("id", "name", "popularity", "duration_ms", "energy", "tempo")
    tracks_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "track_metadata"))

    # Select subset of artist metadata fields
    artists_metadata = artists_df.select("id", "name", "followers", "popularity")
    artists_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artist_metadata"))

    print("Stage 3: Query-optimized tables saved")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()

    artists_df, recommendations_df, tracks_df = load_and_clean(spark, input_dir, output_dir)
    create_master_table(output_dir, artists_df, recommendations_df, tracks_df)
    create_query_tables(output_dir, artists_df, recommendations_df, tracks_df)

    print("Transformation pipeline completed")
