from pyspark.sql.functions import col
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    RAW_API_FOOTBALL_TEAMS,
    PROCESSED_DIM_TEAMS_API_FOOTBALL,
)

def main():
    spark = create_spark_session("Process API Football Teams")

    # Read raw teams data from S3
    df_raw = spark.read.json(RAW_API_FOOTBALL_TEAMS)

    # Transform raw data to structured dim_teams format
    df_teams = (
        df_raw
        .select(
            col("team.id").alias("team_id"),
            col("team.name").alias("team_name"),
            col("team.code").alias("team_code"),
            col("team.country").alias("country"),
            col("team.founded").alias("founded"),
            col("team.logo").alias("team_logo"),
            col("team.national").alias("is_national_team"),
            col("venue.id").alias("venue_id"),
            col("venue.name").alias("venue_name"),
            col("venue.city").alias("venue_city"),
            col("venue.address").alias("venue_address"),
            col("venue.capacity").alias("venue_capacity"),
            col("venue.surface").alias("venue_surface"),
            col("venue.image").alias("venue_image"),
            col("league").alias("league"),
            col("season").alias("season"),
        )
        .dropDuplicates(["team_id", "league", "season"])
    )

    # Verify transformed data
    print("\n=== SCHEMA PROCESSED DIM_TEAMS ===")
    df_teams.printSchema()

    print("\n=== SAMPLE ===")
    df_teams.show(10, truncate=False)

    print("\n=== TOTAL ROWS ===")
    print(df_teams.count())

    # Save processed teams data back to S3 in Parquet format
    (
        df_teams
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_DIM_TEAMS_API_FOOTBALL)
    )

    print(f"\nData written to: {PROCESSED_DIM_TEAMS_API_FOOTBALL}")

    spark.stop()

if __name__ == "__main__":
    main()