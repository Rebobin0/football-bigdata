from pyspark.sql.functions import col, lit, lower, regexp_replace, trim
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_DIM_TEAMS_API_FOOTBALL,
    PROCESSED_DIM_TEAMS_FOOTBALL_DATA,
    PROCESSED_DIM_TEAMS_UNIFIED,
)

def normalize_name(column):
    return regexp_replace(
        lower(trim(column)),
        "[^a-z0-9 ]",
        ""
    )

def main():
    spark = create_spark_session("Union Dim Teams")

    # Read processed dim_teams data from both sources
    df_api = spark.read.parquet(PROCESSED_DIM_TEAMS_API_FOOTBALL)
    df_fd = spark.read.parquet(PROCESSED_DIM_TEAMS_FOOTBALL_DATA)

    # Normalize API Football teams
    df_api_normalized = (
        df_api
        .select(
            col("team_id"),
            col("team_name"),
            lit(None).cast("string").alias("team_short_name"),
            col("team_code").alias("team_tla"),
            col("team_logo"),
            col("country"),
            col("founded"),
            col("venue_name"),
            col("venue_city"),
            col("venue_capacity"),
            col("league"),
            col("season"),
            lit("api_football").alias("source"),
        )
        .withColumn(
            "team_name_normalized",
            normalize_name(col("team_name"))
        )
    )

    # Normalize Football Data Org teams
    df_fd_normalized = (
        df_fd
        .select(
            col("team_id"),
            col("team_name"),
            col("team_short_name"),
            col("team_tla"),
            col("team_logo"),
            col("country"),
            col("founded"),
            col("venue_name"),
            lit(None).cast("string").alias("venue_city"),
            lit(None).cast("long").alias("venue_capacity"),
            col("league"),
            col("season"),
            col("source"),
        )
        .withColumn(
            "team_name_normalized",
            normalize_name(col("team_name"))
        )
    )

    # Union the two datasets and drop duplicates
    df_unified = (
        df_api_normalized
        .unionByName(df_fd_normalized)
        .dropDuplicates(["team_id", "league", "season", "source"])
    )

    # Verify the unified data
    print("\n=== SCHEMA UNIFIED DIM_TEAMS ===")
    df_unified.printSchema()

    print("\n=== TOTAL ROWS ===")
    print(df_unified.count())

    print("\n=== ROWS BY SOURCE ===")
    df_unified.groupBy("source").count().show(truncate=False)

    print("\n=== ROWS BY LEAGUE AND SEASON ===")
    df_unified.groupBy("league", "season").count().orderBy("league", "season").show(100, truncate=False)

    print("\n=== SAMPLE ===")
    df_unified.show(20, truncate=False)

    # Save the unified data in Parquet format partitioned by league and season
    (
        df_unified
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_DIM_TEAMS_UNIFIED)
    )

    print(f"\nData written to: {PROCESSED_DIM_TEAMS_UNIFIED}")

    spark.stop()

if __name__ == "__main__":
    main()