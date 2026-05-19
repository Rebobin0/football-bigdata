from pyspark.sql.functions import col, lit, when

from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_FACT_MATCHES_API_FOOTBALL,
    PROCESSED_FACT_MATCHES_FOOTBALL_DATA,
    PROCESSED_FACT_MATCHES_UNIFIED,
)

def main():
    spark = create_spark_session("Union Fact Matches")

    # Read processed matches data from both sources
    df_api = spark.read.parquet(PROCESSED_FACT_MATCHES_API_FOOTBALL)
    df_fd = spark.read.parquet(PROCESSED_FACT_MATCHES_FOOTBALL_DATA)

    # Normalize API Football data
    df_api_normalized = (
        df_api
        .select(
            col("match_id"),
            col("match_date_raw"),
            col("match_date"),

            col("league"),
            col("season"),

            col("home_team_id"),
            col("home_team_name"),
            col("home_team_logo"),

            col("away_team_id"),
            col("away_team_name"),
            col("away_team_logo"),

            col("home_goals"),
            col("away_goals"),
            col("home_goals_halftime"),
            col("away_goals_halftime"),

            col("match_status"),
            col("result"),

            lit("api_football").alias("source"),
        )
    )

    # Normalize Football Data Org data
    df_fd_normalized = (
        df_fd
        .withColumn(
            "league_normalized",
            when(col("league") == "PL", "premier_league")
            .when(col("league") == "PD", "la_liga")
            .when(col("league") == "SA", "serie_a")
            .when(col("league") == "BL1", "bundesliga")
            .when(col("league") == "FL1", "ligue_1")
            .otherwise(col("league"))
        )
        .withColumn(
            "result_normalized",
            when(col("winner_raw") == "HOME_TEAM", "HOME_WIN")
            .when(col("winner_raw") == "AWAY_TEAM", "AWAY_WIN")
            .when(col("winner_raw") == "DRAW", "DRAW")
            .otherwise("UNKNOWN")
        )
        .select(
            col("match_id"),
            col("match_date_raw"),
            col("match_date"),

            col("league_normalized").alias("league"),
            col("season"),

            col("home_team_id"),
            col("home_team_name"),
            col("home_team_logo"),

            col("away_team_id"),
            col("away_team_name"),
            col("away_team_logo"),

            col("home_goals"),
            col("away_goals"),
            col("home_goals_halftime"),
            col("away_goals_halftime"),

            col("match_status"),
            col("result_normalized").alias("result"),

            col("source"),
        )
    )

    # Union the two DataFrames
    df_unified = df_api_normalized.unionByName(df_fd_normalized)

    # Verify unified data
    print("\n=== SCHEMA UNIFIED FACT_MATCHES ===")
    df_unified.printSchema()

    print("\n=== TOTAL ROWS ===")
    print(df_unified.count())

    print("\n=== ROWS BY SOURCE ===")
    df_unified.groupBy("source").count().show(truncate=False)

    print("\n=== ROWS BY LEAGUE AND SEASON ===")
    df_unified.groupBy("league", "season").count().orderBy("league", "season").show(100, truncate=False)

    print("\n=== SAMPLE ===")
    df_unified.show(10, truncate=False)

    # Save unified matches data back to S3 in Parquet format
    (
        df_unified
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_FACT_MATCHES_UNIFIED)
    )

    print(f"\nData written to: {PROCESSED_FACT_MATCHES_UNIFIED}")

    spark.stop()

if __name__ == "__main__":
    main()