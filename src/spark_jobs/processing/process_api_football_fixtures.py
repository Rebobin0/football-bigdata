from pyspark.sql.functions import col, when, to_timestamp
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    RAW_API_FOOTBALL_FIXTURES, 
    PROCESSED_FACT_MATCHES_API_FOOTBALL
)

def main():
    spark = create_spark_session("Process API Football Fixtures")

    # Read raw data from S3
    df_raw = spark.read.json(RAW_API_FOOTBALL_FIXTURES)

    # Transform raw data to structured fact_matches format
    df_matches = (
        df_raw
        .select(
            col("fixture.id").alias("match_id"),
            col("fixture.date").alias("match_date_raw"),
            to_timestamp(col("fixture.date")).alias("match_date"),
            col("fixture.timestamp").alias("match_timestamp"),
            col("fixture.timezone").alias("timezone"),
            col("fixture.referee").alias("referee"),
            col("league").alias("league"),
            col("season").alias("season"),
            col("fixture.venue.id").alias("venue_id"),
            col("fixture.venue.name").alias("venue_name"),
            col("fixture.venue.city").alias("venue_city"),
            col("teams.home.id").alias("home_team_id"),
            col("teams.home.name").alias("home_team_name"),
            col("teams.home.logo").alias("home_team_logo"),
            col("teams.home.winner").alias("home_winner"),
            col("teams.away.id").alias("away_team_id"),
            col("teams.away.name").alias("away_team_name"),
            col("teams.away.logo").alias("away_team_logo"),
            col("teams.away.winner").alias("away_winner"),
            col("goals.home").alias("home_goals"),
            col("goals.away").alias("away_goals"),
            col("score.halftime.home").alias("home_goals_halftime"),
            col("score.halftime.away").alias("away_goals_halftime"),
            col("score.fulltime.home").alias("home_goals_fulltime"),
            col("score.fulltime.away").alias("away_goals_fulltime"),
            col("fixture.status.long").alias("match_status"),
            col("fixture.status.short").alias("match_status_short"),
            col("fixture.status.elapsed").alias("elapsed"),
        )
        .withColumn(
            "result",
            when(col("home_goals") > col("away_goals"), "HOME_WIN")
            .when(col("home_goals") < col("away_goals"), "AWAY_WIN")
            .when(col("home_goals") == col("away_goals"), "DRAW")
            .otherwise("UNKNOWN")
        )
    )

    # Verify transformed data
    print("\n=== SCHEMA PROCESSED FACT_MATCHES ===")
    df_matches.printSchema()

    print("\n=== SAMPLE ===")
    df_matches.show(5, truncate=False)

    print("\n=== TOTAL ROWS ===")
    print(df_matches.count())

    # Save processed data back to S3 in Parquet format
    (
        df_matches
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_FACT_MATCHES_API_FOOTBALL)
    )

    print(f"\nData written to: {PROCESSED_FACT_MATCHES_API_FOOTBALL}")

    spark.stop()

if __name__ == "__main__":
    main()