from pyspark.sql.functions import col, when, to_timestamp
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    RAW_API_FOOTBALL_STANDINGS,
    PROCESSED_FACT_STANDINGS_API_FOOTBALL
)

def main():
    spark = create_spark_session("Process API Football Standings")

    # Read raw teams data from S3
    df_raw = spark.read.json(RAW_API_FOOTBALL_STANDINGS)

    # Transform raw data to structured fact_standings format
    df_standings = (
        df_raw
        .select(
            col("team.id").alias("team_id"),
            col("team.name").alias("team_name"),
            col("team.logo").alias("team_logo"),
            col("rank").alias("rank"),
            col("points").alias("points"),
            col("goalsDiff").alias("goal_difference"),
            col("form").alias("recent_form"),
            col("description").alias("description"),
            col("status").alias("status"),
            col("group").alias("group_name"),
            col("all.played").alias("played_all"),
            col("all.win").alias("wins_all"),
            col("all.draw").alias("draws_all"),
            col("all.lose").alias("losses_all"),
            col("all.goals.for").alias("goals_for_all"),
            col("all.goals.against").alias("goals_against_all"),
            col("home.played").alias("played_home"),
            col("home.win").alias("wins_home"),
            col("home.draw").alias("draws_home"),
            col("home.lose").alias("losses_home"),
            col("home.goals.for").alias("goals_for_home"),
            col("home.goals.against").alias("goals_against_home"),
            col("away.played").alias("played_away"),
            col("away.win").alias("wins_away"),
            col("away.draw").alias("draws_away"),
            col("away.lose").alias("losses_away"),
            col("away.goals.for").alias("goals_for_away"),
            col("away.goals.against").alias("goals_against_away"),
            col("update").alias("last_updated"),
            col("league").alias("league"),
            col("season").alias("season"),
        )
        .dropDuplicates(["team_id", "league", "season"])
    )

    # Verify transformed data
    print("\n=== SCHEMA PROCESSED FACT_STANDINGS ===")
    df_standings.printSchema()

    print("\n=== SAMPLE ===")
    df_standings.show(10, truncate=False)

    print("\n=== TOTAL ROWS ===")
    print(df_standings.count())

    # Save processed standings data back to S3 in Parquet format
    (
        df_standings
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_FACT_STANDINGS_API_FOOTBALL)
    )

    print(f"\nData written to: {PROCESSED_FACT_STANDINGS_API_FOOTBALL}")

    spark.stop()


if __name__ == "__main__":
    main()