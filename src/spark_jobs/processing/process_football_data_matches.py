from pyspark.sql.functions import col, explode, to_timestamp, lit
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    RAW_FOOTBALL_DATA_MATCHES,
    PROCESSED_FACT_MATCHES_FOOTBALL_DATA,
)


def main():
    spark = create_spark_session("Process Football Data Matches")

    # Read raw matches data from Football Data Org
    df_raw = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .option("multiLine", True)
        .json(RAW_FOOTBALL_DATA_MATCHES)
    )

    # Explode the matches array to get one row per match
    df_exploded = df_raw.select(
        col("competition.code").alias("competition_code"),
        col("competition.name").alias("competition_name"),
        explode(col("matches")).alias("match")
    )

    # Transform raw data to structured fact_matches format
    df_matches = (
        df_exploded
        .select(
            col("match.id").alias("match_id"),
            col("match.utcDate").alias("match_date_raw"),
            to_timestamp(col("match.utcDate")).alias("match_date"),
            col("competition_code"),
            col("competition_name"),
            col("match.season.startDate").alias("season_start_date"),
            col("match.season.endDate").alias("season_end_date"),
            col("match.matchday").alias("matchday"),
            col("match.stage").alias("stage"),
            col("match.status").alias("match_status"),
            col("match.homeTeam.id").alias("home_team_id"),
            col("match.homeTeam.name").alias("home_team_name"),
            col("match.homeTeam.shortName").alias("home_team_short_name"),
            col("match.homeTeam.tla").alias("home_team_tla"),
            col("match.homeTeam.crest").alias("home_team_logo"),
            col("match.awayTeam.id").alias("away_team_id"),
            col("match.awayTeam.name").alias("away_team_name"),
            col("match.awayTeam.shortName").alias("away_team_short_name"),
            col("match.awayTeam.tla").alias("away_team_tla"),
            col("match.awayTeam.crest").alias("away_team_logo"),
            col("match.score.fullTime.home").alias("home_goals"),
            col("match.score.fullTime.away").alias("away_goals"),
            col("match.score.halfTime.home").alias("home_goals_halftime"),
            col("match.score.halfTime.away").alias("away_goals_halftime"),
            col("match.score.winner").alias("winner_raw"),
            lit("football_data_org").alias("source"),
        )
    )

    # Add league and season columns
    df_matches = (
        df_matches
        .withColumn(
            "league",
            col("competition_code")
        )
        .withColumn(
            "season",
            col("season_start_date").substr(1, 4).cast("int")
        )
    )

    # Verify transformed data
    print("\n=== SCHEMA PROCESSED FOOTBALL DATA MATCHES ===")
    df_matches.printSchema()

    print("\n=== SAMPLE ===")
    df_matches.show(10, truncate=False)

    print("\n=== TOTAL ROWS ===")
    print(df_matches.count())

    # Save processed matches data back to S3 in Parquet format
    (
        df_matches
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_FACT_MATCHES_FOOTBALL_DATA)
    )

    print(f"\nData written to: {PROCESSED_FACT_MATCHES_FOOTBALL_DATA}")

    spark.stop()


if __name__ == "__main__":
    main()