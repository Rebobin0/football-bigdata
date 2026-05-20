from pyspark.sql.functions import col
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_FACT_MATCHES_UNIFIED,
    PROCESSED_TEAM_TRANSFERMARKT_MATCHES,
    PROCESSED_ENRICHED_FACT_MATCHES,
)


def main():
    spark = create_spark_session("Process Enriched Fact Matches")

    # Read the unified fact matches and the team market values
    df_matches = spark.read.parquet(PROCESSED_FACT_MATCHES_UNIFIED)
    df_team_values = spark.read.parquet(PROCESSED_TEAM_TRANSFERMARKT_MATCHES)

    # Prepare the team market values for joining
    home_values = (
        df_team_values
        .select(
            col("team_id").alias("home_team_id"),
            col("league").alias("home_league"),
            col("transfermarkt_club_id").alias("home_transfermarkt_club_id"),
            col("transfermarkt_club_name").alias("home_transfermarkt_club_name"),
            col("squad_market_value_eur").alias("home_squad_market_value_eur"),
            col("avg_player_market_value_eur").alias("home_avg_player_market_value_eur"),
            col("players_count").alias("home_players_count"),
        )
    )

    away_values = (
        df_team_values
        .select(
            col("team_id").alias("away_team_id"),
            col("league").alias("away_league"),
            col("transfermarkt_club_id").alias("away_transfermarkt_club_id"),
            col("transfermarkt_club_name").alias("away_transfermarkt_club_name"),
            col("squad_market_value_eur").alias("away_squad_market_value_eur"),
            col("avg_player_market_value_eur").alias("away_avg_player_market_value_eur"),
            col("players_count").alias("away_players_count"),
        )
    )

    # Join the matches with the team market values for both home and away teams
    df_enriched = (
        df_matches
        .join(
            home_values,
            (df_matches.home_team_id == home_values.home_team_id)
            & (df_matches.league == home_values.home_league),
            "left"
        )
        .drop(home_values.home_team_id)
        .drop("home_league")
        .join(
            away_values,
            (df_matches.away_team_id == away_values.away_team_id)
            & (df_matches.league == away_values.away_league),
            "left"
        )
        .drop(away_values.away_team_id)
        .drop("away_league")
        .withColumn(
            "market_value_diff_eur",
            col("home_squad_market_value_eur") - col("away_squad_market_value_eur")
        )
    )

    # Verify the results
    print("\n=== ENRICHED FACT MATCHES SCHEMA ===")
    df_enriched.printSchema()

    print("\n=== TOTAL ROWS ===")
    print(df_enriched.count())

    print("\n=== NULL MARKET VALUES CHECK ===")
    df_enriched.select(
        "home_squad_market_value_eur",
        "away_squad_market_value_eur",
        "market_value_diff_eur"
    ).summary("count").show()

    print("\n=== SAMPLE ===")
    df_enriched.select(
        "league",
        "season",
        "match_date",
        "home_team_name",
        "away_team_name",
        "home_squad_market_value_eur",
        "away_squad_market_value_eur",
        "market_value_diff_eur",
        "result",
        "source"
    ).show(30, truncate=False)

    # Save the enriched matches data back to S3 in Parquet format
    (
        df_enriched
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_ENRICHED_FACT_MATCHES)
    )

    print(f"\nData written to: {PROCESSED_ENRICHED_FACT_MATCHES}")

    spark.stop()

if __name__ == "__main__":
    main()