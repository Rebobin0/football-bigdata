from pyspark.sql.functions import (col, sum, avg, count, round,)
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    RAW_TRANSFERMARKT_PLAYERS,
    PROCESSED_TEAM_MARKET_VALUES_TRANSFERMARKT,
)

def main():
    spark = create_spark_session("Process Transfermarkt Team Values")

    # Read the raw data
    df_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_TRANSFERMARKT_PLAYERS)
    )

    # Select relevant columns
    df_players = (
        df_raw
        .select(
            col("player_id"),
            col("name").alias("player_name"),
            col("current_club_id"),
            col("current_club_name"),
            col("market_value_in_eur"),
            col("highest_market_value_in_eur"),
            col("position"),
            col("country_of_citizenship"),
            col("last_season")
        )
        .filter(col("current_club_id").isNotNull())
        .filter(col("market_value_in_eur").isNotNull())
        .filter(col("last_season") >= 2023)
    )

    # Aggregate market values by club
    df_team_values = (
        df_players
        .groupBy(
            "current_club_id",
            "current_club_name"
        )
        .agg(
            sum("market_value_in_eur")
            .alias("squad_market_value_eur"),

            round(
                avg("market_value_in_eur"),
                2
            ).alias("avg_player_market_value_eur"),

            count("player_id")
            .alias("players_count"),

            round(
                avg("highest_market_value_in_eur"),
                2
            ).alias("avg_highest_market_value_eur")
        )
        .orderBy(
            col("squad_market_value_eur").desc()
        )
    )

    # Verify the results
    print("\n=== TEAM MARKET VALUES SCHEMA ===")
    df_team_values.printSchema()

    print("\n=== TOTAL CLUBS ===")
    print(df_team_values.count())

    print("\n=== TOP 20 MOST VALUABLE TEAMS ===")

    df_team_values.show(20, truncate=False)

    # Save the results in Parquet format
    (
        df_team_values
        .write
        .mode("overwrite")
        .parquet(
            PROCESSED_TEAM_MARKET_VALUES_TRANSFERMARKT
        )
    )

    print(
        f"\nData written to: "
        f"{PROCESSED_TEAM_MARKET_VALUES_TRANSFERMARKT}"
    )

    spark.stop()

if __name__ == "__main__":
    main()