from pyspark.sql.functions import col, lower, regexp_replace, trim, levenshtein
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_DIM_TEAMS_UNIFIED,
    PROCESSED_TEAM_MARKET_VALUES_TRANSFERMARKT,
    PROCESSED_TEAM_MATCHING_CANDIDATES,
)

def normalize_name(column):
    return regexp_replace(
        lower(trim(column)),
        "[^a-z0-9 ]",
        ""
    )

def main():
    spark = create_spark_session("Generate Team Matching Candidates")

    # Read the processed teams and market values
    df_teams = spark.read.parquet(PROCESSED_DIM_TEAMS_UNIFIED)
    df_market = spark.read.parquet(PROCESSED_TEAM_MARKET_VALUES_TRANSFERMARKT)

    # Select relevant columns and normalize names
    df_teams_clean = (
        df_teams
        .select(
            col("team_id"),
            col("team_name"),
            col("team_name_normalized"),
            col("league"),
            col("season"),
            col("source")
        )
        .filter(col("season") == 2025)
    )

    df_market_clean = (
        df_market
        .select(
            col("current_club_id").alias("transfermarkt_club_id"),
            col("current_club_name").alias("transfermarkt_club_name"),
            col("squad_market_value_eur"),
            col("avg_player_market_value_eur"),
            col("players_count"),
        )
        .withColumn(
            "transfermarkt_name_normalized",
            normalize_name(col("transfermarkt_club_name"))
        )
    )

    # Generate matching candidates using a cross join and Levenshtein distance
    candidates = (
        df_teams_clean
        .crossJoin(df_market_clean)
        .withColumn(
            "name_distance",
            levenshtein(
                col("team_name_normalized"),
                col("transfermarkt_name_normalized")
            )
        )
        .filter(col("name_distance") <= 12)
        .orderBy("league", "team_name", "name_distance")
    )

    print("\n=== MATCHING CANDIDATES SCHEMA ===")
    candidates.printSchema()

    print("\n=== TOTAL CANDIDATES ===")
    print(candidates.count())

    print("\n=== SAMPLE CANDIDATES ===")
    candidates.select(
        "league",
        "team_id",
        "team_name",
        "transfermarkt_club_id",
        "transfermarkt_club_name",
        "name_distance",
        "squad_market_value_eur",
        "players_count",
    ).show(100, truncate=False)

    (
        candidates
        .write
        .mode("overwrite")
        .partitionBy("league")
        .parquet(PROCESSED_TEAM_MATCHING_CANDIDATES)
    )

    print(f"\nData written to: {PROCESSED_TEAM_MATCHING_CANDIDATES}")

    spark.stop()

if __name__ == "__main__":
    main()