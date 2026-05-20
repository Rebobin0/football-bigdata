from pyspark.sql.window import Window
from pyspark.sql.functions import (col,lower,regexp_replace,trim,when,row_number,length,lit,)
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_DIM_TEAMS_UNIFIED,
    PROCESSED_TEAM_MARKET_VALUES_TRANSFERMARKT,
    PROCESSED_TEAM_TRANSFERMARKT_MATCHES,
    LOCAL_TEAM_TRANSFERMARKT_MANUAL_MAPPING,
)

def normalize_name(column):
    cleaned = lower(trim(column))
    cleaned = regexp_replace(cleaned, "[^a-z0-9 ]", " ")
    cleaned = regexp_replace(cleaned, r"\bfootball club\b", "")
    cleaned = regexp_replace(cleaned, r"\bfutbol club\b", "")
    cleaned = regexp_replace(cleaned, r"\bclub de futbol\b", "")
    cleaned = regexp_replace(cleaned, r"\bclub\b", "")
    cleaned = regexp_replace(cleaned, r"\bfc\b", "")
    cleaned = regexp_replace(cleaned, r"\bcf\b", "")
    cleaned = regexp_replace(cleaned, r"\bsad\b", "")
    cleaned = regexp_replace(cleaned, r"\bcalcio\b", "")
    cleaned = regexp_replace(cleaned, r"\bspvgg\b", "")
    cleaned = regexp_replace(cleaned, r"\bsa\b", "")
    cleaned = regexp_replace(cleaned, r"\s+", " ")

    return trim(cleaned)

def main():
    spark = create_spark_session("Match Teams With Transfermarkt")

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
        .withColumn(
            "team_name_match",
            normalize_name(col("team_name"))
        )
        .crossJoin(
            df_market_clean.withColumn(
                "transfermarkt_name_match",
                normalize_name(col("transfermarkt_club_name"))
            )
        )
        .withColumn(
            "match_type",
            when(
                col("team_name_match") == col("transfermarkt_name_match"),
                "exact"
            )
            .when(
                col("transfermarkt_name_match").contains(col("team_name_match")),
                "contains_transfermarkt"
            )
            .when(
                col("team_name_match").contains(col("transfermarkt_name_match")),
                "contains_team"
            )
            .otherwise("no_match")
        )
        .filter(col("match_type") != "no_match")
        .filter(length(col("team_name_match")) >= 4)
        .filter(length(col("transfermarkt_name_match")) >= 4)
    )

    # Define a window to rank candidates for each team
    window = Window.partitionBy(
        "team_id",
        "league",
        "season",
        "source"
    ).orderBy(
        when(col("match_type") == "exact", 1)
        .when(col("match_type") == "contains_transfermarkt", 2)
        .when(col("match_type") == "contains_team", 3)
        .otherwise(99),
        col("squad_market_value_eur").desc()
    )

    # Select the best match for each team
    df_best_matches = (
        candidates
        .withColumn("rank", row_number().over(window))
        .filter(col("rank") == 1)
        .drop("rank")
    )

    # Manual
    manual_mapping = (
        spark.read
        .option("header", True)
        .csv(LOCAL_TEAM_TRANSFERMARKT_MANUAL_MAPPING)
        .select(
            col("league"),
            col("team_name"),
            col("transfermarkt_club_name").alias("manual_transfermarkt_club_name"),
            col("action")
        )
    )

    manual_market = (
        df_market_clean
        .withColumn(
            "manual_transfermarkt_name_match",
            normalize_name(col("transfermarkt_club_name"))
        )
        .select(
            col("transfermarkt_club_id"),
            col("transfermarkt_club_name"),
            col("squad_market_value_eur"),
            col("avg_player_market_value_eur"),
            col("players_count"),
            col("transfermarkt_name_normalized"),
            col("manual_transfermarkt_name_match")
        )
    )

    manual_matches = (
        df_teams_clean
        .withColumn(
            "team_name_match",
            normalize_name(col("team_name"))
        )
        .join(
            manual_mapping.withColumn(
                "manual_transfermarkt_name_match",
                normalize_name(col("manual_transfermarkt_club_name"))
            ),
            on=["league", "team_name"],
            how="inner"
        )
        .join(
            manual_market,
            on="manual_transfermarkt_name_match",
            how="inner"
        )
        .select(
            col("team_id"),
            col("team_name"),
            col("team_name_normalized"),
            col("league"),
            col("season"),
            col("source"),
            col("team_name_match"),
            col("transfermarkt_club_id"),
            col("transfermarkt_club_name"),
            col("squad_market_value_eur"),
            col("avg_player_market_value_eur"),
            col("players_count"),
            col("transfermarkt_name_normalized"),
            col("manual_transfermarkt_name_match").alias("transfermarkt_name_match"),
            lit("manual").alias("match_type")
        )
    )

    manual_keys = manual_matches.select(
        "team_id",
        "league",
        "season",
        "source"
    ).dropDuplicates()

    automatic_without_manual = (
        df_best_matches
        .join(
            manual_keys,
            on=["team_id", "league", "season", "source"],
            how="left_anti"
        )
    )

    df_final_matches = (
        manual_matches
        .unionByName(automatic_without_manual)
    )

    # Verify the results
    print("\n=== BEST MATCHES SCHEMA ===")
    df_final_matches.printSchema()

    print("\n=== TOTAL BEST MATCHES ===")
    print(df_final_matches.count())

    print("\n=== MATCHES BY TYPE ===")
    df_final_matches.groupBy("match_type").count().show(truncate=False)

    print("\n=== MATCHES BY LEAGUE ===")
    df_final_matches.groupBy("league").count().orderBy("league").show(50, truncate=False)

    print("\n=== SAMPLE BEST MATCHES ===")
    df_final_matches.select(
        "league",
        "team_id",
        "team_name",
        "team_name_match",
        "transfermarkt_club_id",
        "transfermarkt_club_name",
        "transfermarkt_name_match",
        "match_type",
        "squad_market_value_eur",
        "players_count",
    ).orderBy("league", "team_name").show(100, truncate=False)

    # Save the best matches to S3 in Parquet format
    (
        df_final_matches
        .write
        .mode("overwrite")
        .partitionBy("league")
        .parquet(PROCESSED_TEAM_TRANSFERMARKT_MATCHES)
    )

    print(f"\nData written to: {PROCESSED_TEAM_TRANSFERMARKT_MATCHES}")

    spark.stop()

if __name__ == "__main__":
    main()