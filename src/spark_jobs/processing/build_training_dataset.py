from pyspark.sql.functions import col, when, lit
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_ENRICHED_FACT_MATCHES,
    PROCESSED_ML_TRAINING_DATASET,
    PROCESSED_FACT_STANDINGS_API_FOOTBALL,
    s3_path
)

PROCESSED_FACT_STANDINGS_FOOTBALL_DATA = s3_path("processed/football_data_org/fact_standings/")

def main():
    spark = create_spark_session("Build ML Training Dataset")

    df = spark.read.parquet(PROCESSED_ENRICHED_FACT_MATCHES)

    df_api = spark.read.parquet(PROCESSED_FACT_STANDINGS_API_FOOTBALL).withColumn("source", lit("api_football"))
    df_fd = spark.read.parquet(PROCESSED_FACT_STANDINGS_FOOTBALL_DATA).withColumn("source", lit("football_data_org"))

    cols = ["team_id", "league", "season", "source", "rank", "points", "goal_difference"]
    df_standings = df_api.select(cols).unionByName(df_fd.select(cols))

    home_standings = (
        df_standings
        .select(
            col("team_id").alias("home_team_id"), col("league"), col("season"), col("source"),
            col("rank").alias("home_rank"), col("points").alias("home_points"), col("goal_difference").alias("home_goal_diff")
        )
    )

    away_standings = (
        df_standings
        .select(
            col("team_id").alias("away_team_id"), col("league"), col("season"), col("source"),
            col("rank").alias("away_rank"), col("points").alias("away_points"), col("goal_difference").alias("away_goal_diff")
        )
    )

    df_enriched_with_standings = (
        df
        .join(home_standings, on=["league", "season", "source", "home_team_id"], how="left")
        .join(away_standings, on=["league", "season", "source", "away_team_id"], how="left")
    )

    df_training = (
        df_enriched_with_standings
        .filter(col("home_goals").isNotNull())
        .filter(col("away_goals").isNotNull())
        .filter(col("result").isin("HOME_WIN", "DRAW", "AWAY_WIN"))
        .select(
            col("match_id"), col("match_date"), col("league"), col("season"), col("source"),
            col("home_team_id"), col("home_team_name"), col("away_team_id"), col("away_team_name"),
            col("home_goals"), col("away_goals"),
            
            col("home_squad_market_value_eur"), col("away_squad_market_value_eur"), col("market_value_diff_eur"),
            col("home_avg_player_market_value_eur"), col("away_avg_player_market_value_eur"),
            col("home_players_count"), col("away_players_count"),

            col("home_rank"), col("away_rank"), 
            col("home_points"), col("away_points"), 
            col("home_goal_diff"), col("away_goal_diff"),

            col("result")
        )
        .withColumn(
            "target",
            when(col("result") == "HOME_WIN", 0)
            .when(col("result") == "DRAW", 1)
            .when(col("result") == "AWAY_WIN", 2)
        )
    )

    df_future = (
        df_enriched_with_standings
        .filter(col("home_goals").isNull())
    )

    PROCESSED_ML_FUTURE_DATASET = s3_path("processed/ml/future_dataset/")
    
    (
        df_future
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_ML_FUTURE_DATASET)
    )
    print(f"\nPartidos futuros guardados en: {PROCESSED_ML_FUTURE_DATASET}")

    (
        df_training
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_ML_TRAINING_DATASET)
    )

    print(f"\nDataset listo y guardado en S3")
    spark.stop()

if __name__ == "__main__":
    main()
