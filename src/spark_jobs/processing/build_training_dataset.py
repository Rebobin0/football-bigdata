from pyspark.sql.functions import col, when
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_ENRICHED_FACT_MATCHES,
    PROCESSED_ML_TRAINING_DATASET,
)

def main():
    spark = create_spark_session("Build ML Training Dataset")

    # Read enriched fact matches
    df = spark.read.parquet(PROCESSED_ENRICHED_FACT_MATCHES)

    # Build training dataset
    df_training = (
        df
        .filter(col("home_goals").isNotNull())
        .filter(col("away_goals").isNotNull())
        .filter(col("result").isin("HOME_WIN", "DRAW", "AWAY_WIN"))
        .select(
            col("match_id"),
            col("match_date"),
            col("league"),
            col("season"),
            col("source"),

            col("home_team_id"),
            col("home_team_name"),
            col("away_team_id"),
            col("away_team_name"),

            col("home_goals"),
            col("away_goals"),
            col("home_goals_halftime"),
            col("away_goals_halftime"),

            col("home_squad_market_value_eur"),
            col("away_squad_market_value_eur"),
            col("market_value_diff_eur"),

            col("home_avg_player_market_value_eur"),
            col("away_avg_player_market_value_eur"),
            col("home_players_count"),
            col("away_players_count"),

            col("result")
        )
        .withColumn(
            "target",
            when(col("result") == "HOME_WIN", 0)
            .when(col("result") == "DRAW", 1)
            .when(col("result") == "AWAY_WIN", 2)
        )
    )

    # Verify the training dataset
    print("\n=== TRAINING DATASET SCHEMA ===")
    df_training.printSchema()

    print("\n=== TOTAL ROWS ===")
    print(df_training.count())

    print("\n=== TARGET DISTRIBUTION ===")
    df_training.groupBy("result", "target").count().orderBy("target").show()

    print("\n=== ROWS BY LEAGUE AND SEASON ===")
    df_training.groupBy("league", "season").count().orderBy("league", "season").show(100, truncate=False)

    print("\n=== NULL MARKET VALUE CHECK ===")
    df_training.select(
        "home_squad_market_value_eur",
        "away_squad_market_value_eur",
        "market_value_diff_eur"
    ).summary("count").show()

    print("\n=== SAMPLE ===")
    df_training.show(20, truncate=False)

    # Write the training dataset to S3
    (
        df_training
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_ML_TRAINING_DATASET)
    )

    print(f"\nData written to: {PROCESSED_ML_TRAINING_DATASET}")

    spark.stop()

if __name__ == "__main__":
    main()