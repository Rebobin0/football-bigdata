from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_DIM_TEAMS_UNIFIED,
    PROCESSED_TEAM_TRANSFERMARKT_MATCHES,
    PROCESSED_TEAM_MARKET_VALUES_TRANSFERMARKT,
    PROCESSED_FACT_STANDINGS_API_FOOTBALL,
    PROCESSED_FACT_STANDINGS_FOOTBALL_DATA,
    PROCESSED_ENRICHED_FACT_MATCHES
)


def show_dataset(spark, name, path):
    print(f"\n\n===== {name} =====")
    df = spark.read.parquet(path)
    df.printSchema()
    df.show(5, truncate=False)


def main():
    spark = create_spark_session("Inspeccionar datasets equipos")

    show_dataset(spark, "DIM_TEAMS_UNIFIED", PROCESSED_DIM_TEAMS_UNIFIED)
    show_dataset(spark, "TEAM_TRANSFERMARKT_MATCHES", PROCESSED_TEAM_TRANSFERMARKT_MATCHES)
    show_dataset(spark, "TEAM_MARKET_VALUES_TRANSFERMARKT", PROCESSED_TEAM_MARKET_VALUES_TRANSFERMARKT)
    show_dataset(spark, "STANDINGS_API_FOOTBALL", PROCESSED_FACT_STANDINGS_API_FOOTBALL)
    show_dataset(spark, "STANDINGS_FOOTBALL_DATA", PROCESSED_FACT_STANDINGS_FOOTBALL_DATA)
    show_dataset(spark, "ENRICHED_FACT_MATCHES", PROCESSED_ENRICHED_FACT_MATCHES)

    spark.stop()


if __name__ == "__main__":
    main()