from pyspark.sql.functions import col, explode, when
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import RAW_FOOTBALL_DATA_STANDINGS, s3_path

PROCESSED_FACT_STANDINGS_FOOTBALL_DATA = s3_path("processed/football_data_org/fact_standings/")

def main():
    spark = create_spark_session("Process Football Data Standings")

    df_raw = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .option("multiLine", True)
        .json(RAW_FOOTBALL_DATA_STANDINGS)
    )

    df_exploded = df_raw.select(
        col("competition.code").alias("competition_code"),
        col("season.startDate").alias("season_start_date"),
        explode(col("standings")).alias("standing")
    ).filter(col("standing.type") == "TOTAL")

    df_table = df_exploded.select(
        col("competition_code"),
        col("season_start_date"),
        explode(col("standing.table")).alias("row")
    )

    df_standings = (
        df_table
        .select(
            col("row.team.id").alias("team_id"),
            col("row.team.name").alias("team_name"),
            col("row.position").alias("rank"),
            col("row.points").alias("points"),
            col("row.goalDifference").alias("goal_difference"),
            col("row.form").alias("recent_form"),
            col("competition_code"),
            col("season_start_date")
        )
        .withColumn(
            "league",
            when(col("competition_code") == "PL", "premier_league")
            .when(col("competition_code") == "PD", "la_liga")
            .when(col("competition_code") == "SA", "serie_a")
            .when(col("competition_code") == "BL1", "bundesliga")
            .when(col("competition_code") == "FL1", "ligue_1")
            .otherwise(col("competition_code"))
        )
        .withColumn(
            "season",
            col("season_start_date").substr(1, 4).cast("int")
        )
        .drop("competition_code", "season_start_date")
    )

    print("\n=== ESQUEMA FOOTBALL DATA STANDINGS ===")
    df_standings.printSchema()
    
    (
        df_standings
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_FACT_STANDINGS_FOOTBALL_DATA)
    )
    print(f"\nDatos guardados en: {PROCESSED_FACT_STANDINGS_FOOTBALL_DATA}")
    
    spark.stop()

if __name__ == "__main__":
    main()