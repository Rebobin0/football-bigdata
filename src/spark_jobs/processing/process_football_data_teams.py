from pyspark.sql.functions import col, explode, when
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    RAW_FOOTBALL_DATA_TEAMS,
    PROCESSED_DIM_TEAMS_FOOTBALL_DATA,
)

def main():
    spark = create_spark_session("Process Football Data Teams")

    # Read raw teams data from Football Data Org
    df_raw = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .option("multiLine", True)
        .json(RAW_FOOTBALL_DATA_TEAMS)
    )

    # Explode the teams array to get one row per team
    df_exploded = df_raw.select(
        col("competition.code").alias("competition_code"),
        col("competition.name").alias("competition_name"),
        col("season.startDate").alias("season_start_date"),
        explode(col("teams")).alias("team")
    )

    # Transform raw data to structured dim_teams format
    df_teams = (
        df_exploded
        .select(
            col("team.id").alias("team_id"),
            col("team.name").alias("team_name"),
            col("team.shortName").alias("team_short_name"),
            col("team.tla").alias("team_tla"),
            col("team.crest").alias("team_logo"),
            col("team.founded").alias("founded"),
            col("team.venue").alias("venue_name"),
            col("team.website").alias("website"),
            col("team.clubColors").alias("club_colors"),
            col("team.area.id").alias("area_id"),
            col("team.area.name").alias("country"),
            col("team.area.code").alias("country_code"),
            col("team.area.flag").alias("country_flag"),
            col("team.coach.id").alias("coach_id"),
            col("team.coach.name").alias("coach_name"),
            col("team.coach.nationality").alias("coach_nationality"),
            col("competition_code"),
            col("competition_name"),
            col("season_start_date"),
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
        .withColumn(
            "source",
            when(col("team_id").isNotNull(), "football_data_org")
            .otherwise("football_data_org")
        )
        .dropDuplicates(["team_id", "league", "season"])
    )

    # Verify the transformed data
    print("\n=== SCHEMA PROCESSED FOOTBALL DATA TEAMS ===")
    df_teams.printSchema()

    print("\n=== TOTAL ROWS ===")
    print(df_teams.count())

    print("\n=== ROWS BY LEAGUE AND SEASON ===")
    df_teams.groupBy("league", "season").count().orderBy("league", "season").show(100, truncate=False)

    print("\n=== SAMPLE ===")
    df_teams.show(10, truncate=False)

    # Save processed teams data back to S3 in Parquet format
    (
        df_teams
        .write
        .mode("overwrite")
        .partitionBy("league", "season")
        .parquet(PROCESSED_DIM_TEAMS_FOOTBALL_DATA)
    )

    print(f"\nData written to: {PROCESSED_DIM_TEAMS_FOOTBALL_DATA}")

    spark.stop()

if __name__ == "__main__":
    main()