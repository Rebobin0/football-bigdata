from pyspark.sql.functions import col, lower, regexp_replace, trim
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import(
    RAW_TRANSFERMARKT_CLUBS,
    PROCESSED_DIM_CLUBS_TRANSFERMARKT
)

def main():
    spark = create_spark_session("Process Transfermarkt Clubs")

    # Read raw clubs data from Transfermarkt
    df_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RAW_TRANSFERMARKT_CLUBS)
    )

    # Transform raw data to structured dim_clubs format
    df_clubs = (
        df_raw
        .select(
            col("club_id"),
            col("club_code"),
            col("name").alias("club_name"),
            col("domestic_competition_id"),
            col("squad_size"),
            col("average_age"),
            col("foreigners_number"),
            col("foreigners_percentage"),
            col("national_team_players"),
            col("stadium_name"),
            col("stadium_seats"),
            col("net_transfer_record"),
            col("coach_name"),
            col("last_season"),
            col("url"),
            col("league"),
        )
        .withColumn(
            "club_name_normalized",
            lower(trim(col("club_name")))
        )
        .withColumn(
            "club_name_normalized",
            regexp_replace(col("club_name_normalized"), "[^a-z0-9 ]", "")
        )
        .dropDuplicates(["club_id", "league"])
    )

    # Verify the transformed data
    print("\n=== SCHEMA PROCESSED TRANSFERMARKT CLUBS ===")
    df_clubs.printSchema()

    print("\n=== TOTAL ROWS ===")
    print(df_clubs.count())

    print("\n=== ROWS BY LEAGUE ===")
    df_clubs.groupBy("league").count().orderBy("league").show(50, truncate=False)

    print("\n=== SAMPLE ===")
    df_clubs.show(10, truncate=False)

    # Save data in Parquet format partitioned by league
    (
        df_clubs
        .write
        .mode("overwrite")
        .partitionBy("league")
        .parquet(PROCESSED_DIM_CLUBS_TRANSFERMARKT)
    )

    print(f"\nData written to: {PROCESSED_DIM_CLUBS_TRANSFERMARKT}")

    spark.stop()

if __name__ == "__main__":
    main()