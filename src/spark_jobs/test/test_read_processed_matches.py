from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_FACT_MATCHES_API_FOOTBALL,
    PROCESSED_FACT_MATCHES_FOOTBALL_DATA,
)


def main():
    spark = create_spark_session("Test Processed Matches")

    print("\n=== API FOOTBALL ===")

    df_api = spark.read.parquet(
        PROCESSED_FACT_MATCHES_API_FOOTBALL
    )

    df_api.printSchema()

    print("\nTOTAL:")
    print(df_api.count())

    df_api.show(5, truncate=False)

    print("\n=== FOOTBALL DATA ===")

    df_fd = spark.read.parquet(
        PROCESSED_FACT_MATCHES_FOOTBALL_DATA
    )

    df_fd.printSchema()

    print("\nTOTAL:")
    print(df_fd.count())

    df_fd.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()