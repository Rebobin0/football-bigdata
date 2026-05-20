from pyspark.sql.functions import col
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_DIM_TEAMS_UNIFIED
)

spark = create_spark_session("Test Unified Teams")
df_unified = spark.read.parquet(PROCESSED_DIM_TEAMS_UNIFIED)

df_unified.groupBy("team_id").count() \
    .filter(col("count") > 1) \
    .show(50, truncate=False)

df_unified.groupBy("league", "season", "source").count() \
    .orderBy("league", "season", "source") \
    .show(100, truncate=False)

df_unified.groupBy("source").count().show()
df_unified.groupBy("league", "season", "source").count().orderBy("league", "season", "source").show(100)
df_unified.select("team_id", "team_name", "team_name_normalized", "league", "season", "source").show(50, truncate=False)