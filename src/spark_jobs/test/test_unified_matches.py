from pyspark.sql.functions import col
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_FACT_MATCHES_UNIFIED
)

spark = create_spark_session("Test Unified Matches")
df_unified = spark.read.parquet(PROCESSED_FACT_MATCHES_UNIFIED)

df_unified.groupBy("match_id").count() \
    .filter(col("count") > 1) \
    .show(50, truncate=False)

df_unified.groupBy("league", "season", "source").count() \
    .orderBy("league", "season", "source") \
    .show(100, truncate=False)