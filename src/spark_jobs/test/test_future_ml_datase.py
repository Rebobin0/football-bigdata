from pyspark.sql.functions import col
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    PROCESSED_ML_TRAINING_DATASET,
    PROCESSED_ML_FUTURE_DATASET
)

spark = create_spark_session("Test TRAINING Teams")
df_unified = spark.read.parquet(PROCESSED_ML_FUTURE_DATASET)

df_unified.show(50, truncate=False)