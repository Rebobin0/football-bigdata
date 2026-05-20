from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import PROCESSED_DIM_TEAMS_FOOTBALL_DATA

spark = create_spark_session("Test Read Processed Dim Teams")

df = spark.read.parquet(PROCESSED_DIM_TEAMS_FOOTBALL_DATA)
df.printSchema()
df.show(5, truncate=False)
print(f"Total rows: {df.count()}")

spark.stop()