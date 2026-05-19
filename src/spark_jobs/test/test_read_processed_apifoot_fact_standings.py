from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import PROCESSED_FACT_STANDINGS_API_FOOTBALL

spark = create_spark_session("Test Read Processed Fact Standings")

df = spark.read.parquet(PROCESSED_FACT_STANDINGS_API_FOOTBALL)
df.printSchema()
df.show(5, truncate=False)
print(f"Total rows: {df.count()}")

spark.stop()