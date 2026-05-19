import os
from dotenv import load_dotenv
from src.spark_jobs.utils.spark_session import create_spark_session

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")

spark = create_spark_session("Explore API Football Raw Data")

path = f"s3a://{S3_BUCKET}/raw/api_football/fixtures/"

df = spark.read.option("multiLine", True).json(path)

print("\n=== SCHEMA FIXTURES ===")
df.printSchema()

print("\n=== TOTAL REGISTROS ===")
print(df.count())

print("\n=== MUESTRA ===")
df.show(3, truncate=False)

spark.stop()