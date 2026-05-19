import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")

spark = (
    SparkSession.builder
    .appName("Read Fixtures From S3")

    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.profile.ProfileCredentialsProvider"
    )

    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "s3.amazonaws.com"
    )

    .getOrCreate()
)

fixtures_path = (
    f"s3a://{S3_BUCKET}/raw/api_football/fixtures/"
)

print(f"\nReading data from:\n{fixtures_path}\n")

df = spark.read.json(fixtures_path)

print("\nSchema:\n")
df.printSchema()

print("\nSample data:\n")
df.show(5, truncate=False)

print("\nTotal rows:\n")
print(df.count())

spark.stop()