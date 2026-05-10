import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET_TEST")

spark = (
    SparkSession.builder
    .appName("S3 Connection Test")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4"
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

df = spark.read.csv(
    f"s3a://{S3_BUCKET}/",
    header=True
)

df.show(5)

spark.stop()