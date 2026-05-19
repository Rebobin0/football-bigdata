from pyspark.sql import SparkSession

def create_spark_session(app_name: str):

    spark = (
        SparkSession.builder
        .appName(app_name)
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

    return spark