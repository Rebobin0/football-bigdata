import os
from dotenv import load_dotenv
from src.spark_jobs.utils.spark_session import create_spark_session

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")

spark = create_spark_session("Explore Raw Source Data")

sources = [
    ("api_football_fixtures", "json", "raw/api_football/fixtures/", False),
    ("api_football_teams", "json", "raw/api_football/teams/", False),
    ("api_football_standings", "json", "raw/api_football/standings/", False),

    ("football_data_matches", "json", "raw/football_data_org/matches/", True),
    ("football_data_teams", "json", "raw/football_data_org/teams/", True),
    ("football_data_standings", "json", "raw/football_data_org/standings/", True),

    ("transfermarkt_clubs", "csv", "raw/transfermarkt/clubs/", False),
    ("transfermarkt_players", "csv", "raw/transfermarkt/players/", False),
    ("transfermarkt_player_valuations", "csv", "raw/transfermarkt/player_valuations/", False),
]

for name, fmt, prefix, recursive in sources:
    print("\n" + "=" * 80)
    print(f"SOURCE: {name}")
    print(f"PATH: s3a://{S3_BUCKET}/{prefix}")
    print("=" * 80)

    try:
        path = f"s3a://{S3_BUCKET}/{prefix}"

        reader = spark.read

        if recursive:
            reader = reader.option("recursiveFileLookup", "true")

        if fmt == "json":
            df = (
                reader
                .option("multiLine", True)
                .option("pathGlobFilter", "*.json")
                .json(path)
            )
        else:
            df = (
                reader
                .option("header", True)
                .option("inferSchema", True)
                .option("pathGlobFilter", "*.csv")
                .csv(path)
            )

        print("\nSCHEMA:")
        df.printSchema()

        print("\nCOUNT:")
        print(df.count())

        print("\nCOLUMNS:")
        print(df.columns)

        # print("\nSAMPLE:")
        # df.show(3, truncate=False)

    except Exception as e:
        print(f"\nERROR reading {name}:")
        print(e)

spark.stop()