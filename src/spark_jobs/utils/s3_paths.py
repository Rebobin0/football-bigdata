import os
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
BASE_DIR = os.getcwd()

if not S3_BUCKET:
    raise ValueError("S3_BUCKET no está definido en el archivo .env")


def s3_path(prefix: str) -> str:
    prefix = prefix.lstrip("/")
    return f"s3a://{S3_BUCKET}/{prefix}"


# RAW - API Football
RAW_API_FOOTBALL_FIXTURES = s3_path("raw/api_football/fixtures/")
RAW_API_FOOTBALL_TEAMS = s3_path("raw/api_football/teams/")
RAW_API_FOOTBALL_STANDINGS = s3_path("raw/api_football/standings/")

# RAW - Football Data Org
RAW_FOOTBALL_DATA_MATCHES = s3_path("raw/football_data_org/matches/")
RAW_FOOTBALL_DATA_TEAMS = s3_path("raw/football_data_org/teams/")
RAW_FOOTBALL_DATA_STANDINGS = s3_path("raw/football_data_org/standings/")

# RAW - Transfermarkt
RAW_TRANSFERMARKT_CLUBS = s3_path("raw/transfermarkt/clubs/")
RAW_TRANSFERMARKT_PLAYERS = s3_path("raw/transfermarkt/players/")
RAW_TRANSFERMARKT_PLAYER_VALUATIONS = s3_path("raw/transfermarkt/player_valuations/")

# PROCESSED
# MATCHES
PROCESSED_FACT_MATCHES_API_FOOTBALL = s3_path("processed/api_football/fact_matches/")
PROCESSED_FACT_MATCHES_FOOTBALL_DATA = s3_path("processed/football_data_org/fact_matches/")
PROCESSED_FACT_MATCHES_UNIFIED = s3_path("processed/fact_matches/")
PROCESSED_ENRICHED_FACT_MATCHES = s3_path("processed/enriched_fact_matches/")

# TEAMS
PROCESSED_DIM_TEAMS_API_FOOTBALL = s3_path("processed/api_football/dim_teams/")
PROCESSED_DIM_TEAMS_FOOTBALL_DATA = s3_path("processed/football_data_org/dim_teams/")
PROCESSED_DIM_CLUBS_TRANSFERMARKT = s3_path("processed/transfermarkt/dim_clubs/")
PROCESSED_DIM_TEAMS_UNIFIED = s3_path("processed/dim_teams/")

# MARKET VALUES
PROCESSED_TEAM_MARKET_VALUES_TRANSFERMARKT = s3_path("processed/transfermarkt/team_market_values/")

# MATCH-TEAM CANDIDATES BY NAME
PROCESSED_TEAM_MATCHING_CANDIDATES = s3_path("processed/team_matching_candidates/")
PROCESSED_TEAM_TRANSFERMARKT_MATCHES = s3_path("processed/team_transfermarkt_matches/")

# STANDINGS
PROCESSED_FACT_STANDINGS_API_FOOTBALL = s3_path(
    "processed/api_football/fact_standings/"
)

# ML
PROCESSED_ML_TRAINING_DATASET = s3_path(
    "processed/ml/training_dataset/"
)

# LOCAL
LOCAL_TEAM_TRANSFERMARKT_MANUAL_MAPPING = "file://" + os.path.join(
    BASE_DIR,
    "data",
    "reference",
    "team_transfermarkt_manual_mapping.csv"
)