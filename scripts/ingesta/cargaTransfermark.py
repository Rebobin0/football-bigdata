import pandas as pd
import boto3
from io import StringIO

# CONFIGURACIÓN
BASE_PATH = "/home/hadoop/transfermarkt-datasets"

BUCKET_NAME = "football-data-lake-22030108"

LEAGUES = {
    "GB1": "premier_league",
    "ES1": "la_liga",
    "L1": "bundesliga",
    "IT1": "serie_a",
    "FR1": "ligue_1"
}

# CLIENTE S3
s3 = boto3.client("s3")

# LEER CSVs
competitions = pd.read_csv(f"{BASE_PATH}/competitions.csv.gz", compression="gzip")
clubs = pd.read_csv(f"{BASE_PATH}/clubs.csv.gz", compression="gzip")
players = pd.read_csv(f"{BASE_PATH}/players.csv.gz", compression="gzip")
valuations = pd.read_csv(f"{BASE_PATH}/player_valuations.csv.gz", compression="gzip")


# FILTRAR COMPETITIONS
competitions_filtered = competitions[
    competitions["competition_code"].isin(LEAGUES.keys())
]


# SUBIR COMPETITIONS
for code, league_name in LEAGUES.items():

    print(f"\nProcesando {league_name}")


    # CLUBS
    clubs_filtered = clubs[
        clubs["domestic_competition_id"] == code
    ]

    club_ids = clubs_filtered["club_id"].tolist()


    # PLAYERS
    players_filtered = players[
        players["current_club_id"].isin(club_ids)
    ]

    player_ids = players_filtered["player_id"].tolist()

    # VALUATIONS
    valuations_filtered = valuations[
        valuations["player_id"].isin(player_ids)
    ]


    # SUBIR A S3
    datasets = {
        "clubs": clubs_filtered,
        "players": players_filtered,
        "player_valuations": valuations_filtered
    }

    for dataset_name, df in datasets.items():

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_key = (
            f"raw/transfermarkt/"
            f"{dataset_name}/"
            f"league={league_name}/"
            f"{dataset_name}.csv"
        )

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )

        print(f"Subido: {s3_key}")

print("\nProceso terminado.")

