import requests
import boto3
import json
import time
from datetime import datetime

# CONFIGURACIÓN
API_KEY = "ecfa6805b906b8f5e79b1e0dd8381d03"
BASE_URL = "https://v3.football.api-sports.io/standings"

HEADERS = {
    "x-apisports-key": API_KEY
}

BUCKET_NAME = "football-data-lake-22030108"

# Ligas
LEAGUES = {
    140: "la_liga",
    39: "premier_league",
    78: "bundesliga",
    135: "serie_a",
    61: "ligue_1",
    262: "liga_mx"
}

# Temporadas
SEASONS = [2023, 2024]

# CLIENTE S3
s3 = boto3.client("s3")

# FUNCIÓN DE INGESTA
def fetch_standings(league_id, season):
    params = {
        "league": league_id,
        "season": season
    }

    response = requests.get(BASE_URL, headers=HEADERS, params=params)

    # Rate limit
    if response.status_code == 429:
        print(" Rate limit alcanzado. Esperando 60s...")
        time.sleep(60)
        return fetch_standings(league_id, season)

    if response.status_code != 200:
        print(f" Error API {league_id}-{season}: {response.status_code}")
        return []

    data = response.json()

    if "errors" in data and data["errors"]:
        print(f" Error API interno {league_id}-{season}: {data['errors']}")
        return []

    if not data.get("response"):
        print(f"Sin datos: league={league_id}, season={season}")
        return []

    # Aplanar standings
    standings = data["response"][0]["league"]["standings"]
    flat = [team for group in standings for team in group]
    print(f"{league_id}-{season}: {len(flat)} equipos")
    return flat

# SUBIR A S3
def upload_to_s3(data, league_name, season):
    today = datetime.now().strftime("%Y-%m-%d")
    key = f"raw/api_football/standings/league={league_name}/season={season}/{today}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f" Subido: {key}")


# MAIN
def main():
    print(" Iniciando ingesta STANDINGS multi-liga...")

    for league_id, league_name in LEAGUES.items():
        for season in SEASONS:
            print(f"\n Procesando {league_name} - {season}")
            data = fetch_standings(league_id, season)
            if len(data) == 0:
                print("⏭ Saltando")
                continue

            upload_to_s3(data, league_name, season)
            time.sleep(2)

    print("\n Ingesta completada")

if __name__ == "__main__":
    main()
