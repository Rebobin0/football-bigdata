import requests
import boto3
import json
import time
from datetime import datetime

# CONFIGURACIÓN
API_KEY = "ecfa6805b906b8f5e79b1e0dd8381d03"
BASE_URL = "https://v3.football.api-sports.io/fixtures"

HEADERS = {
    "x-apisports-key": API_KEY
}

BUCKET_NAME = "football-data-lake-22030108"

LEAGUE_ID = 262
LEAGUE_NAME = "liga_mx"
SEASON = 2024


# CLIENTE S3
s3 = boto3.client("s3")

# FUNCIÓN DE INGESTA
def fetch_fixtures():
    params = {
        "league": LEAGUE_ID,
        "season": SEASON,
        "from": f"{SEASON}-01-01",
        "to": f"{SEASON}-12-31"
    }

    response = requests.get(BASE_URL, headers=HEADERS, params=params)

    if response.status_code == 429:
        print("Rate limit. Esperando 60s...")
        time.sleep(60)
        return fetch_fixtures()

    if response.status_code != 200:
        print(f"Error API: {response.status_code}")
        return []

    data = response.json()

    if "errors" in data and data["errors"]:
        print("Error API interno:", data["errors"])
        return []

    if not data.get("response"):
        print("No hay datos")
        return []

    print(f"Registros obtenidos: {len(data['response'])}")

    return data["response"]


# SUBIR A S3
def upload_to_s3(data):
    today = datetime.now().strftime("%Y-%m-%d")

    key = f"raw/api_football/league={LEAGUE_NAME}/season={SEASON}/{today}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Archivo subido a S3: {key}")


# MAIN
def main():
    print("Iniciando ingesta LaLiga 2023...")

    data = fetch_fixtures()

    if len(data) == 0:
        print("No se obtuvieron datos")
        return

    print(f"Total registros: {len(data)}")

    upload_to_s3(data)

    print("Proceso completado")

if __name__ == "__main__":
    main()