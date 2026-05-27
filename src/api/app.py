import json
import pickle
from pathlib import Path

import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

FEATURES = [
    "home_squad_market_value_eur",
    "away_squad_market_value_eur",
    "market_value_diff_eur",
    "home_avg_player_market_value_eur",
    "away_avg_player_market_value_eur",
    "home_players_count",
    "away_players_count",
    "home_rank",
    "away_rank",
    "home_points",
    "away_points",
    "home_goal_diff",
    "away_goal_diff",
]
LABEL_MAP = {0: "Gana Local", 1: "Empate", 2: "Gana Visitante"}

BASE_DIR = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "output"
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

app = FastAPI(title="Football Predictions API")

model = None
team_stats: dict[str, dict] = {}
predictions: list[dict] = []


@app.on_event("startup")
def load_assets():
    global model, team_stats, predictions

    model_path = OUTPUT_DIR / "model.pkl"
    if model_path.exists():
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        print(f"Modelo cargado desde {model_path}")
    else:
        print(f"AVISO: model.pkl no encontrado. Ejecuta: python scripts/train_sklearn_model.py")

    team_stats_path = OUTPUT_DIR / "team_stats.json"
    if team_stats_path.exists():
        with open(team_stats_path) as f:
            teams_list = json.load(f)
        team_stats.update({t["team_name"]: t for t in teams_list})
        print(f"Cargados {len(team_stats)} equipos desde team_stats.json")
    else:
        print(f"AVISO: team_stats.json no encontrado. Ejecuta: python scripts/export_team_stats.py")

    pred_dir = OUTPUT_DIR / "predicciones_mongo"
    if pred_dir.exists():
        for pred_file in sorted(pred_dir.glob("part-*.json")):
            with open(pred_file) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        predictions.append(json.loads(line))
        print(f"Cargadas {len(predictions)} predicciones pre-generadas")


@app.get("/", response_class=HTMLResponse)
def index():
    return (TEMPLATES_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/api/predictions")
def get_predictions():
    return predictions


@app.get("/api/teams")
def get_teams():
    return [
        {"team_name": name, "league": data["league"]}
        for name, data in sorted(team_stats.items(), key=lambda x: x[0])
    ]


class PredictRequest(BaseModel):
    home_team: str
    away_team: str


@app.post("/api/predict")
def predict(req: PredictRequest):
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Modelo no cargado. Ejecuta: python scripts/train_sklearn_model.py",
        )

    home = team_stats.get(req.home_team)
    away = team_stats.get(req.away_team)

    if not home:
        raise HTTPException(status_code=404, detail=f"Equipo no encontrado: {req.home_team}")
    if not away:
        raise HTTPException(status_code=404, detail=f"Equipo no encontrado: {req.away_team}")

    def val(d, key):
        return d.get(key) or 0.0

    market_diff = val(home, "squad_market_value_eur") - val(away, "squad_market_value_eur")

    X = np.array([[
        val(home, "squad_market_value_eur"),
        val(away, "squad_market_value_eur"),
        market_diff,
        val(home, "avg_player_market_value_eur"),
        val(away, "avg_player_market_value_eur"),
        val(home, "players_count"),
        val(away, "players_count"),
        val(home, "rank"),
        val(away, "rank"),
        val(home, "points"),
        val(away, "points"),
        val(home, "goal_diff"),
        val(away, "goal_diff"),
    ]])

    proba = model.predict_proba(X)[0]
    pred = int(model.predict(X)[0])

    # predict_proba returns classes in sorted order (0, 1, 2)
    classes = list(model.classes_)
    prob_map = {int(c): round(float(p) * 100, 2) for c, p in zip(classes, proba)}

    return {
        "local": req.home_team,
        "visitante": req.away_team,
        "liga_local": home.get("league"),
        "liga_visitante": away.get("league"),
        "pronostico": LABEL_MAP[pred],
        "prob_gana_local": prob_map.get(0, 0.0),
        "prob_empate": prob_map.get(1, 0.0),
        "prob_gana_visitante": prob_map.get(2, 0.0),
    }
