import os
import pickle
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score

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
TARGET = "target"

DATA_PATH = "data/output/training_data.parquet"
MODEL_PATH = "data/output/model.pkl"


def main():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(
            f"No se encontró {DATA_PATH}. Ejecuta primero: python scripts/export_team_stats.py"
        )

    print(f"Leyendo datos de entrenamiento desde: {DATA_PATH}")
    df = pd.read_parquet(DATA_PATH)

    df_clean = df.dropna(subset=FEATURES + [TARGET])
    print(f"Filas disponibles para entrenamiento (sin NaN): {len(df_clean)}")

    X = df_clean[FEATURES]
    y = df_clean[TARGET].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(f"Train: {len(X_train)} filas | Test: {len(X_test)} filas")

    print("\nEntrenando Random Forest (100 árboles, max_depth=5)...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=5,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average="weighted")

    print(f"\n=== EVALUACIÓN ===")
    print(f"Accuracy : {acc:.4f}")
    print(f"F1 Score : {f1:.4f}")

    print("\n=== IMPORTANCIA DE FEATURES ===")
    for feat, imp in sorted(
        zip(FEATURES, model.feature_importances_), key=lambda x: -x[1]
    ):
        bar = "█" * int(imp * 40)
        print(f"  {feat:<42} {bar} {imp:.4f}")

    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    print(f"\nModelo guardado en: {MODEL_PATH}")


if __name__ == "__main__":
    main()
