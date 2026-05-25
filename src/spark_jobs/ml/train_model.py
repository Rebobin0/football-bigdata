import os
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import PROCESSED_ML_TRAINING_DATASET

def main():
    # 1. Iniciar sesión de Spark
    spark = create_spark_session("Entrenamiento de Modelo ML - Partidos")

    print(f"\nLeyendo datos de entrenamiento desde: {PROCESSED_ML_TRAINING_DATASET}")
    
    # 2. Leer el dataset preparado por tu compañero
    df = spark.read.parquet(PROCESSED_ML_TRAINING_DATASET)

    # 3. Limpieza: MLlib no soporta valores nulos en las características
    features_cols = [
        "home_squad_market_value_eur", 
        "away_squad_market_value_eur", 
        "market_value_diff_eur",
        "home_avg_player_market_value_eur",
        "away_avg_player_market_value_eur",
        "home_players_count",
        "away_players_count"
    ]
    
    df_clean = df.dropna(subset=features_cols + ["target"])

    # 4. Ensamblar las características en un solo vector
    assembler = VectorAssembler(
        inputCols=features_cols,
        outputCol="features"
    )
    
    df_assembled = assembler.transform(df_clean)

    # 5. Dividir los datos en entrenamiento (80%) y prueba (20%)
    train_data, test_data = df_assembled.randomSplit([0.8, 0.2], seed=42)

    print("\n=== ENTRENANDO MODELO (RANDOM FOREST) ===")
    # 6. Configurar y entrenar el modelo Random Forest
    rf = RandomForestClassifier(
        featuresCol="features", 
        labelCol="target", 
        numTrees=100, 
        maxDepth=5, 
        seed=42
    )
    
    model = rf.fit(train_data)

    # 7. Realizar predicciones con el set de prueba
    predictions = model.transform(test_data)

    print("\n=== EVALUACIÓN DEL MODELO ===")
    # 8. Evaluar precisión (Accuracy)
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="target", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = evaluator_acc.evaluate(predictions)
    
    # Evaluar F1 Score
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="target", predictionCol="prediction", metricName="f1"
    )
    f1_score = evaluator_f1.evaluate(predictions)

    print(f"Precisión (Accuracy): {accuracy:.4f}")
    print(f"F1 Score: {f1_score:.4f}")

    # 9. Mostrar la importancia de cada característica para ver qué influye más
    print("\n=== IMPORTANCIA DE LAS VARIABLES ===")
    importances = model.featureImportances
    for i, feature in enumerate(features_cols):
        print(f"{feature}: {importances[i]:.4f}")

    spark.stop()

if __name__ == "__main__":
    main()