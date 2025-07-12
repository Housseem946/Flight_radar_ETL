# ####################
# #                  #
# # Authored BY : me #
# #                  #  
# ####################

# # fichier spark contenant les transformations demandées 

# For my own use 
# os.environ["JAVA_HOME"] = r"C:\Users\houss\AppData\Local\Programs\Eclipse Adoptium\jdk-11.0.26.4-hotspot"
# os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, avg, desc, length, expr
# from datetime import datetime
# import os

# def get_latest_parquet(base_path="Flights/rawzone"):
#     """
#     Retourne le chemin du fichier parquet le plus récent.
#     """
#     folders = []
#     for root, dirs, files in os.walk(base_path):
#         for file in files:
#             if file.endswith(".parquet"):
#                 folders.append(os.path.join(root, file))
#     return max(folders) if folders else None


# def run_spark_kpis():
#     spark = SparkSession.builder \
#         .appName("FlightRadar KPIs") \
#         .getOrCreate()

#     file_path = get_latest_parquet()
#     if not file_path:
#         print("Aucun fichier parquet trouvé.")
#         return

#     df = spark.read.parquet(file_path)
#     df.cache()

#     # 📊 KPI 1 : La compagnie avec le + de vols en cours
#     df.groupBy("airline_icao").count().orderBy(desc("count")).show(1, truncate=False)

#     # 📊 KPI 2 : Le vol en cours avec le trajet le plus long (si tu as distance calculée)
#     # Ex: rajoute un champ 'distance' dans ta phase de transformation si tu veux faire ça proprement
#     # Sinon on peut calculer la distance avec Haversine dans Pandas, puis charger le champ ici

#     # 📊 KPI 3 : Moyenne de longueur des vols par continent
#     # Il te faut une table de mapping IATA ↔ continent pour les aéroports

#     # 📊 KPI 4 : Constructeur d'avion avec le + de vols actifs
#     # Il te faut un mapping registration ↔ manufacturer ↔ model
#     # Tu peux enrichir les données avec OpenSky, Planespotters ou autres sources (ex: CSV externe)

#     # 📊 KPI 5 : Pour chaque pays de la compagnie, top 3 modèles d’avion
#     # Requiert mapping ICAO compagnie ↔ pays + registration ↔ modèle
#     # Exemple si enrichi :
#     # df.groupBy("pays_compagnie", "modele_avion").agg(count("*").alias("nb")).orderBy(desc("nb")).show()

#     spark.stop()


# from pyspark.sql import SparkSession
# import os
# spark = SparkSession.builder \
#     .appName("FlightRadarAnalysis") \
#     .getOrCreate()
# path = os.path.join("etl", "Flights", "rawzone", "tech_year=2025", "tech_month=2025-07", "tech_day=2025-07-11", "flights_20250711213100.parquet")

# df = spark.read.parquet(path)
# df.printSchema()
# df.show(5)

##########################################################################################

# ============================================
# Analyse de trafic aérien avec PySpark
# ============================================

import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, row_number
from pyspark.sql.window import Window
from collections import defaultdict

# --------------------------------------------
# 1. Initialisation de la session Spark
# --------------------------------------------

spark = SparkSession.builder.appName("FlightAnalysis").getOrCreate()
print("✅ SparkSession créée avec succès")

# --------------------------------------------
# 2. Chargement des données
# --------------------------------------------

df = spark.read.parquet("etl/Flights/rawzone/tech_year=2025/tech_month=2025-07/tech_day=2025-07-11/flights_20250711213100.parquet")

# --------------------------------------------
# 3. Nettoyage des données
# --------------------------------------------

clean_df = df.filter(
    (col("latitude").isNotNull()) &
    (col("longitude").isNotNull()) &
    (col("airline_iata").isNotNull()) &
    (col("on_ground") == 0)  # vols en cours uniquement
)

# --------------------------------------------
# 4. INDICATEUR 1 : Compagnie avec le plus de vols en cours
# --------------------------------------------

top_airline = clean_df.groupBy("airline_iata").count().orderBy("count", ascending=False).first()
if top_airline:
    print(f"➡️ La compagnie avec le plus de vols en cours est : {top_airline['airline_iata']} avec {top_airline['count']} vols.")

# --------------------------------------------
# 5. INDICATEUR 2 : Compagnie avec le plus de vols régionaux par continent
# (NB: nécessite des colonnes origin_continent et destination_continent)
# --------------------------------------------

if "origin_continent" in clean_df.columns and "destination_continent" in clean_df.columns:
    regional_flights = clean_df.filter(col("origin_continent") == col("destination_continent"))

    window = Window.partitionBy("origin_continent").orderBy(col("count").desc())

    top_regional = regional_flights.groupBy("origin_continent", "airline_iata") \
        .count() \
        .withColumn("rank", row_number().over(window)) \
        .filter(col("rank") == 1) \
        .collect()

    for row in top_regional:
        print(f"➡️ Dans le continent {row['origin_continent']}, la compagnie {row['airline_iata']} a le plus de vols régionaux actifs ({row['count']} vols).")
else:
    print("⚠️ Les colonnes 'origin_continent' et 'destination_continent' sont absentes.")

# --------------------------------------------
# 6. INDICATEUR 3 : Le vol avec le trajet le plus long (basé sur l’altitude si pas de distance)
# --------------------------------------------

if "distance_km" in clean_df.columns:
    longest_flight = clean_df.orderBy(col("distance_km").desc()).select("callsign", "airline_iata", "distance_km").first()
    if longest_flight:
        print(f"➡️ Le vol le plus long est {longest_flight['callsign']} ({longest_flight['airline_iata']}) avec {round(longest_flight['distance_km'], 2)} km.")
elif "altitude" in clean_df.columns:
    longest_flight = clean_df.orderBy(col("altitude").desc()).select("callsign", "airline_iata", "altitude").first()
    if longest_flight:
        print(f"➡️ Le vol le plus haut est {longest_flight['callsign']} ({longest_flight['airline_iata']}) avec {longest_flight['altitude']} pieds.")
else:
    print("⚠️ Pas de colonne 'distance_km' ni 'altitude' pour mesurer la longueur de vol.")

# --------------------------------------------
# 7. INDICATEUR 4 : Longueur moyenne des vols par continent
# --------------------------------------------

if "origin_continent" in clean_df.columns and "distance_km" in clean_df.columns:
    avg_distances = clean_df.groupBy("origin_continent").agg(avg("distance_km").alias("avg_distance_km")).collect()
    for row in avg_distances:
        print(f"➡️ Dans le continent {row['origin_continent']}, la distance moyenne des vols est de {round(row['avg_distance_km'], 2)} km.")
else:
    print("⚠️ Les colonnes nécessaires pour calculer la longueur moyenne des vols sont manquantes.")

# --------------------------------------------
# 8. INDICATEUR 5 : Constructeur d’avions avec le plus de vols actifs
# --------------------------------------------

if "manufacturer" in clean_df.columns:
    top_manufacturer = clean_df.groupBy("manufacturer").count().orderBy("count", ascending=False).first()
    if top_manufacturer:
        print(f"➡️ Le constructeur avec le plus de vols actifs est : {top_manufacturer['manufacturer']} ({top_manufacturer['count']} vols).")
else:
    print("⚠️ La colonne 'manufacturer' est absente.")

# --------------------------------------------
# 9. INDICATEUR 6 : Top 3 modèles d’avion par pays de la compagnie
# --------------------------------------------

if "country" in clean_df.columns and "aircraft_code" in clean_df.columns:
    window2 = Window.partitionBy("country").orderBy(col("count").desc())

    top_models = clean_df.groupBy("country", "aircraft_code").count() \
        .withColumn("rank", row_number().over(window2)) \
        .filter(col("rank") <= 3) \
        .orderBy("country", "rank") \
        .collect()

    result = defaultdict(list)
    for row in top_models:
        result[row["country"]].append((row["aircraft_code"], row["count"]))

    for country, models in result.items():
        models_str = ", ".join([f"{model} ({count} vols)" for model, count in models])
        print(f"➡️ Dans le pays {country}, les 3 modèles d’avion les plus utilisés sont : {models_str}.")
else:
    print("⚠️ Les colonnes 'country' et 'aircraft_code' sont absentes.")

# --------------------------------------------
# Fin de l’analyse
# --------------------------------------------



