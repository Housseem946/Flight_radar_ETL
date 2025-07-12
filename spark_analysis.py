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


##########################################################################################

import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, row_number, desc
from pyspark.sql.window import Window
from collections import defaultdict

#  Initialisation de la session Spark
spark = SparkSession.builder.appName("FlightAnalysis").getOrCreate()
print("✅ SparkSession créée avec succès")

# Chargement des données sous format parquet (manip plus rapide avvec spark)
df = spark.read.parquet("etl/Flights/rawzone/tech_year=2025/tech_month=2025-07/tech_day=2025-07-11/flights_20250711213100.parquet")

df.printSchema()
df.show(5)

#  Nettoyage : vols en cours avec infos géographiques et compagnie
clean_df = df.filter(
    (col("latitude").isNotNull()) &
    (col("longitude").isNotNull()) &
    (col("airline_iata").isNotNull()) &
    (col("on_ground") == 0)
)

# -------------------------------
# 1. Compagnie avec le plus de vols en cours
# -------------------------------
print("\n 1. Compagnie avec le plus de vols en cours :")
top_airline = (
    clean_df.filter((col("airline_iata").isNotNull()) & (col("airline_iata") != ""))
    .groupBy("airline_iata")
    .count()
    .orderBy(desc("count"))
    .first()
)
if top_airline:
    print(f"✈️ {top_airline['airline_iata']} avec {top_airline['count']} vols.")
else:
    print("❌ Données insuffisantes.")


# -------------------------------
# 2. Compagnie avec le plus de vols régionaux par continent
# -------------------------------
print("\n 2. Compagnie avec le plus de vols régionaux par continent :")
if "origin" in clean_df.columns and "destination" in clean_df.columns:
    regional_flights = clean_df.filter(
        (col("origin").isNotNull()) &
        (col("destination").isNotNull()) &
        (col("origin") == col("destination")) &
        (col("airline_iata").isNotNull()) & (col("airline_iata") != "")
    )
    window = Window.partitionBy("origin").orderBy(desc("count"))
    top_regional = (
        regional_flights.groupBy("origin", "airline_iata")
        .count()
        .withColumn("rank", row_number().over(window))
        .filter(col("rank") == 1)
        .collect()
    )
    for row in top_regional:
        print(f"🌍 {row['origin']} ➤ {row['airline_iata']} ({row['count']} vols)")
else:
    print("⚠️ Colonnes 'origin' ou 'destination' absentes.")

# -------------------------------
# 3. Vol avec le trajet le plus long
# -------------------------------
print("\n 3. Vol en cours avec le trajet le plus long :")

# distance_km difference entre destination et origin
if "distance_km" in clean_df.columns:
    longest_flight = (
        clean_df.filter(col("distance_km").isNotNull())
        .orderBy(desc("distance_km"))
        .select("callsign", "airline_iata", "distance_km")
        .first()
    )
    if longest_flight:
        print(f"📏 {longest_flight['callsign']} ({longest_flight['airline_iata']}) : {round(longest_flight['distance_km'], 2)} km")
elif "altitude" in clean_df.columns:
    longest_flight = (
        clean_df.filter(col("altitude").isNotNull())
        .orderBy(desc("altitude"))
        .select("callsign", "airline_iata", "altitude")
        .first()
    )
    if longest_flight:
        print(f"🛫 {longest_flight['callsign']} ({longest_flight['airline_iata']}) : {longest_flight['altitude']} pieds")
else:
    print("⚠️ Aucune donnée de distance ni d'altitude.")


# -------------------------------
# 4 . Pour chaque continent, la longueur de vol moyenne
# -------------------------------
iata_to_continent = {
    "AF": "Europe",     # Air France
    "LH": "Europe",     # Lufthansa
    "DL": "North America",  # Delta
    "EK": "Asia",       # Emirates (Moyen-Orient = Asie)
    "BA": "Europe",     # British Airways
    "NH": "Asia",       # All Nippon Airways
    "QF": "Oceania",    # Qantas
    # Ajoute ce dont tu as besoin
}

continent_df = spark.createDataFrame(
    [(k, v) for k, v in iata_to_continent.items()],
    ["airline_iata", "continent"]
)

flights_with_continent = clean_df.join(continent_df, on="airline_iata", how="left")

avg_altitude = (
    flights_with_continent
    .filter(col("altitude").isNotNull() & col("continent").isNotNull())
    .groupBy("continent")
    .agg(avg("altitude").alias("avg_altitude"))
    .orderBy("continent")
)

# Affichage clair
print("\n📌 Altitude moyenne des vols en cours par continent (approximation de la longueur) :")
for row in avg_altitude.collect():
    print(f"🌍 {row['continent']} ➤ {round(row['avg_altitude'], 2)} pieds")

# -------------------------------
# 5 . Constructeur avec le plus de vols actifs
# -------------------------------

print("\n 5. Constructeur d’avions avec le plus de vols actifs :")
if "aircraft_code" in clean_df.columns:
    top_manufacturer = (
        clean_df.filter((col("aircraft_code").isNotNull()) & (col("aircraft_code") != ""))
        .groupBy("aircraft_code")
        .count()
        .orderBy(desc("count"))
        .first()
    )
    if top_manufacturer:
        print(f"🛩️ {top_manufacturer['aircraft_code']} ({top_manufacturer['count']} vols)")
else:
    print("⚠️ Colonne 'aircraft_code' absente.")


# -------------------------------
# 6. Top 3 modèles par pays de la compagnie
# -------------------------------

# Il faut faire un mapping des noms de compagnie pour ajouter une nouvelle colonne country

iata_to_country = {
    "AF": "France",
    "LH": "Germany",
    "DL": "USA",
    "EK": "UAE",
    "BA": "UK",
    # ajoute d'autres si besoin...
}

mapping_df = spark.createDataFrame(
    [(k, v) for k, v in iata_to_country.items()],
    ["airline_iata", "country"]
)

enriched_df = clean_df.join(mapping_df, on="airline_iata", how="left")

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

print("\n📌 Top 3 modèles d’avion par pays de la compagnie :")

# On garde que les lignes valides
valid_flights = enriched_df.filter(
    col("country").isNotNull() & col("aircraft_code").isNotNull()
)

# Regrouper et compter
top_models = valid_flights.groupBy("country", "aircraft_code").count()

# Appliquer le classement
window = Window.partitionBy("country").orderBy(desc("count"))
ranked_models = top_models.withColumn("rank", row_number().over(window)) \
                          .filter(col("rank") <= 3) \
                          .orderBy("country", "rank")

# Afficher
grouped = ranked_models.collect()
from collections import defaultdict
result = defaultdict(list)
for row in grouped:
    result[row["country"]].append((row["aircraft_code"], row["count"]))

for country, models in result.items():
    models_str = ", ".join([f"{m} ({c} vols)" for m, c in models])
    print(f"🌍 {country} ➤ {models_str}")

