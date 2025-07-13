# ####################
# #                  #
# # Authored BY : me #
# #                  #  
# ####################

# # fichier spark contenant les transformations demandées 

# For my own use 
# os.environ["JAVA_HOME"] = r"C:\Users\houss\AppData\Local\Programs\Eclipse Adoptium\jdk-11.0.26.4-hotspot"
# os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")

##########################################################################################

import glob
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, row_number, desc
from pyspark.sql.window import Window

def run_spark_analysis():
    print("🚀 Début de l’analyse Spark")

    spark = SparkSession.builder.appName("FlightAnalysis").getOrCreate()

    # Chercher le dernier fichier CSV/Parquet dans la rawzone
    files = glob.glob("Flights/rawzone/tech_year=*/tech_month=*/tech_day=*/flights_*.csv")
    if not files:
        raise FileNotFoundError("Aucun fichier trouvé dans la rawzone.")

    latest_file = sorted(files)[-1]
    print(f" Chargement du fichier : {latest_file}")
    df = spark.read.csv(latest_file, header=True, inferSchema=True)

    # Nettoyage minimal
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
        print(" Données insuffisantes.")


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
            print(f" {row['origin']} ➤ {row['airline_iata']} ({row['count']} vols)")
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
            print(f" {longest_flight['callsign']} ({longest_flight['airline_iata']}) : {round(longest_flight['distance_km'], 2)} km")
    elif "altitude" in clean_df.columns:
        longest_flight = (
            clean_df.filter(col("altitude").isNotNull())
            .orderBy(desc("altitude"))
            .select("callsign", "airline_iata", "altitude")
            .first()
        )
        if longest_flight:
            print(f" {longest_flight['callsign']} ({longest_flight['airline_iata']}) : {longest_flight['altitude']} pieds")
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
    print("\n 4. Altitude moyenne des vols en cours par continent (approximation de la longueur) :")
    for row in avg_altitude.collect():
        print(f" {row['continent']} ➤ {round(row['avg_altitude'], 2)} pieds")


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
            print(f" {top_manufacturer['aircraft_code']} ({top_manufacturer['count']} vols)")
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

    print("\n 6. Top 3 modèles d’avion par pays de la compagnie :")

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

