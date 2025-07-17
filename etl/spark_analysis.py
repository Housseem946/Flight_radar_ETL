# ####################
# #                  #
# # Authored BY : me #
# #                  #  
# ####################

# # fichier spark contenant les transformations

from itertools import count
import findspark
findspark.init()

from fonctions import get_latest_csv_path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, count, desc, row_number, when

def run_spark_analysis():
    spark = SparkSession.builder.appName("FlightRadar24 Analysis").getOrCreate()

    csv_path = get_latest_csv_path()
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
    df.cache()

    print(" Début de l’analyse Spark...\n")

    # -------------------------------
    # 1. Compagnie avec le plus de vols en cours
    # -------------------------------
    df1 = df.groupBy("airline_icao").agg(count("*").alias("nb_vols")).orderBy(desc("nb_vols"))
    top_airline = df1.limit(1)
    print("1. Compagnie avec le plus de vols en cours :")
    top_airline.show(truncate=False)

    # -------------------------------
    # 2. Compagnie avec le plus de vols régionaux par continent
    # -------------------------------
    regional_df = df.filter(
        (col("origin_continent").isNotNull()) &
        (col("dest_continent").isNotNull()) &
        (col("origin_continent") == col("dest_continent")) &
        (col("airline_icao").isNotNull()) &
        (col("airline_icao") != "")
    )

    regional_counts = regional_df.groupBy("origin_continent", "airline_icao") \
                                 .agg(count("*").alias("nb_vols"))

    region_window = Window.partitionBy("origin_continent").orderBy(desc("nb_vols"))
    top_regionals = regional_counts.withColumn("rank", row_number().over(region_window)) \
                                   .filter(col("rank") == 1) \
                                   .drop("rank")

    print("2. Compagnie avec le plus de vols régionaux par continent :")
    top_regionals.show(truncate=False)

    # -------------------------------
    # 3. Vol avec le trajet le plus long
    # -------------------------------
    longest_flight = df.filter(col("distance_km").isNotNull()) \
                       .orderBy(desc("distance_km")) \
                       .limit(1)

    print("3. Vol avec le trajet le plus long :")
    longest_flight.show(truncate=False)

    # -------------------------------
    # 4. Distance moyenne par continent
    # -------------------------------
    avg_distance = df.filter(col("distance_km").isNotNull()) \
                     .groupBy("origin_continent") \
                     .agg(avg("distance_km").alias("avg_distance_km")) \
                     .orderBy("origin_continent")

    print("4. Distance moyenne des vols par continent :")
    avg_distance.show(truncate=False)

    # -------------------------------
    # 5. Constructeur avec le plus de vols actifs
    # -------------------------------
    constructor_map = {
        "A3": "Airbus", "A2": "Airbus",
        "B7": "Boeing", "B8": "Boeing",
        "E1": "Embraer", "E2": "Embraer", "EMB": "Embraer",
        "CRJ": "Bombardier", "C": "Bombardier", "DH": "Bombardier",
        "AT": "ATR",
        "DC": "McDonnell Douglas", "MD": "McDonnell Douglas",
        "G": "Gulfstream", "F": "Dassault",
        "C5": "Cessna", "C6": "Cessna",
        "SU": "Sukhoi", "C9": "COMAC", "MRJ": "Mitsubishi",
        "TU": "Tupolev", "IL": "Ilyushin", "AN": "Antonov"
    }

    constructor_expr = None
    for prefix, name in constructor_map.items():
        condition = col("aircraft_code").startswith(prefix)
        constructor_expr = when(condition, name) if constructor_expr is None else constructor_expr.when(condition, name)
    constructor_expr = constructor_expr.otherwise("Inconnu")

    df = df.withColumn("constructeur", constructor_expr)

    constructeur_top = df.groupBy("constructeur").count().orderBy(col("count").desc()).limit(1)

    print("5. Constructeur avec le plus de vols actifs :")
    constructeur_top.show(truncate=False)

    # -------------------------------
    # 6. Top 3 modèles d’avion par pays de la compagnie
    # -------------------------------
    if "origin_country" in df.columns and "aircraft_code" in df.columns:
        model_counts = df.filter(
            col("origin_country").isNotNull() & col("aircraft_code").isNotNull()
        ).groupBy("origin_country", "aircraft_code") \
         .agg(count("*").alias("nb_vols"))

        window = Window.partitionBy("origin_country").orderBy(desc("nb_vols"))
        top_models = model_counts.withColumn("rank", row_number().over(window)) \
                                 .filter(col("rank") <= 3) \
                                 .select("origin_country", "aircraft_code", "nb_vols", "rank") \
                                 .orderBy("origin_country", "rank")

        print("6. Top 3 modèles d’avion en usage par pays de compagnie :")
        top_models.show(100, truncate=False)
    else:
        print(" Colonnes nécessaires absentes pour KPI 6.")

    spark.stop()
