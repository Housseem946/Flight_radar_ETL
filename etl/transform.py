####################
#                  #
# Authored BY : me #
#                  #  
####################

# Extract data from flightRadar api sous format csv 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

def transform_data(csv_path):
    spark = SparkSession.builder \
        .appName("FlightRadar - Analyse des vols") \
        .getOrCreate()

    # Lire le CSV
    df = spark.read.option("header", "true").csv(csv_path)

    df.printSchema()
    df.show(10, truncate=False)

    # ✅ Exemple d’indicateur 1 : nombre de vols par compagnie (IATA)
    vols_par_compagnie = df.groupBy("airline_iata") \
        .agg(count("*").alias("nombre_vols")) \
        .orderBy(desc("nombre_vols"))

    vols_par_compagnie.show()

    # Tu peux retourner les résultats ou les sauvegarder
    return vols_par_compagnie

if __name__ == "__main__":
    # Mets ici le chemin du fichier CSV généré dans `extract.py`
    csv_path = "data/rawzone/tech_year=2025/tech_month=2025-07/tech_day=2025-07-11/flights_20250711_XXXXXX.csv"
    transform_data(csv_path)
