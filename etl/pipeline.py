####################
#                  #
# Authored BY : me #
#                  #  
####################

import logging
import traceback

from spark_analysis import run_spark_analysis
from extract import extract_flights
from load import save_to_csv
from transform import clean_flights_data

def run_pipeline():
    logger = logging.getLogger("FlightRadarETL")
    logging.basicConfig(level=logging.INFO)

    try:
        logger.info("Lancement du pipeline ETL FlightRadar24")

        # 1. Extraction
        df = extract_flights()

        # 2. Nettoyage
        df_cleaned = clean_flights_data(df)

        # 3. Sauvegarde (CSV ou Parquet)
        path_saved = save_to_csv(df_cleaned)

        logger.info(f" Données sauvegardées dans : {path_saved}")

        # 4. Déclenchement de l’analyse Spark
        run_spark_analysis()

    except Exception as e:
        logger.error(f" Une erreur est survenue : {e}")
        traceback.print_exc()


# Pour tester la pipeline 

# run_pipeline()