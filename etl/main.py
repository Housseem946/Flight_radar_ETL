# Fichier pour éxecuter les etl ( 3 fichiers(extract.py, transform.py, load.py)) : 
# --> output : Parquet(pour une meileure manipulation avec pyspark)

from extract import extract_flights
from transform import clean_flights_data
from load import save_to_parquet
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        raw_flights = extract_flights()
        if raw_flights.empty:
            logger.warning("Aucun vol récupéré.")
        else:
            cleaned_df = clean_flights_data(raw_flights)
            logger.info(f"{len(cleaned_df)} vols après nettoyage.")
            save_to_parquet(cleaned_df)


    except Exception as e:
        logger.error(f"Erreur critique dans le pipeline : {e}")

