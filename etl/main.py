# Fichier pour éxecuter les elt 

from extract import extract_flights
from transform import clean_flights_data
from load import save_to_parquet
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    flights = extract_flights()

    df_clean = clean_flights_data(flights)
    logger.info(f"{len(df_clean)} vols après nettoyage")
    # if not flights:
    #     logger.warning("Aucune donnée extraite")
    # else:
        # df_clean = clean_flights_data(flights)
        # logger.info(f"{len(df_clean)} vols après nettoyage")
        # save_to_parquet(df_clean)
        # logger.info("Données sauvegardées avec succès.")
