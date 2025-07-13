import logging
import datetime
import os
import subprocess

from extract import extract_flights
from transform import clean_flights_data
from load import save_to_parquet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # Génération d'un nom de fichier avec horodatage
        now = datetime.datetime.utcnow()
        date_path = now.strftime("tech_year=%Y/tech_month=%Y-%m/tech_day=%Y-%m-%d")
        filename = now.strftime("flights_%Y%m%d_%H%M.parquet")
        output_path = os.path.join("data", "rawzone", date_path)
        full_path = os.path.join(output_path, filename)

        os.makedirs(output_path, exist_ok=True)

        logger.info(" Étape 1 : Extraction des données...")


        raw_df = extract_flights()

        if raw_df.empty:
            logger.warning("Aucun vol récupéré. Le pipeline s'arrête ici.")
            return

        logger.info(" Étape 2 : Transformation...")
        clean_df = clean_flights_data(raw_df)

        logger.info(f" Étape 3 : Sauvegarde dans {full_path}")
        save_to_parquet(clean_df, full_path)

        logger.info(" Étape 4 : Lancement de l’analyse Spark...")
        subprocess.run(["python", "spark_analysis.py"], check=True)

        logger.info("✅ Pipeline exécuté avec succès.")

    except Exception as e:
        logger.exception("❌ Erreur critique dans le pipeline :")

if __name__ == "__main__":
    main()
