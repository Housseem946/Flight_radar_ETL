import logging
import pandas as pd
from extract import extract_flights, save_to_csv
from transform import clean_flights_data
from datetime import datetime, timezone
import glob
from load import save_to_parquet


# Setup des logs
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Extract function
def extract_data():

    logger.info("Début de l'extraction des données")
    df = extract_flights()

    if not df.empty:
        output_path = save_to_csv(df)
        logger.info(f"Fichier CSV sauvegardé : {output_path}")
        return output_path
    else:
        logger.warning("Aucune donnée de vol extraite.")
        return None

# transform function 
def transform_data():
    
    logger.info("Début de la transformation")
    
    # On va chercher le dernier fichier CSV ajouté dans le dossier horodaté
    now = datetime.now(timezone.utc)
    folder = f"data/rawzone/tech_year={now.year}/tech_month={now.strftime('%Y-%m')}/tech_day={now.strftime('%Y-%m-%d')}"
    files = sorted(glob.glob(f"{folder}/flights*.csv"), reverse=True)
    if not files:
        logger.warning("Aucun fichier CSV trouvé pour aujourd’hui.")
        return None

    df = pd.read_csv(files[0])
    df_clean = clean_flights_data(df)
    logger.info(f"Transformation terminée, {len(df_clean)} lignes conservées")
    return df_clean


# Load function 
def load_data():
    

    logger.info("Début du chargement des données")
    df = transform_data()

    if df is not None and not df.empty:
        output_path = save_to_parquet(df)
        logger.info(f"Fichier Parquet généré : {output_path}")
        return output_path
    else:
        logger.warning("Aucune donnée nettoyée à charger.")
        return None
