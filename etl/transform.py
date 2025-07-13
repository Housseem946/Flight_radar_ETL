####################
#                  #
# Authored BY : me #
#                  #  
####################

# transform data ( EDA( exploration data analysis, data cleaning.. )

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def clean_flights_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Nettoyage des données de vol...")

    # Étape 1 : Supprimer les colonnes avec + de 50% de valeurs manquantes
    missing_ratio = (df.isna().sum() + (df == "").sum()) / len(df)
    cols_to_drop = missing_ratio[missing_ratio > 0.5].index.tolist()
    if cols_to_drop:
        logger.info(f"Colonnes supprimées (plus de 50% de valeurs manquantes) : {cols_to_drop}")
        df = df.drop(columns=cols_to_drop)

    # Étape 2 : Supprimer les lignes avec au moins une valeur manquante ou vide
    initial_row_count = len(df)
    df = df[~(df.isna() | (df == "")).any(axis=1)]
    removed_rows = initial_row_count - len(df)
    logger.info(f"Lignes supprimées pour valeurs manquantes : {removed_rows}")

    logger.info("Nettoyage terminé.")
    return df
