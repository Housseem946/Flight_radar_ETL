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

    ##### let's do some data exploration to understand our data, so that we can do the data cleaning 

    # Aperçu général

    print("shape \n",df.shape)

    print (" \n")
    
    print("head of df \n",df.head())
    print (" \n")


    print("Columns \n",df.columns)
    print (" \n")


    print("types \n", df.dtypes)
    print (" \n")

    # Valeurs manquantes

    print("Valeurs manquantes en % \n",(df.isnull().sum() / len(df)).sort_values(ascending=False))
    print (" \n")


    # Valeurs uniques

    print("Valeurs unique \n",df.nunique().sort_values(ascending=False))
    print (" \n")

    # Statistiques descriptives

    print("Statistiques descriptives \n", df.describe(include='all'))
    print (" \n")

    # check for Duplicats

    print("duplicates \n",df.duplicated(subset=['id']).sum())

    # Répartition des compagnies 

    print("Répartition des compagnies   \n", df["airline_icao"].value_counts().head(10))
    print (" \n")

    # Répartition des origines :
    
    df["destination"].value_counts().head(10)

    print("Répartition des origines   \n", df["origin"].value_counts().head(10))
    print (" \n")

    # Répartition des destinations :
    
    print("Répartition des destinations   \n", df["destination"].value_counts().head(10))
    print (" \n")

    # Suppression des lignes incomplètes ( dans le cas ou il ya des donnes manquantes)
    df.dropna(subset=["origin", "destination", "airline_icao"], inplace=True)

    # Suppression des doublons
    df.drop_duplicates(subset=["id"], inplace=True)

    # Conversion de types
    df["altitude"] = pd.to_numeric(df["altitude"], errors="coerce")
    df["ground_speed"] = pd.to_numeric(df["ground_speed"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")


    logger.info("Nettoyage terminé.")
    return df