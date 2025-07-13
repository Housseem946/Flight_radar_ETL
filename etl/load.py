####################
#                  #
# Authored BY : me #
#                  #  
####################

# Load Data sous format parquet ( pour ....)

import os
from datetime import datetime, timezone
import pandas as pd

def save_to_csv(df: pd.DataFrame, base_path="Flights/rawzone") -> str:
    """
    Sauvegarde le DataFrame au format CSV avec une nomenclature horodatée.
    Exemple de chemin : Flights/rawzone/tech_year=2025/tech_month=2025-07/tech_day=2025-07-13/flights_20250713124500.csv
    """
    now = datetime.now(timezone.utc)

    # Création du chemin horodaté
    path = os.path.join(
        base_path,
        f"tech_year={now.year}",
        f"tech_month={now.strftime('%Y-%m')}",
        f"tech_day={now.strftime('%Y-%m-%d')}"
    )
    os.makedirs(path, exist_ok=True)

    # Nom du fichier CSV
    filename = f"flights_{now.strftime('%Y%m%d%H%M%S')}.csv"
    full_path = os.path.join(path, filename)

    # Sauvegarde en CSV
    df.to_csv(full_path, index=False)
    print(f"[INFO] Fichier CSV sauvegardé : {full_path}")

    return full_path
