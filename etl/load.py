####################
#                  #
# Authored BY : me #
#                  #  
####################

# Load Data sous format parquet ( pour ....)

# etl/load.py
import os
from datetime import datetime, timezone

def save_to_parquet(df, base_path="Flights/rawzone"):
    now = datetime.now(timezone.utc)

    # Création du chemin horodaté
    path = os.path.join(
        base_path,
        f"tech_year={now.year}",
        f"tech_month={now.strftime('%Y-%m')}",
        f"tech_day={now.strftime('%Y-%m-%d')}"
    )
    os.makedirs(path, exist_ok=True)

    # Nom du fichier parquet
    filename = f"flights_{now.strftime('%Y%m%d%H%M%S')}.parquet"
    full_path = os.path.join(path, filename)

    # Sauvegarde en Parquet
    df.to_parquet(full_path, index=False)
    print(f"Fichier sauvegardé : {full_path}")
    return full_path
