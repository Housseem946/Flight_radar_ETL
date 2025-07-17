####################
#                  #
# Authored BY : me #
#                  #  
####################

# Fichier contenant toutes les fonctions utilséés dans le code du projet 

from math import radians, sin, cos, sqrt, atan2
import os


def haversine(lat1, lon1, lat2, lon2):
    if None in (lat1, lon1, lat2, lon2):
        return None
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def get_continent(lat, lon, zones_dict):
    for continent, zone in zones_dict.items():
        if all(k in zone for k in ['tl_y', 'tl_x', 'br_y', 'br_x']):
            if zone['br_y'] <= lat <= zone['tl_y'] and zone['tl_x'] <= lon <= zone['br_x']:
                return continent
    return None

def get_latest_csv_path(base_path="Flights/rawzone") -> str:
    """
    Parcourt récursivement les sous-dossiers pour trouver le fichier CSV le plus récent.
    """
    latest_time = None
    latest_file = None

    for root, _, files in os.walk(base_path):
        for file in files:
            if file.endswith(".csv"):
                full_path = os.path.join(root, file)
                file_time = os.path.getmtime(full_path)
                if not latest_time or file_time > latest_time:
                    latest_time = file_time
                    latest_file = full_path

    if latest_file:
        print(f"[INFO] Dernier fichier CSV trouvé : {latest_file}")
        return latest_file
    else:
        raise FileNotFoundError("Aucun fichier CSV trouvé dans le répertoire.")
