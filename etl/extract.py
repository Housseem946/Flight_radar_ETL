####################
#                  #
# Authored BY : me #
#                  #  
####################

# Extract data from flightRadar api sous format csv 

import pandas as pd
import logging
from datetime import datetime, timezone
import os
from FlightRadar24.api import FlightRadar24API 


# Setup des logs
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def extract_flights():
    api = FlightRadar24API()
    flights = api.get_flights()

    logger.info(f"{len(flights)} vols récupérés")

    data = []
    for flight in flights:
        try:
            data.append({
                "id": flight.id,
                "callsign": flight.callsign,
                "airline_iata": flight.airline_iata,
                "airline_icao": flight.airline_icao,
                "origin": flight.origin_airport_iata,
                "destination": flight.destination_airport_iata,
                "aircraft_code": flight.aircraft_code,
                "registration": flight.registration,
                "latitude": flight.latitude,
                "longitude": flight.longitude,
                "altitude": flight.altitude,
                "ground_speed": flight.ground_speed,
                "heading": flight.heading,
                "vertical_speed": flight.vertical_speed,
                "on_ground": flight.on_ground,
                "squawk": flight.squawk,
                "time": flight.time
            })
        except Exception as e:
            logger.warning(f"Vol ignoré à cause d'une erreur : {e}")

    return pd.DataFrame(data)


# En respectant la nomenclature hordatée demandée 

def save_to_csv(df: pd.DataFrame):
    now = datetime.now(timezone.utc)
    folder_path = f"data/rawzone/tech_year={now.year}/tech_month={now.strftime('%Y-%m')}/tech_day={now.strftime('%Y-%m-%d')}"
    os.makedirs(folder_path, exist_ok=True)
    file_path = f"{folder_path}/flights{now.strftime('%Y%m%d%H%M%S')}.csv"
    df.to_csv(file_path, index=False)
    logger.info(f"Données sauvegardées dans {file_path}")
    return file_path

if __name__ == "__main__":
    df = extract_flights()
    print("df head \n",df.head(5))
    if not df.empty:
        save_to_csv(df)
    else:
        logger.warning("Aucune donnée de vol extraite.")
