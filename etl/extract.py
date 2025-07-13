####################
#                  #
# Authored BY : me #
#                  #  
####################

# Extract data from flightRadar api sous format dataframe

import pandas as pd
import logging
from datetime import datetime, timezone
import os
from FlightRadar24 import FlightRadar24API

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

#df = extract_flights()