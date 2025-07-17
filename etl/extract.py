####################
#                  #
# Authored BY : me #
#                  #
####################

# Extract data from flightRadar api sous format dataframe

import pandas as pd
import logging
from FlightRadar24 import FlightRadar24API

from fonctions import get_continent, haversine

# Setup du logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def extract_flights():
    api = FlightRadar24API()

    logger.info(" Récupération des vols en cours...")
    flights = api.get_flights()
    logger.info(f" {len(flights)} vols récupérés.")

    logger.info(" Récupération des IATA pour enrichissement...")
    iata_codes = set()
    for f in flights:
        if f.origin_airport_iata:
            iata_codes.add(f.origin_airport_iata)
        if f.destination_airport_iata:
            iata_codes.add(f.destination_airport_iata)

    logger.info(f" {len(iata_codes)} codes IATA collectés. Enrichissement des aéroports...")
    airport_info = {}
    for code in list(iata_codes):
        try:
            a = api.get_airport(code)
            airport_info[code] = {
                "iata": code,
                "latitude": a.latitude,
                "longitude": a.longitude,
                "name": a.name,
                "country": a.country,
            }
        except Exception:
            continue

    logger.info(f"{len(airport_info)} aéroports enrichis.")

    logger.info(" Chargement des zones géographiques...")
    zones = api.get_zones()

    logger.info(" Construction du DataFrame enrichi...")
    enriched_flights = []

    for f in flights:
        origin = airport_info.get(f.origin_airport_iata)
        dest = airport_info.get(f.destination_airport_iata)

        if not origin or not dest:
            continue

        origin_continent = get_continent(origin["latitude"], origin["longitude"], zones)
        dest_continent = get_continent(dest["latitude"], dest["longitude"], zones)

        distance_km = haversine(
            origin["latitude"], origin["longitude"],
            dest["latitude"], dest["longitude"]
        )

        enriched_flights.append({
            "flight_id": f.id,
            "callsign": f.callsign,
            "airline_icao": f.airline_icao,
            "origin_iata": f.origin_airport_iata,
            "dest_iata": f.destination_airport_iata,
            "origin_country": origin["country"],
            "dest_country": dest["country"],
            "origin_continent": origin_continent,
            "dest_continent": dest_continent,
            "distance_km": distance_km,
            "aircraft_code": f.aircraft_code,
            "registration": f.registration,
        })

    df = pd.DataFrame(enriched_flights)
    logger.info(f" DataFrame final contenant {len(df)} lignes.")

    return df
