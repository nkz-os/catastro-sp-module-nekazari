
import logging
import sys
import json
import argparse
from app.catastro_clients import SpanishStateCatastroClient, NavarraCatastroClient, EuskadiCatastroClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_spain():
    print("\n--- Testing Spanish Context (Madrid) ---")
    lon, lat = -3.7038, 40.4168
    client = SpanishStateCatastroClient()
    try:
        # Pass srs as string "4326"
        result = client.query_by_coordinates(lon, lat, "4326")
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        logger.error(f"Spain error: {e}")

def test_euskadi():
    print("\n--- Testing Euskadi Context (User Provided) ---")
    # Coordinates provided by user: lat: 43.25302012597231, lon: -2.177476767784858
    lat, lon = 43.25302012597231, -2.177476767784858
    client = EuskadiCatastroClient()
    try:
        result = client.query_by_coordinates(lon, lat, "4326")
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        logger.error(f"Euskadi error: {e}")

def test_navarra():
    print("\n--- Testing Navarra Context (Pamplona) ---")
    lon, lat = -1.6432, 42.8169
    client = NavarraCatastroClient()
    try:
        result = client.query_by_coordinates(lon, lat, "4326")
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        logger.error(f"Navarra error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('region', choices=['spain', 'euskadi', 'navarra'])
    args = parser.parse_args()
    
    if args.region == 'spain':
        test_spain()
    elif args.region == 'euskadi':
        test_euskadi()
    elif args.region == 'navarra':
        test_navarra()
