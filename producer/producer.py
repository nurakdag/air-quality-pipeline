import os
import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "air_quality_raw")
OPENAQ_API_KEY  = os.getenv("OPENAQ_API_KEY")
POLL_INTERVAL   = 900
BASE_URL        = "https://api.openaq.org/v3"

HEADERS = {
    "X-API-Key": OPENAQ_API_KEY,
    "Accept": "application/json"
}

TARGET_COUNTRIES = ["TR", "GB", "IN", "CN"]  

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_country_ids() -> dict:
    """ISO code → numeric ID mapping"""
    resp = requests.get(
        f"{BASE_URL}/countries",
        params={"limit": 200},
        headers=HEADERS,
        timeout=15
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])
    
    mapping = {}
    for c in results:
        if c.get("code") in TARGET_COUNTRIES:
            mapping[c["code"]] = c["id"]
            print(f"  {c['code']} ({c.get('name')}) → ID: {c['id']}")
    return mapping

def get_locations(country_numeric_id: int) -> list:
    resp = requests.get(
        f"{BASE_URL}/locations",
        params={"countries_id": country_numeric_id, "limit": 10},
        headers=HEADERS,
        timeout=15
    )
    resp.raise_for_status()
    return resp.json().get("results", [])

def get_sensors(location_id: int) -> list:
    resp = requests.get(
        f"{BASE_URL}/locations/{location_id}/sensors",
        headers=HEADERS,
        timeout=15
    )
    resp.raise_for_status()
    return resp.json().get("results", [])

def get_measurements(sensor_id: int) -> list:
    resp = requests.get(
        f"{BASE_URL}/sensors/{sensor_id}/measurements",
        params={"limit": 5},
        headers=HEADERS,
        timeout=15
    )
    resp.raise_for_status()
    return resp.json().get("results", [])

def run():
    print("Country ID'leri alınıyor...")
    country_map = get_country_ids()
    print(f"Bulunan ülkeler: {country_map}\n")

    print(f"Producer başladı | Topic: {KAFKA_TOPIC}")
    while True:
        for iso_code, country_id in country_map.items():
            try:
                locations = get_locations(country_id)
                msg_count = 0

                for location in locations:
                    loc_id = location["id"]
                    sensors = get_sensors(loc_id)

                    for sensor in sensors:
                        param = sensor.get("parameter", {}).get("name", "")
                        if param not in ["pm25", "pm10", "no2", "o3"]:
                            continue

                        measurements = get_measurements(sensor["id"])
                        for m in measurements:
                            msg = {
                                "ingested_at":   datetime.now(timezone.utc).isoformat(),
                                "country_code":  iso_code,
                                "location_id":   loc_id,
                                "location_name": location.get("name"),
                                "latitude":      location.get("coordinates", {}).get("latitude"),
                                "longitude":     location.get("coordinates", {}).get("longitude"),
                                "sensor_id":     sensor["id"],
                                "parameter":     param,
                                "value":         m.get("value"),
                                "unit":          sensor.get("parameter", {}).get("units"),
                                "measured_at":   m.get("period", {}).get("datetimeTo", {}).get("utc"),
                            }
                            if msg.get("value") is not None:
                                producer.send(KAFKA_TOPIC, value=msg)
                                msg_count += 1

                producer.flush()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {iso_code}: {msg_count} mesaj → Kafka")

            except Exception as e:
                print(f"[HATA] {iso_code}: {e}")

        print(f"--- Sonraki poll {POLL_INTERVAL // 60} dakika sonra ---\n")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    run()