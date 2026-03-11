import os
import json
import time
import logging
import requests
from kafka import KafkaProducer
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("producer")

# --- Config & Validation ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "air_quality_raw")
OPENAQ_API_KEY  = os.getenv("OPENAQ_API_KEY")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL_SECONDS", "900"))
BASE_URL        = "https://api.openaq.org/v3"
RADIUS          = int(os.getenv("SEARCH_RADIUS_METERS", "25000"))

if not OPENAQ_API_KEY:
    raise ValueError("OPENAQ_API_KEY environment variable is required")

HEADERS = {
    "X-API-Key": OPENAQ_API_KEY,
    "Accept": "application/json"
}

TARGET_PARAMS = [
    "pm25", "pm10", "pm1", "pm4",
    "no2", "no", "nox",
    "o3",
    "so2",
    "co", "co2",
    "bc",
    "temperature", "humidity"
]

CITIES = [
    {"city": "Jakarta",       "lat": -6.2088,  "lon": 106.8456, "country": "ID"},
    {"city": "Dhaka",         "lat": 23.8103,  "lon": 90.4125,  "country": "BD"},
    {"city": "Tokyo",         "lat": 35.6762,  "lon": 139.6503, "country": "JP"},
    {"city": "Delhi",         "lat": 28.6139,  "lon": 77.2090,  "country": "IN"},
    {"city": "Shanghai",      "lat": 31.2304,  "lon": 121.4737, "country": "CN"},
    {"city": "Cairo",         "lat": 30.0444,  "lon": 31.2357,  "country": "EG"},
    {"city": "Beijing",       "lat": 39.9042,  "lon": 116.4074, "country": "CN"},
    {"city": "Mumbai",        "lat": 19.0760,  "lon": 72.8777,  "country": "IN"},
    {"city": "Osaka",         "lat": 34.6937,  "lon": 135.5023, "country": "JP"},
    {"city": "Seoul",         "lat": 37.5665,  "lon": 126.9780, "country": "KR"},
    {"city": "Washington DC", "lat": 38.9072,  "lon": -77.0369, "country": "US"},
    {"city": "London",        "lat": 51.5074,  "lon": -0.1278,  "country": "GB"},
    {"city": "Singapore",     "lat": 1.3521,   "lon": 103.8198, "country": "SG"},
    {"city": "Frankfurt",     "lat": 50.1109,  "lon": 8.6821,   "country": "DE"},
    {"city": "Sao Paulo",     "lat": -23.5505, "lon": -46.6333, "country": "BR"},
    {"city": "Istanbul",      "lat": 41.0082,  "lon": 28.9784,  "country": "TR"},
]


def create_producer() -> KafkaProducer:
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP}")
        return p
    except Exception as e:
        logger.critical(f"Failed to connect to Kafka at {KAFKA_BOOTSTRAP}: {e}")
        raise


def get_locations_by_coords(lat: float, lon: float) -> list:
    resp = requests.get(
        f"{BASE_URL}/locations",
        params={"coordinates": f"{lat},{lon}", "radius": RADIUS, "limit": 10},
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


def get_measurements(sensor_id: int, datetime_from: str, datetime_to: str) -> list:
    all_results = []
    page = 1

    while True:
        resp = None

        for attempt in range(3):
            try:
                resp = requests.get(
                    f"{BASE_URL}/sensors/{sensor_id}/measurements",
                    params={
                        "datetime_from": datetime_from,
                        "datetime_to":   datetime_to,
                        "limit":         1000,
                        "page":          page
                    },
                    headers=HEADERS,
                    timeout=15
                )

                if resp.status_code == 429:
                    # Exponential backoff on rate limit — retry same page
                    wait = 30 * (2 ** attempt)
                    logger.warning(
                        f"Rate limited on sensor {sensor_id} page {page}, "
                        f"waiting {wait}s (attempt {attempt + 1}/3)"
                    )
                    time.sleep(wait)
                    resp = None
                    continue

                resp.raise_for_status()
                break  # success — exit retry loop

            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Request error for sensor {sensor_id} page {page} "
                    f"(attempt {attempt + 1}/3): {e}"
                )
                if attempt < 2:
                    time.sleep(10 * (attempt + 1))
                else:
                    logger.error(f"Giving up on sensor {sensor_id} page {page}")
                    return all_results

        if resp is None:
            logger.error(f"Sensor {sensor_id} page {page} exhausted all rate-limit retries, skipping")
            return all_results

        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)

        found = int(data.get("meta", {}).get("found", 0))
        if len(all_results) >= found or len(results) == 0:
            break
        page += 1
        time.sleep(1)

    return all_results


def run():
    producer = create_producer()
    logger.info(
        f"Producer started | {len(CITIES)} cities | "
        f"{len(TARGET_PARAMS)} parameters | Topic: {KAFKA_TOPIC}"
    )

    while True:
        now = datetime.now(timezone.utc)
        datetime_from = (now - timedelta(hours=20)).isoformat()
        datetime_to = now.isoformat()
        total = 0

        for city_info in CITIES:
            try:
                locations = get_locations_by_coords(city_info["lat"], city_info["lon"])
                msg_count = 0

                for location in locations:
                    sensors = get_sensors(location["id"])
                    for sensor in sensors:
                        param = sensor.get("parameter", {}).get("name", "")
                        if param not in TARGET_PARAMS:
                            continue

                        measurements = get_measurements(sensor["id"], datetime_from, datetime_to)
                        for m in measurements:
                            value = m.get("value")
                            measured_at = m.get("period", {}).get("datetimeTo", {}).get("utc")

                            if value is None or measured_at is None:
                                continue

                            msg = {
                                "ingested_at":   now.isoformat(),
                                "city":          city_info["city"],
                                "country_code":  city_info["country"],
                                "location_id":   location["id"],
                                "location_name": location.get("name"),
                                "latitude":      location.get("coordinates", {}).get("latitude"),
                                "longitude":     location.get("coordinates", {}).get("longitude"),
                                "sensor_id":     sensor["id"],
                                "parameter":     param,
                                "value":         value,
                                "unit":          sensor.get("parameter", {}).get("units"),
                                "measured_at":   measured_at,
                            }
                            producer.send(KAFKA_TOPIC, value=msg)
                            msg_count += 1

                producer.flush()
                logger.info(f"[{city_info['city']}] {msg_count} messages sent")
                total += msg_count
                time.sleep(2)

            except Exception as e:
                logger.error(f"[{city_info['city']}] Failed: {e}", exc_info=True)

        logger.info(f"Poll complete | Total: {total} messages | Next poll in {POLL_INTERVAL // 60}m")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
