import os
import json
import time
from datetime import datetime, timezone, timedelta

import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

OPENAQ_API_KEY  = os.getenv("OPENAQ_API_KEY", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "air_quality_raw")

BASE_URL = "https://api.openaq.org/v3"
HEADERS  = {"X-API-Key": OPENAQ_API_KEY}

POLL_INTERVAL_SECONDS = 15 * 60
PARAMETERS            = ["pm25", "pm10", "no2", "o3"]
TARGET_COUNTRIES      = ["TR", "DE", "US", "GB", "CN", "IN", "FR", "IT", "JP", "PL"]


def get_locations() -> list:
    """Hedef ülkeler ve parametreler için OpenAQ konum listesini getirir."""
    locations = []
    for country in TARGET_COUNTRIES:
        for param in PARAMETERS:
            page = 1
            while True:
                try:
                    resp = requests.get(
                        f"{BASE_URL}/locations",
                        params={
                            "countries_id":    country,
                            "parameters_name": param,
                            "limit":           1000,
                            "page":            page,
                        },
                        headers=HEADERS,
                        timeout=15,
                    )
                    if resp.status_code == 429:
                        print("  [RATE LIMIT] /locations, 30 saniye bekleniyor...")
                        time.sleep(30)
                        continue
                    resp.raise_for_status()
                    data    = resp.json()
                    results = data.get("results", [])
                    locations.extend(results)
                    found = int(data.get("meta", {}).get("found", 0))
                    if len(locations) >= found or not results:
                        break
                    page += 1
                    time.sleep(0.5)
                except Exception as exc:
                    print(f"  [UYARI] Konum alınamadı ({country}/{param}): {exc}")
                    break
    return locations


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
                    print(f"  [RATE LIMIT] Sensor {sensor_id}, 30 saniye bekleniyor...")
                    time.sleep(30)
                    resp = None
                    continue
                resp.raise_for_status()
                break
            except requests.exceptions.HTTPError:
                resp = None
                if attempt == 2:
                    return all_results
                time.sleep(10)

        if resp is None:
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


if __name__ == "__main__":
    print(f"Producer başlatılıyor | Topic: {KAFKA_TOPIC} | Bootstrap: {KAFKA_BOOTSTRAP}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )

    print("Konumlar ve sensörler keşfediliyor...")
    locations = get_locations()
    print(f"{len(locations)} konum bulundu.")

    cycle = 0
    while True:
        cycle += 1
        now     = datetime.now(timezone.utc)
        dt_to   = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        dt_from = (now - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f"\n[{now.isoformat()}] Döngü #{cycle} | {dt_from} → {dt_to}")

        total = 0
        for loc in locations:
            loc_id   = loc.get("id")
            loc_name = loc.get("name", "unknown")
            city     = loc.get("locality", "unknown")
            country  = loc.get("country", {}).get("code", "unknown")
            coords   = loc.get("coordinates", {})
            lat      = coords.get("latitude")
            lon      = coords.get("longitude")

            for sensor in loc.get("sensors", []):
                sensor_id = sensor.get("id")
                param     = sensor.get("parameter", {}).get("name", "unknown")
                unit      = sensor.get("parameter", {}).get("units", "unknown")

                if param not in PARAMETERS:
                    continue

                measurements = get_measurements(sensor_id, dt_from, dt_to)
                for m in measurements:
                    value       = m.get("value")
                    measured_at = (
                        m.get("period", {}).get("datetimeTo", {}).get("utc")
                        or m.get("date", {}).get("utc")
                    )
                    if value is None:
                        continue

                    row = {
                        "ingested_at":   now.isoformat(),
                        "city":          city,
                        "country_code":  country,
                        "location_id":   loc_id,
                        "location_name": loc_name,
                        "latitude":      lat,
                        "longitude":     lon,
                        "sensor_id":     sensor_id,
                        "parameter":     param,
                        "value":         float(value),
                        "unit":          unit,
                        "measured_at":   measured_at,
                    }
                    producer.send(KAFKA_TOPIC, value=row)
                    total += 1

        producer.flush()
        print(f"  {total} mesaj Kafka'ya gönderildi.")

        # Her 24 saatte bir konum listesini yenile (96 döngü × 15 dk = 24 saat)
        if cycle % 96 == 0:
            print("Konum listesi yenileniyor...")
            locations = get_locations()
            print(f"{len(locations)} konum bulundu.")

        print(f"  {POLL_INTERVAL_SECONDS // 60} dakika bekleniyor...")
        time.sleep(POLL_INTERVAL_SECONDS)
