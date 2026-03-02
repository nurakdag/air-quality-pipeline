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