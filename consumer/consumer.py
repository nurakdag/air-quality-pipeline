import os
import json
from kafka import KafkaConsumer
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/gcp-key.json"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "air_quality_raw")
BQ_PROJECT      = "air-quality-pipeline-488422"
BQ_DATASET      = "raw_air_quality"
BQ_TABLE        = "air_quality_raw"

client    = bigquery.Client(project=BQ_PROJECT)
table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

def create_table_if_not_exists():
    schema = [
        bigquery.SchemaField("ingested_at",   "TIMESTAMP"),
        bigquery.SchemaField("country_code",  "STRING"),
        bigquery.SchemaField("location_id",   "INTEGER"),
        bigquery.SchemaField("location_name", "STRING"),
        bigquery.SchemaField("latitude",      "FLOAT"),
        bigquery.SchemaField("longitude",     "FLOAT"),
        bigquery.SchemaField("sensor_id",     "INTEGER"),
        bigquery.SchemaField("parameter",     "STRING"),
        bigquery.SchemaField("value",         "FLOAT"),
        bigquery.SchemaField("unit",          "STRING"),
        bigquery.SchemaField("measured_at",   "TIMESTAMP"),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    try:
        client.get_table(table_ref)
        print(f"Tablo zaten var: {table_ref}")
    except Exception:
        client.create_table(table)
        print(f"Tablo oluşturuldu: {table_ref}")

create_table_if_not_exists()

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="earliest",
    group_id="bigquery-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

BATCH_SIZE = 50
batch = []

print(f"Consumer başladı | Topic: {KAFKA_TOPIC} → BigQuery: {table_ref}")

for message in consumer:
    row = message.value
    if row.get("value") is not None:
        batch.append(row)

    if len(batch) >= BATCH_SIZE:
        errors = client.insert_rows_json(table_ref, batch)
        if errors:
            print(f"[HATA] BigQuery: {errors}")
        else:
            print(f"[OK] {len(batch)} satır BigQuery'e yazıldı")
        batch = []