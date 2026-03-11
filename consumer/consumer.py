import os
import json
import signal
import sys
import logging
from kafka import KafkaConsumer
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("consumer")

# --- Config & Validation ---
GCP_KEY_PATH    = os.getenv("GCP_KEY_PATH", "secrets/gcp-key.json")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "air_quality_raw")
BQ_PROJECT      = os.getenv("BQ_PROJECT")
BQ_DATASET      = os.getenv("BQ_DATASET", "raw_air_quality")
BQ_TABLE        = os.getenv("BQ_TABLE", "air_quality_raw")
BATCH_SIZE      = int(os.getenv("CONSUMER_BATCH_SIZE", "50"))

if not BQ_PROJECT:
    raise ValueError("BQ_PROJECT environment variable is required")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH


def create_bq_client() -> bigquery.Client:
    try:
        client = bigquery.Client(project=BQ_PROJECT)
        logger.info(f"BigQuery client connected to project {BQ_PROJECT}")
        return client
    except Exception as e:
        logger.critical(f"Failed to connect to BigQuery: {e}")
        raise


def create_kafka_consumer() -> KafkaConsumer:
    try:
        c = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            group_id="bigquery-consumer-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        logger.info(f"Kafka consumer connected to {KAFKA_BOOTSTRAP}, topic: {KAFKA_TOPIC}")
        return c
    except Exception as e:
        logger.critical(f"Failed to connect to Kafka: {e}")
        raise


client    = create_bq_client()
table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"


def create_table_if_not_exists():
    schema = [
        bigquery.SchemaField("ingested_at",   "TIMESTAMP"),
        bigquery.SchemaField("city",          "STRING"),
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
        logger.info(f"Table already exists: {table_ref}")
    except Exception:
        client.create_table(table)
        logger.info(f"Table created: {table_ref}")


def flush_batch(batch: list):
    if not batch:
        return
    errors = client.insert_rows_json(table_ref, batch)
    if errors:
        logger.error(f"BigQuery insert errors: {errors}")
    else:
        logger.info(f"{len(batch)} rows written to BigQuery")
    batch.clear()


create_table_if_not_exists()
consumer = create_kafka_consumer()

batch = []


def shutdown(signum, frame):
    logger.info("Shutdown signal received, flushing remaining batch...")
    flush_batch(batch)
    consumer.close()
    logger.info("Consumer closed.")
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

logger.info(f"Consumer started | Topic: {KAFKA_TOPIC} → BigQuery: {table_ref}")

for message in consumer:
    try:
        row = message.value

        if not isinstance(row, dict):
            logger.warning(f"Invalid message format, skipping: {type(row)}")
            continue

        if row.get("value") is None or row.get("measured_at") is None:
            logger.debug("Skipping message with missing required fields")
            continue

        batch.append(row)

        if len(batch) >= BATCH_SIZE:
            flush_batch(batch)

    except json.JSONDecodeError as e:
        logger.error(f"Failed to deserialize message: {e}")
    except Exception as e:
        logger.error(f"Unexpected error processing message: {e}", exc_info=True)
