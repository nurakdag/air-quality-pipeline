# Air Quality & Health Risk Analytics Pipeline

A production-grade end-to-end streaming data pipeline that monitors global air quality in real time, scores health risks based on WHO thresholds, and surfaces insights through an interactive Power BI dashboard.

---

## Architecture

```
OpenAQ API
    │
    ▼
Python Producer (poll every 15 min)
    │
    ▼
Apache Kafka  ──  Topic: air_quality_raw
    │
    ▼
Python Consumer (batch processing)
    │
    ▼
Google BigQuery  ──  raw_air_quality.air_quality_raw
    │
    ▼
dbt Core
    ├── dim_location
    ├── dim_pollutant
    ├── fct_measurements
    ├── agg_city_daily
    └── exposure_risk_score
    │
    ▼
Power BI Dashboard
    ├── City Trend Analysis
    ├── Top 10 Most Polluted Cities
    └── 7-Day Moving Average KPIs
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | OpenAQ API v3 |
| Message Broker | Apache Kafka (Docker) |
| Container Orchestration | Docker Compose |
| Data Warehouse | Google BigQuery |
| Transformation | dbt Core |
| Visualization | Power BI |
| Language | Python 3.11 |

---

## Project Structure

```
air-quality-pipeline/
│
├── docker-compose.yml          # Kafka, Zookeeper, Kafka UI
├── .env                        # API keys and config
│
├── producer/
│   └── producer.py             # OpenAQ → Kafka
│
├── consumer/
│   └── consumer.py             # Kafka → BigQuery
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_measurements.sql
│   │   ├── dimensions/
│   │   │   ├── dim_location.sql
│   │   │   └── dim_pollutant.sql
│   │   ├── facts/
│   │   │   └── fct_measurements.sql
│   │   └── marts/
│   │       ├── agg_city_daily.sql
│   │       └── exposure_risk_score.sql
│   └── dbt_project.yml
│
├── connect-plugins/            # Kafka Connect BigQuery Sink
└── secrets/
    └── gcp-key.json            # GCP Service Account (gitignored)
```

---

## Setup & Installation

### Prerequisites

- Docker Desktop
- Python 3.11+
- Google Cloud account with BigQuery enabled
- OpenAQ API key ([explore.openaq.org](https://explore.openaq.org))

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/air-quality-pipeline.git
cd air-quality-pipeline
```

### 2. Environment Variables

Create a `.env` file in the root directory:

```env
OPENAQ_API_KEY=your_openaq_api_key
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=air_quality_raw
```

### 3. GCP Service Account

- Create a service account in GCP with **BigQuery Admin** role
- Download the JSON key and place it at `secrets/gcp-key.json`

### 4. Start Kafka

```bash
docker compose up -d
```

Verify all containers are healthy:

```bash
docker compose ps
```

Kafka UI is available at `http://localhost:8080`

### 5. Install Python Dependencies

```bash
python -m venv venv
venv\Scripts\activate       # Windows
pip install kafka-python requests python-dotenv google-cloud-bigquery
```

### 6. Start the Producer

```bash
python producer/producer.py
```

The producer polls OpenAQ every 15 minutes for PM2.5, PM10, NO2 and O3 measurements across target cities and streams them into the `air_quality_raw` Kafka topic.

### 7. Start the Consumer

```bash
python consumer/consumer.py
```

The consumer reads from Kafka in batches of 50 messages and loads them into BigQuery, automatically creating the table on first run.

---

## dbt Transformations

### Models

**`dim_location`** — Unique monitoring stations with city, country and coordinates.

**`dim_pollutant`** — Pollutant metadata including WHO threshold values for PM2.5, PM10, NO2 and O3.

**`fct_measurements`** — Normalized fact table joining measurements with location and pollutant dimensions.

**`agg_city_daily`** — Daily aggregations per city including average, median and 95th percentile values.

**`exposure_risk_score`** — WHO threshold-based health risk scoring model:

| Score | PM2.5 (µg/m³) | Label |
|---|---|---|
| 1 | ≤ 15 | Good |
| 2 | ≤ 35 | Moderate |
| 3 | ≤ 55 | Unhealthy for Sensitive Groups |
| 4 | ≤ 150 | Unhealthy |
| 5 | ≤ 250 | Very Unhealthy |
| 6 | > 250 | Hazardous |

### Run dbt

```bash
cd dbt
dbt deps
dbt run
dbt test
```

---
