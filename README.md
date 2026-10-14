# UNDP Streaming Analytics

A local development stack for capturing changes from an existing UNDP PostgreSQL database and exploring project impact analytics with ClickHouse, dbt, and supporting services.

## What is included

- Docker Compose services for Kafka, ZooKeeper, Debezium Connect, ClickHouse, Airflow, Prometheus, and Grafana.
- A Debezium PostgreSQL source connector example for the `projects`, `regions`, `funding`, and `beneficiaries` tables.
- dbt staging and mart models under `undp_dbt/models`.
- An Airflow DAG that provisions CDC ingestion and runs dbt plus impact scoring every 15 minutes.
- A PostgreSQL data simulator and a Python impact-scoring script.

ClickHouse's native Kafka engine consumes Debezium topics and materialized views write source rows into the `undp` database. The Airflow DAG creates these tables/views, registers or checks the Debezium connector, waits for source data, then runs dbt and impact scoring. A Grafana data source is not provisioned automatically.

## Prerequisites

- Docker Desktop with Docker Compose v2.
- An existing PostgreSQL database named `undp_analytics`, reachable on host port `5433`, containing the four tables listed above.
- PostgreSQL logical replication enabled (`wal_level = logical`) and a database user with the replication/table permissions Debezium requires.
- Python 3.10 or newer for the optional scripts and dbt CLI.

The PostgreSQL database is not created by this Compose file. The `airflow-db` service is a separate PostgreSQL instance used only for Airflow metadata.

## Configure and start the stack

In PowerShell, create `.env` from the template if needed, then set private ClickHouse and PostgreSQL credentials in `.env`:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
notepad .env
```

Set `CLICKHOUSE_PASSWORD` and `PG_PASSWORD` to the credentials for your ClickHouse and existing PostgreSQL servers. Adjust `PG_HOST`, `PG_PORT`, `PG_DB`, and `PG_USER` if your PostgreSQL settings differ. `.env` is ignored by Git; do not commit it.

Validate the Compose configuration and start the services:

```powershell
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

| Service | Local address | Notes |
| --- | --- | --- |
| Airflow | <http://localhost:8081> | Standalone mode; initial credentials are reported in the Airflow container logs. |
| ClickHouse HTTP | `localhost:8124` | Native protocol is on `localhost:9001`. |
| Debezium Connect REST API | <http://localhost:8084> | PostgreSQL connector registration endpoint. |
| Grafana | <http://localhost:3001> | Compose currently sets the development login to `admin` / `admin`. |
| Prometheus | <http://localhost:9091> | Scrapes Kafka broker/topic/consumer-group metrics through Kafka Exporter. |
| Kafka Exporter | <http://localhost:9308/metrics> | Exposes Kafka metrics to Prometheus. |
| Kafka | `localhost:9092` | Internal Compose clients use `kafka:29092`. |

The current Airflow metadata database password and Grafana login are development defaults in `docker-compose.yml`. Change them before exposing this stack beyond a trusted local environment.

Prometheus scrapes Kafka metrics through Kafka Exporter. Kafka Connect worker/JVM metrics are not included; those require a separate JMX exporter setup.

## PostgreSQL CDC setup

The DAG uses the settings in `.env` to register `undp-postgres-connector` with Debezium. The connector example remains available as a reference, but you do not need to register it manually. The first DAG run creates the ClickHouse Kafka tables and materialized views before registering the connector. Later runs reuse a healthy connector.

Check connector status at any time:

```powershell
Invoke-RestMethod -Uri "http://localhost:8084/connectors/undp-postgres-connector/status"
```

PostgreSQL must be running with logical replication enabled. The DAG waits for all four source tables to contain data before starting dbt.

## Run dbt models

The project includes `undp_dbt/profiles.yml`, which the Airflow DAG uses to connect to ClickHouse on the Compose network. For manual dbt runs from Windows, install the ClickHouse adapter and set the password environment variable before running dbt:

```powershell
$env:CLICKHOUSE_PASSWORD = "<your ClickHouse password>"
python -m pip install dbt-clickhouse
Set-Location .\undp_dbt
dbt debug
dbt run
Set-Location ..
```

The staging models are configured as views and the mart models as tables. The expected raw ClickHouse tables must already exist before the models can run.

## Optional Python scripts

Install the dependencies for the PostgreSQL simulator:

```powershell
python -m pip install psycopg2-binary
$env:PG_PASSWORD = "<your PostgreSQL password>"
python .\simulate_live_undp_data.py
```

The simulator connects to PostgreSQL on `localhost:5433` and inserts three funding rows and five beneficiary rows every five seconds. It needs populated `projects` and `regions` tables; stop it with Ctrl+C.

Install the impact-scoring dependencies and run the scoring script after `undp.master_project_summary` has been built in ClickHouse:

```powershell
python -m pip install pandas numpy scikit-learn shap clickhouse-connect
$env:CLICKHOUSE_PASSWORD = "<your ClickHouse password>"
python .\ai_scoring_engine.py
```

The scoring script writes refreshed results to `undp.impact_scores`.

## Automated Airflow pipeline

After `docker compose up -d --build`, Airflow schedules `undp_analytics_pipeline` every 15 minutes. Each run ensures ClickHouse CDC tables and the Debezium connector are ready, waits for source rows, runs `dbt run`, then refreshes `undp.impact_scores`. View run status at <http://localhost:8081>. The external PostgreSQL server and its initial schema/data remain prerequisites; the Compose stack does not create that database.

## Stop services

```powershell
docker compose down
```

This stops containers but preserves named data volumes. `docker compose down -v` also deletes the ClickHouse, Kafka, Grafana, Prometheus, and Airflow database volumes.