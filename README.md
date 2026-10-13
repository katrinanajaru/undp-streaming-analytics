# UNDP Streaming Analytics

A local development stack for capturing changes from an existing UNDP PostgreSQL database and exploring project impact analytics with ClickHouse, dbt, and supporting services.

## What is included

- Docker Compose services for Kafka, ZooKeeper, Debezium Connect, ClickHouse, Airflow, Prometheus, and Grafana.
- A Debezium PostgreSQL source connector example for the `projects`, `regions`, `funding`, and `beneficiaries` tables.
- dbt staging and mart models under `undp_dbt/models`.
- A PostgreSQL data simulator and a Python impact-scoring script.

The Compose stack does not currently configure a Kafka-to-ClickHouse sink connector, provision a Grafana data source, or include Airflow DAGs. Those pieces must be added/configured separately for a fully automated end-to-end pipeline. The dbt models expect the source tables to be available in ClickHouse's `undp` database.

## Prerequisites

- Docker Desktop with Docker Compose v2.
- An existing PostgreSQL database named `undp_analytics`, reachable on host port `5433`, containing the four tables listed above.
- PostgreSQL logical replication enabled (`wal_level = logical`) and a database user with the replication/table permissions Debezium requires.
- Python 3.10 or newer for the optional scripts and dbt CLI.

The PostgreSQL database is not created by this Compose file. The `airflow-db` service is a separate PostgreSQL instance used only for Airflow metadata.

## Configure and start the stack

In PowerShell, create `.env` from the template if needed, then set a private ClickHouse password in `.env`:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
notepad .env
```

Set `CLICKHOUSE_PASSWORD` to a non-default value. `.env` is ignored by Git; do not commit it.

Validate the Compose configuration and start the services:

```powershell
docker compose config --quiet
docker compose up -d
docker compose ps
```

| Service | Local address | Notes |
| --- | --- | --- |
| Airflow | <http://localhost:8081> | Standalone mode; initial credentials are reported in the Airflow container logs. |
| ClickHouse HTTP | `localhost:8124` | Native protocol is on `localhost:9001`. |
| Debezium Connect REST API | <http://localhost:8084> | PostgreSQL connector registration endpoint. |
| Grafana | <http://localhost:3001> | Compose currently sets the development login to `admin` / `admin`. |
| Prometheus | <http://localhost:9091> | Scrape configuration is in `prometheus/prometheus.yml`. |
| Kafka | `localhost:9092` | Internal Compose clients use `kafka:29092`. |

The current Airflow metadata database password and Grafana login are development defaults in `docker-compose.yml`. Change them before exposing this stack beyond a trusted local environment.

## Configure PostgreSQL CDC

Copy the connector template to the ignored local config file and edit its database settings and password:

```powershell
Copy-Item .\debezium-postgres-connector.example.json .\debezium-postgres-connector.json
notepad .\debezium-postgres-connector.json
```

The template targets `host.docker.internal:5433`, database `undp_analytics`, and the four tables listed in the prerequisites. The real connector file is ignored by Git because it contains credentials.

Register the connector after the stack is running:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://localhost:8084/connectors" `
  -ContentType "application/json" `
  -InFile ".\debezium-postgres-connector.json"
```

Check connector status:

```powershell
Invoke-RestMethod -Uri "http://localhost:8084/connectors/undp-postgres-connector/status"
```

This creates Debezium change-event topics in Kafka. A sink connector or other consumer is still needed to load those events into ClickHouse.

## Run dbt models

The dbt project uses the profile name `undp_dbt`, but `profiles.yml` is intentionally not included. Install a ClickHouse dbt adapter and create `~/.dbt/profiles.yml` with a profile named `undp_dbt` that connects to `localhost:8124`, database/schema `undp`, and the local ClickHouse credentials.

```powershell
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

## Stop services

```powershell
docker compose down
```

This stops containers but preserves named data volumes. `docker compose down -v` also deletes the ClickHouse, Kafka, Grafana, Prometheus, and Airflow database volumes.