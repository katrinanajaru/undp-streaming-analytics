import os
import time

import clickhouse_connect
import requests


CONNECT_URL = "http://kafka-connect:8083"
CONNECTOR_NAME = "undp-postgres-connector"

SOURCE_COLUMNS = {
    "projects": [
        ("project_id", "String", "string"),
        ("project_name", "String", "string"),
        ("sector", "String", "string"),
        ("region_id", "String", "string"),
        ("start_date", "String", "string"),
        ("end_date", "String", "string"),
        ("status", "String", "string"),
    ],
    "regions": [
        ("region_id", "String", "string"),
        ("region_name", "String", "string"),
        ("country", "String", "string"),
    ],
    "funding": [
        ("funding_id", "String", "string"),
        ("project_id", "String", "string"),
        ("amount_disbursed_usd", "Float64", "float"),
        ("disbursement_date", "String", "string"),
    ],
    "beneficiaries": [
        ("beneficiary_id", "String", "string"),
        ("project_id", "String", "string"),
        ("region_id", "String", "string"),
        ("gender", "String", "string"),
        ("age_group", "String", "string"),
        ("registration_date", "String", "string"),
    ],
}


def _clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username="default",
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )


def _provision_clickhouse_tables(client):
    client.command("CREATE DATABASE IF NOT EXISTS undp")

    for table_name, columns in SOURCE_COLUMNS.items():
        column_definitions = ",\n".join(
            f"    `{column_name}` {column_type}"
            for column_name, column_type, _ in columns
        )
        client.command(
            f"""CREATE TABLE IF NOT EXISTS undp.{table_name}
(
{column_definitions},
    `loaded_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY `{columns[0][0]}`"""
        )
        client.command(
            f"""CREATE TABLE IF NOT EXISTS undp.{table_name}_kafka
(
    `raw_message` String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:29092',
         kafka_topic_list = 'undp.public.{table_name}',
         kafka_group_name = 'clickhouse_{table_name}_consumer_v1',
         kafka_format = 'JSONAsString',
         kafka_num_consumers = 1"""
        )

        projections = []
        for column_name, _, value_type in columns:
            extractor = "JSONExtractFloat" if value_type == "float" else "JSONExtractString"
            projections.append(
                f"    {extractor}(raw_message, 'after', '{column_name}') AS `{column_name}`"
            )
        projection_sql = ",\n".join(projections)
        key_column = columns[0][0]
        client.command(
            f"""CREATE MATERIALIZED VIEW IF NOT EXISTS undp.{table_name}_mv
TO undp.{table_name}
AS SELECT
{projection_sql}
FROM undp.{table_name}_kafka
WHERE JSONExtractString(raw_message, 'after', '{key_column}') != ''"""
        )


def _connector_config():
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.environ.get("PG_HOST", "host.docker.internal"),
        "database.port": os.environ.get("PG_PORT", "5433"),
        "database.user": os.environ.get("PG_USER", "postgres"),
        "database.dbname": os.environ.get("PG_DB", "undp_analytics"),
        "topic.prefix": "undp",
        "table.include.list": (
            "public.projects,public.regions,public.funding,public.beneficiaries"
        ),
        "plugin.name": "pgoutput",
        "slot.name": "undp_debezium_slot_v2",
        "publication.name": "undp_debezium_publication_v2",
        "decimal.handling.mode": "double",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "snapshot.mode": "initial",
    }


def _ensure_debezium_connector():
    connector_url = f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}"
    deadline = time.monotonic() + 300

    while time.monotonic() < deadline:
        try:
            response = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            response.raise_for_status()
            break
        except requests.RequestException:
            time.sleep(5)
    else:
        raise RuntimeError("Kafka Connect did not become ready within 5 minutes.")

    expected_config = _connector_config()
    existing = requests.get(connector_url, timeout=10)
    should_update = existing.status_code == 404

    if existing.status_code == 200:
        existing_config = requests.get(f"{connector_url}/config", timeout=10)
        existing_config.raise_for_status()
        should_update = any(
            existing_config.json().get(key) != value
            for key, value in expected_config.items()
            if key != "database.password"
        )
        status_response = requests.get(f"{connector_url}/status", timeout=10)
        if status_response.ok:
            status = status_response.json()
            healthy = status.get("connector", {}).get("state") == "RUNNING" and all(
                task.get("state") == "RUNNING" for task in status.get("tasks", [])
            )
            if healthy and not should_update:
                return
            if not healthy:
                should_update = True
    elif existing.status_code != 404:
        existing.raise_for_status()

    if should_update:
        password = os.environ.get("PG_PASSWORD", "")
        if not password:
            raise RuntimeError(
                "PG_PASSWORD is missing. Set it in the ignored .env file, then "
                "restart the Airflow service."
            )
        expected_config["database.password"] = password
        update = requests.put(
            f"{connector_url}/config", json=expected_config, timeout=30
        )
        update.raise_for_status()

    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        status_response = requests.get(f"{connector_url}/status", timeout=10)
        if status_response.ok:
            status = status_response.json()
            connector_running = status.get("connector", {}).get("state") == "RUNNING"
            tasks_running = all(
                task.get("state") == "RUNNING" for task in status.get("tasks", [])
            )
            if connector_running and tasks_running and status.get("tasks"):
                return
            if status.get("connector", {}).get("state") == "FAILED":
                raise RuntimeError(f"Debezium connector failed: {status}")
        time.sleep(10)

    raise RuntimeError("Debezium connector did not become healthy within 5 minutes.")


def ensure_cdc_ingestion():
    client = _clickhouse_client()
    try:
        _provision_clickhouse_tables(client)
    finally:
        client.close()

    _ensure_debezium_connector()


def source_data_ready():
    client = _clickhouse_client()
    try:
        return all(
            client.query(f"SELECT count() FROM undp.{table_name}").result_rows[0][0] > 0
            for table_name in SOURCE_COLUMNS
        )
    finally:
        client.close()