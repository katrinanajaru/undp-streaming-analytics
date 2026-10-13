"""
simulate_live_undp_data.py

Simulates a live UNDP operations system by periodically inserting new rows
into the existing `funding` and `beneficiaries` tables in the `undp_analytics`
Postgres database. This gives Debezium real change events to capture, so the
Kafka -> ClickHouse -> Grafana pipeline has something to show in a live demo.

It does NOT touch your existing `projects` or `regions` tables — it only reads
their IDs to generate realistic foreign-key references, then inserts new rows
into `funding` and `beneficiaries`.

USAGE:
    pip install psycopg2-binary
    python simulate_live_undp_data.py
"""

import psycopg2
import random
import time
import uuid
from datetime import date, timedelta

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "undp_analytics",
    "user": "postgres",
    "password": __import__("os").environ.get("PG_PASSWORD", ""),
}

INSERT_INTERVAL_SECONDS = 5
FUNDING_ROWS_PER_TICK = 3
BENEFICIARY_ROWS_PER_TICK = 5

GENDERS = ["Male", "Female", "Other"]
AGE_GROUPS = ["Child (0-14)", "Youth (15-24)", "Adult (25-59)", "Elderly (60+)"]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_reference_ids(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT project_id FROM projects;")
        project_ids = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT region_id FROM regions;")
        region_ids = [row[0] for row in cur.fetchall()]

    if not project_ids or not region_ids:
        raise RuntimeError(
            "No project_ids or region_ids found. Make sure your existing "
            "projects/regions tables are populated before running this script."
        )
    return project_ids, region_ids


def insert_funding_rows(conn, project_ids, count):
    rows = []
    today = date.today()
    for _ in range(count):
        funding_id = "FL" + str(random.randint(100000, 999999))
        project_id = random.choice(project_ids)
        amount = round(random.uniform(5_000, 2_000_000), 2)
        disbursement_date = today - timedelta(days=random.randint(0, 3))
        rows.append((funding_id, project_id, amount, disbursement_date))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO funding (funding_id, project_id, amount_disbursed_usd, disbursement_date)
            VALUES (%s, %s, %s, %s);
            """,
            rows,
        )
    conn.commit()
    return rows


def insert_beneficiary_rows(conn, project_ids, region_ids, count):
    rows = []
    today = date.today()
    for _ in range(count):
        beneficiary_id = "BL" + str(random.randint(100000, 999999))
        project_id = random.choice(project_ids)
        region_id = random.choice(region_ids)
        gender = random.choice(GENDERS)
        age_group = random.choice(AGE_GROUPS)
        registration_date = today - timedelta(days=random.randint(0, 3))
        rows.append(
            (beneficiary_id, project_id, region_id, gender, age_group, registration_date)
        )

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO beneficiaries (beneficiary_id, project_id, region_id, gender, age_group, registration_date)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            rows,
        )
    conn.commit()
    return rows


def main():
    print("Connecting to Postgres...")
    conn = get_connection()
    print("Connected. Fetching reference IDs...")
    project_ids, region_ids = fetch_reference_ids(conn)
    print(f"Loaded {len(project_ids)} project_ids and {len(region_ids)} region_ids.")
    print(
        f"Starting live simulation: every {INSERT_INTERVAL_SECONDS}s, inserting "
        f"{FUNDING_ROWS_PER_TICK} funding rows and {BENEFICIARY_ROWS_PER_TICK} beneficiary rows.\n"
        "Press Ctrl+C to stop."
    )

    tick = 0
    try:
        while True:
            tick += 1
            funding_rows = insert_funding_rows(conn, project_ids, FUNDING_ROWS_PER_TICK)
            beneficiary_rows = insert_beneficiary_rows(
                conn, project_ids, region_ids, BENEFICIARY_ROWS_PER_TICK
            )
            print(
                f"[tick {tick}] inserted {len(funding_rows)} funding rows, "
                f"{len(beneficiary_rows)} beneficiary rows"
            )
            time.sleep(INSERT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
