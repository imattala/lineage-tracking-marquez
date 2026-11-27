"""Shared DB-loading logic, reused by both the manually-instrumented M2
script (ingest.py) and the Airflow DAG (M3) - kept free of any
OpenLineage code so each caller controls how (or whether) lineage gets
emitted around it.
"""

import csv
import os
from pathlib import Path

import psycopg

DATA_DIR = Path(__file__).parent / "data"
WAREHOUSE_DSN = os.environ.get(
    "WAREHOUSE_DSN", "postgresql://warehouse:warehouse@localhost:5432/warehouse"
)

TABLES = {
    "raw_customers": {
        "csv_filename": "customers.csv",
        "columns": ["customer_id", "name", "email", "created_at"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS raw_customers (
                customer_id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                created_at TIMESTAMPTZ
            )
        """,
    },
    "raw_orders": {
        "csv_filename": "orders.csv",
        "columns": ["order_id", "customer_id", "order_date", "amount", "status"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS raw_orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date TIMESTAMPTZ,
                amount NUMERIC(10, 2),
                status TEXT
            )
        """,
    },
}


def load_table(table_name: str) -> int:
    """Load TABLES[table_name]'s CSV into its landing table. Returns row count."""
    spec = TABLES[table_name]
    csv_path = DATA_DIR / spec["csv_filename"]

    with psycopg.connect(WAREHOUSE_DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(spec["ddl"])
        cur.execute(f"TRUNCATE TABLE {table_name}")

        with csv_path.open() as f:
            reader = csv.DictReader(f)
            rows = [tuple(row[c] for c in spec["columns"]) for row in reader]

        placeholders = ", ".join(["%s"] * len(spec["columns"]))
        cur.executemany(
            f"INSERT INTO {table_name} ({', '.join(spec['columns'])}) VALUES ({placeholders})",
            rows,
        )
        return len(rows)
