"""Load the synthetic CSVs into warehouse-db landing tables.

Manually instrumented with the openlineage-python client (no Airflow/dbt
auto-instrumentation yet - that's M3/M4) so the OpenLineage event model
itself has to be understood: START before the work, then COMPLETE (or
FAIL) with facets describing what actually happened.
"""

import csv
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from openlineage.client import OpenLineageClient
from openlineage.client.facet_v2 import (
    datasource_dataset,
    output_statistics_output_dataset,
    schema_dataset,
)
from openlineage.client.run import Dataset, InputDataset, Job, OutputDataset, Run, RunEvent, RunState

DATA_DIR = Path(__file__).parent / "data"
MARQUEZ_URL = os.environ.get("MARQUEZ_URL", "http://localhost:5000")
WAREHOUSE_DSN = os.environ.get(
    "WAREHOUSE_DSN", "postgresql://warehouse:warehouse@localhost:5432/warehouse"
)

# The dataset namespace for the warehouse uses the docker-network hostname
# (not "localhost", which is only how *this script* happens to reach it)
# so that the same dataset identity is produced whether this job runs on
# the host, as now, or inside the compose network later (Airflow in M3).
WAREHOUSE_NAMESPACE = "postgres://warehouse-db:5432"
OL_JOB_NAMESPACE = "lineage-demo"
PRODUCER = "https://github.com/imattala/lineage-tracking-marquez/pipeline/ingest.py"

client = OpenLineageClient(config={"transport": {"type": "http", "url": MARQUEZ_URL}})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit(event_type: RunState, run_id: str, job_name: str, inputs=None, outputs=None) -> None:
    client.emit(
        RunEvent(
            eventType=event_type,
            eventTime=_now(),
            run=Run(runId=run_id),
            job=Job(namespace=OL_JOB_NAMESPACE, name=job_name),
            producer=PRODUCER,
            inputs=inputs or [],
            outputs=outputs or [],
        )
    )


def ingest_table(csv_filename: str, table_name: str, ddl: str, columns: list[str]) -> None:
    job_name = f"ingest_{table_name}"
    run_id = str(uuid.uuid4())
    csv_path = DATA_DIR / csv_filename

    input_dataset = InputDataset(
        namespace=f"file://{csv_path.parent}",
        name=csv_path.name,
    )

    _emit(RunState.START, run_id, job_name, inputs=[input_dataset])

    try:
        with psycopg.connect(WAREHOUSE_DSN, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute(f"TRUNCATE TABLE {table_name}")

            with csv_path.open() as f:
                reader = csv.DictReader(f)
                rows = [tuple(row[c] for c in columns) for row in reader]

            placeholders = ", ".join(["%s"] * len(columns))
            cur.executemany(
                f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                rows,
            )
            row_count = len(rows)
    except Exception:
        _emit(RunState.FAIL, run_id, job_name, inputs=[input_dataset])
        raise

    schema_fields = [schema_dataset.SchemaDatasetFacetFields(name=c, type="string") for c in columns]

    completed_input = InputDataset(
        namespace=input_dataset.namespace,
        name=input_dataset.name,
        facets={"schema": schema_dataset.SchemaDatasetFacet(fields=schema_fields)},
    )
    output_dataset = OutputDataset(
        namespace=WAREHOUSE_NAMESPACE,
        name=f"public.{table_name}",
        facets={
            "schema": schema_dataset.SchemaDatasetFacet(fields=schema_fields),
            "dataSource": datasource_dataset.DatasourceDatasetFacet(
                name="warehouse-db", uri=f"{WAREHOUSE_NAMESPACE}/warehouse"
            ),
        },
        outputFacets={
            "outputStatistics": output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                rowCount=row_count
            )
        },
    )

    _emit(RunState.COMPLETE, run_id, job_name, inputs=[completed_input], outputs=[output_dataset])
    print(f"{job_name}: loaded {row_count} rows into {table_name}")


def main() -> None:
    ingest_table(
        "customers.csv",
        "raw_customers",
        """
        CREATE TABLE IF NOT EXISTS raw_customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            created_at TIMESTAMPTZ
        )
        """,
        ["customer_id", "name", "email", "created_at"],
    )
    ingest_table(
        "orders.csv",
        "raw_orders",
        """
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TIMESTAMPTZ,
            amount NUMERIC(10, 2),
            status TEXT
        )
        """,
        ["order_id", "customer_id", "order_date", "amount", "status"],
    )


if __name__ == "__main__":
    main()
