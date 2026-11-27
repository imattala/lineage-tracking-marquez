"""M3: the same landing-table load as M2's manual script, but orchestrated
by Airflow with apache-airflow-providers-openlineage doing automatic
lineage emission instead of hand-written client.emit() calls - compare
the jobs this produces in Marquez (job name "ingest_landing_tables" with
child jobs "ingest_landing_tables.ingest_customers" / "...ingest_orders")
against M2's manually-named "ingest_raw_customers" / "ingest_raw_orders"
to see what auto-instrumentation gives for free (job/run lifecycle,
parent/child hierarchy) versus what had to be made explicit there
(dataset-level schema/row-count facets - this DAG's plain PythonOperator
tasks don't declare those, so they don't show up here).
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from pipeline.load import load_table


@dag(
    dag_id="ingest_landing_tables",
    description="Load the synthetic customers/orders CSVs into warehouse-db",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lineage-demo"],
)
def ingest_landing_tables():
    @task
    def ingest_customers() -> int:
        row_count = load_table("raw_customers")
        print(f"loaded {row_count} rows into raw_customers")
        return row_count

    @task
    def ingest_orders() -> int:
        row_count = load_table("raw_orders")
        print(f"loaded {row_count} rows into raw_orders")
        return row_count

    ingest_customers()
    ingest_orders()


ingest_landing_tables()
