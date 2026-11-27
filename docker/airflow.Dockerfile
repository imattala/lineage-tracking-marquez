# Thin layer on top of the official image: bakes in the OpenLineage
# provider + the pipeline's own runtime deps, so `docker compose up`
# doesn't pay a pip-install tax on every container start (the officially
# documented `_PIP_ADDITIONAL_REQUIREMENTS` env var does that, which is
# fine for a one-off demo but not for something meant to be rebuilt often).
FROM apache/airflow:2.11.2

RUN pip install --no-cache-dir \
    apache-airflow-providers-openlineage==2.21.1 \
    "psycopg[binary]==3.3.9" \
    faker==40.63.0
