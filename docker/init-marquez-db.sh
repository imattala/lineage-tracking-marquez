#!/bin/bash
# Creates the marquez role + database on first boot of the marquez-db
# container. Runs via Postgres's /docker-entrypoint-initdb.d convention,
# so it only fires against an empty data directory.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER ${MARQUEZ_USER};
  ALTER USER ${MARQUEZ_USER} WITH PASSWORD '${MARQUEZ_PASSWORD}';
  CREATE DATABASE ${MARQUEZ_DB};
  GRANT ALL PRIVILEGES ON DATABASE ${MARQUEZ_DB} TO ${MARQUEZ_USER};
EOSQL
