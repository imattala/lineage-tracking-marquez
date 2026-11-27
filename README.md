# lineage-tracking-marquez

Tracking data lineage with [OpenLineage](https://openlineage.io/) and
[Marquez](https://marquezproject.ai/) across a small pipeline: a manually
instrumented ingestion step, an Airflow DAG, and dbt transformations — so
that both auto-instrumented and hand-emitted lineage events land in the
same graph.

Work in progress — see the [issues](../../issues) for the milestone
breakdown.

## Status

- [x] M1 — Marquez up via docker-compose
- [x] M2 — Manual OpenLineage emission for ingestion
- [x] M3 — Airflow orchestration with auto lineage
- [ ] M4 — dbt transformations with lineage
- [ ] M5 — Data quality facets
- [ ] M6 — Polish

## License

MIT
