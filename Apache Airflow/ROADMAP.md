# Roadmap

Daily e-commerce analytics pipeline, built milestone by milestone on Airflow 3.x.
Sources: synthetic data (Python/Faker). Destinations: PostgreSQL + ClickHouse from the start.

| # | Milestone | Concepts |
|---|-----------|----------|
| 0 | Environment & repo setup | Docker Compose (Airflow 3.x, Postgres, ClickHouse), Airflow vs NiFi |
| 1 | First DAG | DAGs, tasks, dependencies, schedule vs data_interval, catchup |
| 2 | TaskFlow API | `@task`, XCom basics |
| 3 | Connections, Hooks, Variables | Postgres/ClickHouse hooks, credentials out of code |
| 4 | Incremental extraction | Source-availability check, watermarking |
| 5 | Idempotent loading | Upsert/merge, duplicate handling |
| 6 | Retries & failure simulation | retries, retry_delay, timeouts, logs |
| 7 | Data quality gate | DQ checks that can fail the pipeline |
| 8 | Alerting | on_failure_callback, notifications |
| 9 | Transformations | Daily analytics dataset, Postgres → ClickHouse |
| 10 | Backfill & catchup | Safe historical reprocessing |
| 11 | Wrap-up | Observability polish, Airflow vs NiFi writeup, interview prep |

Each milestone: concept explanation -> small task for you to attempt -> code review -> failure-scenario test -> interview-answer framing.
