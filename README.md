# Hackathon Data Pipeline

Local ELT pipeline: scrape → ingest → transform → visualize.

## Quick Start

```bash
make up          # Start all services (postgres, minio, superset, omnidb, airflow)
make scrape      # Scrape citizens + logins to MinIO
make ingest      # Load raw JSON into PostgreSQL
make transform   # Run dbt models + export parquet
```

## Services

| Service    | URL                    | Credentials             |
|------------|------------------------|-------------------------|
| Airflow    | http://localhost:8081  | admin / admin           |
| Superset   | http://localhost:8088  | admin / admin           |
| OmniDB     | http://localhost:8080  | admin / admin           |
| MinIO      | http://localhost:9001  | minioadmin / minioadmin |
| PostgreSQL | localhost:5432         | hackathon / hackathon   |

## Pipeline Stages

```
scrape → MinIO (raw/)  →  ingest → PostgreSQL (raw.*)  →  transform → PostgreSQL (public_marts.*)
                                                                   → MinIO (exports/*.parquet)
```

## Commands

| Command                    | Description                                      |
|----------------------------|--------------------------------------------------|
| `make up`                  | Start all services (builds images)               |
| `make down`                | Stop services, remove containers                 |
| `make clean`               | Stop services and wipe all volumes               |
| `make reset`               | `clean` + `up`                                   |
| `make status`              | Show running containers                          |
| `make logs`                | Tail logs for all services                       |
| `make psql`                | Open psql shell                                  |
| `make console`             | Print service URLs and credentials               |
| `make omnidb`              | Print OmniDB connection details                  |
| `make next-day`            | Advance the simulated pipeline date by one day   |
| `make scrape`              | Scrape citizens + logins to MinIO                |
| `make scrape-citizens`     | Scrape citizens only                             |
| `make scrape-citizens-fail`| Scrape citizens (simulated failure)              |
| `make scrape-logins`       | Scrape logins only                               |
| `make scrape-logins-fail`  | Scrape logins (simulated failure)                |
| `make ingest`              | Load raw JSON into PostgreSQL                    |
| `make ingest-build`        | Rebuild the ingestion image                      |
| `make transform`           | Run dbt models + export parquet                  |
| `make transform-build`     | Rebuild the transform image                      |
| `make scrape-build`        | Rebuild the scraper images (citizens + logins)   |
| `make airflow-logs`        | Tail Airflow scheduler logs                      |

## Data Model

**Marts** (in `public_marts` schema):
- `mart_citizens` - Citizens with login activity metrics and account health
- `mart_logins` - Login attempts enriched with citizen info
