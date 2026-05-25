# Chilean Public Salary Explorer 🇨🇱

This repository provides Python tooling to extract, normalize, audit, and query transparency data from Chilean public institutions (currently the Transparency Council, the Senate, and the Chamber of Deputies).

The system queries over 28GB of data using DuckDB and Parquet. It is designed to run without a dedicated database backend, executing HTTP Range requests directly against static files.

## Features

- 📥 **Smart Sync:** Polls upstream transparency servers and downloads data only when remote headers indicate modifications. Includes a dedicated extractor for the Senate REST API.
- ⚡️ **Query Engine:** Converts 28GB of raw CSV and JSON files into ZSTD-compressed `.parquet` partitions. Uses DuckDB for in-memory analytical queries.
- ☁️ **Stateless Deployment:** Designed for serverless container environments. Reads data directly from GitHub Releases using DuckDB's `httpfs` extension.
- 📊 **Web UI:** Streamlit frontend for filtering salaries by institution, year, and month. Supports fuzzy text search for names, handling accents and special characters.
- 🕵️ **Data Auditing:** Checks for multiple concurrent public jobs, potential nepotism (surname matching), and salary anomalies.

## Project Structure

```text
visor-sueldos-publicos/
├── app.py                        # Streamlit entrypoint
├── Dockerfile                    # Multi-stage Docker build
├── uv.lock / pyproject.toml      # Dependency management
├── scripts/
│   ├── run_senado_extractor.py   # Senate extraction orchestrator
│   └── run_diputados_extractor.py# Chamber extraction orchestrator
├── docs/                         # Technical documentation
│   ├── api-analysis-senate.md
│   └── api-analysis-camara.md
├── src/
│   ├── core/
│   │   ├── api_client.py         # HTTP client with exponential backoff
│   │   ├── config.py             # Global configurations
│   │   ├── logger.py             # Structured logging
│   │   └── queries.py            # DuckDB SQL queries
│   ├── etl/                      # Data pipeline
│   │   ├── ingest.py             # CSV to Parquet conversion
│   │   ├── senado_processor.py   # Senate data normalization
│   │   ├── senado_scraper.py     # Senate API extraction
│   │   ├── diputados_processor.py# Chamber data normalization
│   │   ├── diputados_scraper.py  # Chamber stateful extraction
│   │   └── sync.py               # HTTP HEAD sync logic (CPLT)
│   ├── audits/
│   │   └── audit_utils.py        # Anomaly detection logic
│   └── ui/
│       └── views.py              # UI components and Plotly charts
└── .github/workflows/            # CI/CD (Linter, Data Sync)
```

## Local Development

We use `uv` for dependency management to ensure fast, deterministic builds.

### 1. Clone the repository
```bash
git clone https://github.com/felipe-veas/visor-sueldos-publicos.git
cd visor-sueldos-publicos
```

### 2. Run the ETL Pipelines
You need to process the raw public data before running the frontend locally, unless you want to rely on the remote data fallback.

```bash
# Sync Transparency Council data (large CSVs)
uv run python src/etl/sync.py
uv run python src/etl/ingest.py

# Extract and process Senate data (REST API)
uv run python scripts/run_senado_extractor.py

# Extract and process Chamber of Deputies data (Stateful HTML)
uv run python scripts/run_diputados_extractor.py
```

### 3. Start the Web App
```bash
uv run streamlit run app.py
```

## Production Deployment

The repository includes a production-ready `Dockerfile`. It uses a multi-stage build, runs as a non-root user, and precompiles Python bytecode.

```bash
docker build -t visor-sueldos .
docker run -p 8501:8501 visor-sueldos
```

## Serverless Architecture

The application does not require local storage (`data/`). In serverless environments, it falls back to querying static Parquet files hosted in GitHub Releases (`latest-data`).

1. The `.github/workflows/data-sync.yml` workflow runs on a schedule. It orchestrates the scrapers, packages the Parquet partitions, and publishes a new GitHub Release.
2. DuckDB issues HTTP Range Requests against the GitHub Release URLs. It fetches only the byte ranges required to execute the SQL query, keeping response times in the low milliseconds without downloading full datasets.

## Testing and Linting

Run the validation suite locally:

```bash
# Lint code (Ruff)
uv run ruff check .
```

## Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/new-audit-rule`).
3. Commit your changes. Code and documentation should be in English; UI elements remain in Spanish.
4. Validate with `uv run ruff check .`.
5. Open a Pull Request.

## License

Open-source project. If you use this tooling or architecture for academic or journalistic research, attribution is appreciated.
