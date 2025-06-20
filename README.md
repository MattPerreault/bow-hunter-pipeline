# Bow Hunter Pipeline

Data pipeline for bow build and GMU datasets, powering a data-driven bow hunter journey.

## Setup

1. Install pyenv: Follow [pyenv installation](https://github.com/pyenv/pyenv#installation).
2. Install Python 3.13.5: `pyenv install 3.13.5`.
3. Create virtualenv: `pyenv virtualenv 3.13.5 bow-hunter`.
4. Activate virtualenv: `pyenv activate bow-hunter`.
5. Install dependencies: `pip install -r requirements.txt`.
6. Run MinIO for local S3: `docker run -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"`.
7. Start FastMCP server: `python src/server.py`.
8. Run Mage pipeline: `mage run . process_data`.

## Structure

- **src/**: Pipeline code (FastMCP server, Mage pipelines, DuckDB queries).
  - **server.py**: FastMCP server for ingestion and querying.
  - **pipelines/process_data.py**: Mage pipeline for data processing.
  - **ingest.py**: Stagehand scraping logic.
  - **query.py**: DuckDB query functions.
- **tests/**: Unit tests.
- **.github/workflows/**: CI/CD with GitHub Actions.

## Notes

- Local setup for PoC; IaC deferred for MVP.
- Targets 10 bow models, 5 GMUs for validation.
- MIT license for open-source contributions.