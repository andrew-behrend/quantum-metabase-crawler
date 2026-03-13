# Quantum Metabase Crawler

Local-first Metabase crawler and analysis pipeline.

This project:
- crawls a Metabase instance API
- stores raw JSON outputs
- ingests into DuckDB
- produces multiple analysis/reporting layers (definitions, candidates, dictionary, completeness)

## Prerequisites
- Python 3.9+
- A reachable Metabase instance (local or remote)

## Install
```bash
python3 -m pip install -r requirements.txt
```

## Configuration
Create `.env` with:
```env
METABASE_BASE_URL=http://localhost:3000
METABASE_USERNAME=your-email@example.com
METABASE_PASSWORD=your-password
OUTPUT_DIR=./output
```

Optional crawler reliability controls:
```env
METABASE_AUTH_TIMEOUT_SECONDS=30
METABASE_REQUEST_TIMEOUT_SECONDS=60
METABASE_MAX_RETRIES=2
METABASE_BACKOFF_SECONDS=1.0
```

## Run
End-to-end crawl + ingestion:
```bash
python3 crawler.py
```

Analysis scripts (run after crawl):
```bash
python3 analyze_duckdb.py
python3 analyze_definitions.py
python3 analyze_candidates.py
python3 analyze_dictionary.py
python3 analyze_completeness.py
```

## Outputs
- Raw API payloads: `output/raw/`
- Crawl metadata: `output/metadata/`
- DuckDB database: `output/analysis/metabase.duckdb`
- Report CSV/JSON files: `output/analysis/reports/`
- Reusable SQL query files: `output/analysis/queries/`

## Error Handling / Exit Codes
`crawler.py` uses categorized error handling and non-zero exits:
- `2` config errors
- `3` auth errors
- `4` network failures
- `5` API failures
- `6` filesystem/write failures
- `11` partial run with recorded issues

Run details include:
- `run_id`
- start/end timestamps
- duration
- request status table (phase/target/status/attempts/error)

Stored in:
- `output/metadata/crawl-report.json`

## Tests
Run basic tests:
```bash
python3 -m unittest discover -s tests -p "test_*.py"
```
