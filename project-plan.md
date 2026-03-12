# Metabase crawler - phase 1

## Status
Implemented

## objective
Build a simple local crawler for a local Metabase instance running at http://localhost:3000.

## phase 1 scope
The crawler must only:

1. authenticate to the Metabase API
2. retrieve top-level inventory objects
3. save raw JSON responses locally
4. keep the code simple and readable

## explicitly in scope
- session-based authentication
- calling a small number of Metabase API endpoints
- writing responses to disk
- organizing output into folders

## explicitly out of scope
- no UI
- no database
- no ontology layer
- no deduping logic
- no semantic reconciliation
- no retirement recommendations
- no analysis scoring
- no production hardening

## first target outputs
The project should be able to retrieve and save raw JSON for:
- session info as needed
- databases
- collections
- dashboards
- cards / questions

## design preference
Keep it minimal, transparent, and easy to inspect.

## Implementation summary
- implemented in Python (`crawler.py`)
- loads config from `.env`
- authenticates with `/api/session`
- retrieves top-level inventory endpoints (`/api/database`, `/api/collection`, `/api/dashboard`, `/api/card`)
- writes raw outputs to `output/raw`
- writes per-endpoint metadata to `output/metadata`

# Phase 2

## Status
Implemented

## Scope
Extend retrieval from top-level inventory to structural metadata for databases, tables, and fields.

## In-scope
- database detail metadata
- tables within databases
- fields within tables

## Out-of-scope
- cards/questions detail enrichment
- dashboards detail enrichment
- collections detail enrichment
- analysis-layer ingestion
- duplicate detection
- retirement scoring
- semantic reconciliation
- formalized error-handling framework

## Inputs/config
- none beyond phase 1

## Required behaviors
- retrieve database detail for each discovered database
- retrieve table metadata for each discovered database
- retrieve field metadata for each discovered table
- store the retrieved raw responses in a predictable structure
- preserve identifiers needed for later joining across databases, tables, and fields

## Expected outputs/files
- raw database detail files
- raw table metadata files
- raw field metadata files

## Error handling expectations
- basic visibility only

## Constraints/non-goals
- do not redesign prior phases
- do not build the analysis layer yet
- do not normalize the retrieved metadata yet
- keep the phase focused on metadata capture only

## Implementation summary
- retrieves `/api/database/{id}/metadata` for each discovered database
- stores database detail files in `output/raw/database_details/{database_id}.json`
- stores table metadata files in `output/raw/table_metadata/{table_id}.json`
- stores field metadata files in `output/raw/field_metadata/{table_id}.json`
- excludes hidden/archived entities from table/field outputs
- continues on request-level errors and writes end-of-run report to `output/metadata/crawl-report.json`

## Phase 3

### Status
Implemented

### Scope
Expand retrieval from structural metadata to analytical-object metadata for the assets built on top of the data model.

### In-scope
- cards/questions in more detail
- dashboards in more detail
- collections in more detail
- relationships between these objects where available
- preserving identifiers that link analytical objects back to databases, tables, and fields

### Out-of-scope
- duplicate detection
- retirement scoring
- semantic reconciliation
- recommendations
- analysis-layer design/build
- formalized error-handling framework
- historical snapshotting/versioning

### Inputs/config
- none beyond prior phases

### Required behaviors
- retrieve detailed metadata for cards/questions discovered in phase 1
- retrieve detailed metadata for dashboards discovered in phase 1
- retrieve detailed metadata for collections discovered in phase 1
- capture object-to-object relationships where available, especially:
  - dashboard to cards
  - collection to contents
  - cards/questions to referenced database/table/field ids where exposed
- preserve raw responses in a predictable structure
- exclude hidden/archived entities where supported

### Expected outputs/files
- raw card/question detail files
- raw dashboard detail files
- raw collection detail files
- raw relationship-oriented outputs only if the API returns them directly; otherwise keep the source objects separate

### Error handling expectations
- basic visibility only
- continue and report

### Constraints/non-goals
- do not analyze yet
- do not score yet
- do not infer semantics beyond returned metadata
- keep retrieval simple and inspectable

### Implementation summary
- retrieves card details via `/api/card/{id}` for discovered cards/questions
- retrieves dashboard details via `/api/dashboard/{id}` for discovered dashboards
- retrieves collection details via `/api/collection/{id}` and collection contents via `/api/collection/{id}/items`
- stores detail outputs in:
  - `output/raw/card_details/{card_id}.json`
  - `output/raw/dashboard_details/{dashboard_id}.json`
  - `output/raw/collection_details/{collection_id}.json`
  - `output/raw/collection_items/{collection_id}.json`
- stores relationship outputs in:
  - `output/raw/relationships/dashboard_to_cards.json`
  - `output/raw/relationships/collection_to_contents.json`
  - `output/raw/relationships/card_to_data_model.json`
- excludes hidden/archived entities where supported
- continues on request-level errors and reports run status in `output/metadata/crawl-report.json`

## Phase 4

### Status
Implemented

### Scope
Implement a local DuckDB-based analysis layer to support retrieve-once, query-many analysis of the metadata collected in earlier phases.

### In-scope
- establishing DuckDB as the local analysis store
- creating the initial DuckDB database file
- defining the initial table structure for ingested Metabase metadata
- loading prior phase raw outputs into DuckDB
- preserving object ids needed to join across entity types
- preserving enough source context to trace records back to raw files

### Out-of-scope
- new API retrieval
- duplicate detection
- retirement scoring
- semantic reconciliation
- recommendations
- advanced lineage inference
- historical snapshotting/versioning
- formalized error-handling framework

### Inputs/config
- DuckDB database file location
- prior phase raw outputs

### Required behaviors
- create a local DuckDB database for analysis
- ingest prior raw outputs into DuckDB tables
- preserve stable Metabase ids across entities
- support joins across:
  - databases
  - tables
  - fields
  - cards/questions
  - dashboards
  - collections
  - selected relationship outputs from prior phases
- keep ingestion rerunnable as prior phase outputs expand
- keep the structure simple and inspectable

### Expected outputs/files
- a local DuckDB database file
- ingested tables for core entity types
- ingested tables for selected relationships
- basic documentation in `project-plan.md` of what is stored in DuckDB

### Error handling expectations
- basic visibility only
- continue and report where practical

### Constraints/non-goals
- raw JSON remains the collection system of record
- DuckDB is the local analysis layer, not the source of truth
- do not redesign prior retrieval phases
- do not begin scoring or recommendation logic yet
- do not over-model the schema too early
- optimize for lightweight local analysis

### Implementation summary
- creates a local DuckDB database at `output/analysis/metabase.duckdb`
- ingestion is full-refresh/rerunnable (tables are rebuilt each run)
- ingests flattened core entities from prior raw outputs with source-file traceability (`source_file`, `ingested_at`)
- ingests all selected relationship outputs from phase 3

### DuckDB tables
- `databases`:
  - `database_id`, `name`, `engine`, `description`, `initial_sync_status`, `is_sample`, `is_audit`, `created_at`, `updated_at`, `source_file`, `ingested_at`
- `tables`:
  - `table_id`, `database_id`, `schema_name`, `name`, `display_name`, `entity_type`, `description`, `active`, `visibility_type`, `created_at`, `updated_at`, `source_file`, `ingested_at`
- `fields`:
  - `field_id`, `table_id`, `fk_target_field_id`, `name`, `display_name`, `base_type`, `effective_type`, `semantic_type`, `description`, `active`, `visibility_type`, `created_at`, `updated_at`, `source_file`, `ingested_at`
- `cards`:
  - `card_id`, `entity_id`, `name`, `description`, `card_type`, `query_type`, `database_id`, `table_id`, `collection_id`, `dashboard_id`, `archived`, `created_at`, `updated_at`, `source_file`, `ingested_at`
- `dashboards`:
  - `dashboard_id`, `entity_id`, `name`, `description`, `collection_id`, `archived`, `dashcard_count`, `created_at`, `updated_at`, `source_file`, `ingested_at`
- `collections`:
  - `collection_id`, `entity_id`, `name`, `description`, `parent_id`, `location`, `is_personal`, `archived`, `created_at`, `source_file`, `ingested_at`
- `rel_dashboard_to_cards`:
  - `dashboard_id`, `card_id`, `source_file`, `ingested_at`
- `rel_collection_to_contents`:
  - `collection_id`, `item_id`, `item_type`, `source_file`, `ingested_at`
- `rel_card_to_data_model`:
  - `card_id`, `database_id`, `table_id`, `field_ids_json`, `source_file`, `ingested_at`
- `rel_card_to_fields`:
  - `card_id`, `field_id`, `source_file`, `ingested_at`

## Phase 5

### Status
Implemented

### Scope
Create the first analysis outputs from the DuckDB layer to make the retrieved Metabase metadata understandable and useful for audit work.

### In-scope
- profiling the contents of the DuckDB layer
- producing summary outputs for:
  - databases
  - tables
  - fields
  - cards/questions
  - dashboards
  - collections
  - relationship coverage
- identifying obvious structural patterns such as:
  - orphaned objects
  - unused objects where relationships indicate no usage
  - concentration of usage by database/table
  - naming collisions and near-duplicate names
- generating analysis outputs that can be reviewed locally without rereading raw JSON

### Out-of-scope
- retirement recommendations
- duplicate resolution
- semantic reconciliation
- business-rule interpretation
- scoring models
- automated lineage inference beyond retrieved relationships
- formalized error-handling framework

### Inputs/config
- DuckDB analysis store from phase 4

### Required behaviors
- query DuckDB rather than raw JSON
- produce clear summary outputs that describe what exists and how it is connected
- identify obviously overlapping or potentially duplicative objects based on names and relationships
- identify objects with no observed relationships where that is meaningful
- preserve object ids in all outputs so findings can be traced back to source objects
- keep analysis logic transparent and inspectable

### Expected outputs/files
- summary tables or files for entity counts and coverage
- outputs listing potential duplicates by name or similar naming
- outputs listing potentially unused/orphaned objects
- outputs showing key relationships, such as:
  - dashboards to cards
  - collections to contents
  - cards to underlying data model objects
- a small set of reusable analysis queries or scripts

### Error handling expectations
- basic visibility only
- continue and report where practical

### Constraints/non-goals
- findings are signals, not decisions
- do not automatically recommend deletion or retirement yet
- do not infer semantic equivalence from names alone
- optimize for surfacing candidates for review
- keep the logic simple enough to validate manually

### Implementation summary
- implemented as a separate analysis script: `analyze_duckdb.py` (kept separate from retrieval crawler)
- queries DuckDB at `output/analysis/metabase.duckdb` and writes report outputs to `output/analysis/reports`
- writes reusable SQL files to `output/analysis/queries`
- uses simple normalization for name-duplicate signals (lowercase/trim/punctuation-space normalization)
- preserves ids in outputs for traceability and manual follow-up
- continues across report-level failures and writes a high-level run summary

### Analysis outputs
- high-level summary:
  - `output/analysis/reports/summary_overview.json`
- focused report files:
  - `output/analysis/reports/entity_counts.csv`
  - `output/analysis/reports/relationship_coverage.csv`
  - `output/analysis/reports/potential_name_duplicates.csv`
  - `output/analysis/reports/potential_orphans.csv`
  - `output/analysis/reports/usage_concentration_by_database.csv`
  - `output/analysis/reports/usage_concentration_by_table.csv`
  - `output/analysis/reports/relationship_dashboards_to_cards.csv`
  - `output/analysis/reports/relationship_collections_to_contents.csv`
  - `output/analysis/reports/relationship_cards_to_data_model.csv`
