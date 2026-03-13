# Metabase crawler

## objective
Build a simple local crawler for a local Metabase instance running at http://localhost:3000.

## Phase 1

### status
Implemented

### scope
The crawler must only:

1. authenticate to the Metabase API
2. retrieve top-level inventory objects
3. save raw JSON responses locally
4. keep the code simple and readable

### explicitly in scope
- session-based authentication
- calling a small number of Metabase API endpoints
- writing responses to disk
- organizing output into folders

### explicitly out of scope
- no UI
- no database
- no ontology layer
- no deduping logic
- no semantic reconciliation
- no retirement recommendations
- no analysis scoring
- no production hardening

### first target outputs
The project should be able to retrieve and save raw JSON for:
- session info as needed
- databases
- collections
- dashboards
- cards / questions

### design preference
Keep it minimal, transparent, and easy to inspect.

### Implementation summary
- implemented in Python (`crawler.py`)
- loads config from `.env`
- authenticates with `/api/session`
- retrieves top-level inventory endpoints (`/api/database`, `/api/collection`, `/api/dashboard`, `/api/card`)
- writes raw outputs to `output/raw`
- writes per-endpoint metadata to `output/metadata`

## Phase 2

### Status
Implemented

### Scope
Extend retrieval from top-level inventory to structural metadata for databases, tables, and fields.

### In-scope
- database detail metadata
- tables within databases
- fields within tables

### Out-of-scope
- cards/questions detail enrichment
- dashboards detail enrichment
- collections detail enrichment
- analysis-layer ingestion
- duplicate detection
- retirement scoring
- semantic reconciliation
- formalized error-handling framework

### Inputs/config
- none beyond phase 1

### Required behaviors
- retrieve database detail for each discovered database
- retrieve table metadata for each discovered database
- retrieve field metadata for each discovered table
- store the retrieved raw responses in a predictable structure
- preserve identifiers needed for later joining across databases, tables, and fields

### Expected outputs/files
- raw database detail files
- raw table metadata files
- raw field metadata files

### Error handling expectations
- basic visibility only

### Constraints/non-goals
- do not redesign prior phases
- do not build the analysis layer yet
- do not normalize the retrieved metadata yet
- keep the phase focused on metadata capture only

### Implementation summary
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
  - `output/analysis/reports/summary_outputs.csv`
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

## Phase 6

### Status
Implemented

### Scope
Enrich the analysis layer with question-definition logic so Metabase assets can be compared based on how they are built, not just what they are named.

### In-scope
- extracting comparable definition details from cards/questions
- distinguishing native SQL questions from notebook/GUI questions
- capturing SQL text where present
- capturing notebook query structure where present
- deriving comparison-friendly signals from question definitions
- linking those signals back to cards, dashboards, collections, databases, tables, and fields

### Out-of-scope
- final duplicate decisions
- final retirement recommendations
- semantic reconciliation across business terms
- formal lineage reconstruction beyond what is directly available
- error-handling redesign

### Inputs/config
- DuckDB analysis store from prior phases
- raw card/question detail outputs from phase 3

### Required behaviors
- extract query-definition attributes from cards/questions into analysis-friendly structures
- preserve whether a card is native SQL or notebook/GUI
- preserve raw SQL text where available
- preserve notebook query structure in a comparison-friendly form where available
- create derived comparison signals such as:
  - normalized card name
  - query type
  - referenced database/table ids
  - referenced field ids where available
  - SQL presence flag
  - notebook-structure presence flag
- support later comparison of cards that may represent the same metric or business concept

### Expected outputs/files
- new DuckDB tables or columns for question-definition analysis
- report outputs summarizing:
  - native SQL vs notebook cards
  - cards with similar names but different logic types
  - cards with similar names and similar data-model references
  - cards with similar names but divergent definitions
- reusable SQL queries for these comparisons

### Error handling expectations
- basic visibility only
- continue and report

### Constraints/non-goals
- do not make final duplicate judgments yet
- do not infer business equivalence from names alone
- optimize for exposing definitional comparison signals
- keep the logic inspectable and reviewable

### Implementation summary
- implemented as a separate script: `analyze_definitions.py`
- adds a full-refresh DuckDB table `card_definitions` in `output/analysis/metabase.duckdb`
- extracts expanded question-definition signals from raw card details, including:
  - normalized card name
  - query and logic type (`native_sql` vs `notebook` vs `unknown`)
  - SQL presence/text/normalized text/hash
  - notebook structure presence and expanded counts (stages, aggregations, breakouts, filters)
  - referenced database/table/field ids and derived reference signature
- keeps outputs traceable with `card_id`, `source_file`, and `ingested_at`
- writes reusable phase 6 SQL query files and CSV reports under existing analysis output folders

### DuckDB additions
- `card_definitions`:
  - `card_id`, `card_name`, `normalized_card_name`, `query_type`, `logic_type`, `has_sql`, `sql_text`, `sql_normalized`, `sql_hash`, `has_notebook_structure`, `notebook_stage_count`, `notebook_aggregation_count`, `notebook_breakout_count`, `notebook_filter_count`, `notebook_structure_json`, `notebook_hash`, `database_id`, `card_table_id`, `referenced_table_ids_json`, `referenced_field_ids_json`, `reference_signature`, `source_file`, `ingested_at`

### Analysis outputs
- high-level summary:
  - `output/analysis/reports/phase6_summary_overview.json`
  - `output/analysis/reports/phase6_summary_outputs.csv`
- focused report files:
  - `output/analysis/reports/phase6_native_vs_notebook.csv`
  - `output/analysis/reports/phase6_similar_names_different_logic_types.csv`
  - `output/analysis/reports/phase6_similar_names_similar_references.csv`
  - `output/analysis/reports/phase6_similar_names_divergent_definitions.csv`
- reusable queries:
  - `output/analysis/queries/phase6_native_vs_notebook.sql`
  - `output/analysis/queries/phase6_similar_names_different_logic_types.sql`
  - `output/analysis/queries/phase6_similar_names_similar_references.sql`
  - `output/analysis/queries/phase6_similar_names_divergent_definitions.sql`

## Phase 7

### Status
Implemented

### Scope
Create the first candidate review layer for duplicate, conflicting, and potentially retireable Metabase assets using the structural, relationship, and definition signals produced in phases 1–6.

### In-scope
- grouping cards/questions into candidate comparison sets
- surfacing candidate duplicate assets
- surfacing candidate conflicting-definition assets
- surfacing candidate retirement/cleanup assets
- combining signals from:
  - naming similarity
  - shared data-model references
  - query type
  - SQL/notebook definition characteristics
  - dashboard usage
  - collection placement
  - orphan/unused signals from phase 5
- producing review-friendly outputs for human inspection

### Out-of-scope
- automatic deletion or retirement actions
- final semantic reconciliation
- definitive business-meaning judgments
- UI buildout
- error-handling redesign
- historical version comparison across runs

### Inputs/config
- DuckDB analysis store from phases 4–6
- phase 5 report outputs, where useful

### Required behaviors
- generate candidate duplicate groups for cards/questions based on multiple signals, not names alone
- generate candidate conflicting-definition groups where similar names map to materially different logic
- generate candidate retirement/cleanup groups for assets that appear unused, orphaned, redundant, or superseded
- preserve the underlying evidence for each candidate signal so findings are reviewable
- assign each candidate finding a signal type or rationale, such as:
  - same normalized name
  - same table/field footprint
  - same name but different query logic
  - no dashboard usage
  - no collection usage
  - archived/hidden mismatch where relevant
- keep outputs traceable back to object ids and source assets

### Expected outputs/files
- candidate duplicate report files
- candidate conflicting-definition report files
- candidate retirement/cleanup report files
- one high-level summary file describing counts by candidate type and signal type
- reusable SQL queries or analysis scripts supporting those outputs

### Error handling expectations
- basic visibility only
- continue and report

### Constraints/non-goals
- findings are review candidates, not final decisions
- do not infer semantic equivalence from names alone
- do not recommend deletion without preserving supporting evidence
- optimize for human review and audit usefulness
- keep the scoring/grouping logic inspectable and adjustable

### Implementation summary
- implemented as a separate script: `analyze_candidates.py`
- reads from DuckDB (`output/analysis/metabase.duckdb`) and writes candidate outputs to `output/analysis/reports`
- produces deterministic `candidate_group_id` values using stable hashing of core grouping signals
- applies label + numeric score + confidence to candidate findings
- includes retirement/cleanup candidates across cards, dashboards, and collections
- preserves evidence and traceability fields (object ids, names, usage/context signals, rationale text)
- writes reusable phase 7 SQL base-query files to `output/analysis/queries`

### Candidate outputs
- high-level summary:
  - `output/analysis/reports/candidate_summary_overview.json`
  - `output/analysis/reports/candidate_summary_outputs.csv`
- focused candidate reports:
  - `output/analysis/reports/candidate_duplicates.csv`
  - `output/analysis/reports/candidate_conflicting_definitions.csv`
  - `output/analysis/reports/candidate_retirement_cleanup.csv`
  - `output/analysis/reports/candidate_signal_summary.csv`
- reusable queries:
  - `output/analysis/queries/phase7_base_cards.sql`
  - `output/analysis/queries/phase7_base_dashboards.sql`
  - `output/analysis/queries/phase7_base_collections.sql`
  - `output/analysis/queries/phase7_name_groups.sql`

## Phase 8

### Status
Implemented

### Scope
Produce a first data-dictionary layer for Metabase structural metadata so databases, tables, and fields can be reviewed in a human-friendly format.

### In-scope
- database dictionary outputs
- table dictionary outputs
- field dictionary outputs
- flattening key structural metadata into review-friendly outputs
- preserving identifiers and relationships needed to trace fields to tables and tables to databases
- producing outputs suitable for audit and extraction planning

### Out-of-scope
- completeness profiling
- field value profiling
- historical depth analysis
- duplicate detection changes
- retirement scoring changes
- extraction planning
- UI buildout

### Inputs/config
- DuckDB analysis store from prior phases

### Required behaviors
- query DuckDB rather than raw JSON
- produce database, table, and field level dictionary outputs
- include enough metadata to understand:
  - what the object is
  - where it sits
  - how it is typed
  - whether it appears active/visible
  - how it relates to parent objects
- preserve object ids for traceability
- keep outputs easy to inspect and reuse

### Expected outputs/files
- one or more database dictionary outputs
- one or more table dictionary outputs
- one or more field dictionary outputs
- reusable SQL queries supporting those outputs

### Error handling expectations
- basic visibility only
- continue and report where practical

### Constraints/non-goals
- optimize for human review and downstream planning
- do not attempt completeness or value analysis yet
- do not infer business meaning beyond available metadata
- keep the dictionary structure simple and inspectable

### Implementation summary
- implemented as a separate script: `analyze_dictionary.py`
- queries DuckDB (`output/analysis/metabase.duckdb`) and writes dictionary outputs under `output/analysis/reports/dictionary`
- generates one global file each for database, table, and field dictionaries
- generates per-database dictionary files, including expanded field-join variants
- preserves ids and parent relationships for traceability across database/table/field levels
- writes reusable phase 8 SQL query files to `output/analysis/queries`
- writes summary outputs for report tracking

### Dictionary outputs
- global dictionary files:
  - `output/analysis/reports/dictionary/database_dictionary.csv`
  - `output/analysis/reports/dictionary/table_dictionary.csv`
  - `output/analysis/reports/dictionary/field_dictionary.csv`
  - `output/analysis/reports/dictionary/field_dictionary_expanded.csv`
- per-database files:
  - `output/analysis/reports/dictionary/per_database/database_{database_id}_{database_slug}_tables.csv`
  - `output/analysis/reports/dictionary/per_database/database_{database_id}_{database_slug}_fields.csv`
  - `output/analysis/reports/dictionary/per_database/database_{database_id}_{database_slug}_fields_expanded.csv`
  - `output/analysis/reports/dictionary/dictionary_per_database_manifest.csv`
- summary files:
  - `output/analysis/reports/dictionary_summary_overview.json`
  - `output/analysis/reports/dictionary_summary_outputs.csv`
- reusable queries:
  - `output/analysis/queries/phase8_database_dictionary.sql`
  - `output/analysis/queries/phase8_table_dictionary.sql`
  - `output/analysis/queries/phase8_field_dictionary.sql`
  - `output/analysis/queries/phase8_field_dictionary_expanded.sql`

## Phase 9

### Status
Implemented

### Scope
Profile field completeness and observed field values so the crawler moves from structural metadata into basic data-quality and data-meaning assessment.

### In-scope
- field-level completeness profiling
- table-level rollups of completeness results
- observed value profiling for fields where Metabase exposes usable values
- identifying likely categorical fields versus free-text/high-cardinality fields
- producing review-friendly outputs for completeness and observed values

### Out-of-scope
- historical depth analysis
- extraction planning
- business-rule interpretation
- duplicate detection changes
- retirement scoring changes
- UI buildout

### Inputs/config
- DuckDB analysis store from prior phases
- raw field metadata from earlier phases, including any exposed value/fingerprint metadata

### Required behaviors
- query DuckDB rather than raw JSON directly
- calculate field-level completeness signals using available metadata
- produce table-level summaries of completeness
- surface observed/available values for fields where Metabase exposes them
- distinguish between:
  - completeness signals
  - observed value signals
- preserve ids for traceability back to database, table, and field
- keep outputs easy to review and reuse

### Expected outputs/files
- field completeness report outputs
- table completeness summary outputs
- field value profiling outputs
- reusable SQL queries supporting those outputs

### Error handling expectations
- basic visibility only
- continue and report where practical

### Constraints/non-goals
- use only what has already been retrieved or can be derived from the current local analysis layer
- do not infer business meaning beyond the evidence available
- do not treat observed values as authoritative allowed values unless clearly exposed as such
- optimize for data understanding and extraction readiness
- keep the logic simple and inspectable

### Implementation summary
- implemented as a separate script: `analyze_completeness.py`
- extends DuckDB `fields` ingestion in `crawler.py` to include completeness/value signals:
  - `has_field_values`
  - `fingerprint_distinct_count`
  - `fingerprint_nil_count`
  - `fingerprint_nil_pct`
  - `fingerprint_json`
- computes field-level completeness signals using both metadata and null-ratio style fingerprint signals
- computes table-level completeness rollups
- computes observed-value/cardinality profiling signals
- writes global and per-database outputs using `completeness_*` naming
- writes reusable completeness SQL query files

### Completeness outputs
- global outputs:
  - `output/analysis/reports/completeness/completeness_field_profile.csv`
  - `output/analysis/reports/completeness/completeness_table_summary.csv`
  - `output/analysis/reports/completeness/completeness_field_values_profile.csv`
  - `output/analysis/reports/completeness/completeness_field_values_profile_expanded.csv`
  - `output/analysis/reports/completeness/completeness_per_database_manifest.csv`
- per-database outputs:
  - `output/analysis/reports/completeness/per_database/database_{database_id}_{database_slug}_completeness_field_profile.csv`
  - `output/analysis/reports/completeness/per_database/database_{database_id}_{database_slug}_completeness_table_summary.csv`
  - `output/analysis/reports/completeness/per_database/database_{database_id}_{database_slug}_completeness_field_values_profile.csv`
  - `output/analysis/reports/completeness/per_database/database_{database_id}_{database_slug}_completeness_field_values_profile_expanded.csv`
- summary outputs:
  - `output/analysis/reports/completeness_summary_overview.json`
  - `output/analysis/reports/completeness_summary_outputs.csv`
- reusable queries:
  - `output/analysis/queries/completeness_field_profile.sql`
  - `output/analysis/queries/completeness_table_summary.sql`
  - `output/analysis/queries/completeness_field_values_profile.sql`
  - `output/analysis/queries/completeness_field_values_profile_expanded.sql`
