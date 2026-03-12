from __future__ import annotations

import csv
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from dotenv import dotenv_values


@dataclass(frozen=True)
class AnalysisIssue:
    report_name: str
    error: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_name(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    lowered = value.strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def normalize_sql(value: str) -> str:
    normalized = value.strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def json_text(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=True)


def json_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def write_csv(path: Path, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def run_query_to_csv(
    con: duckdb.DuckDBPyConnection,
    report_name: str,
    sql: str,
    reports_dir: Path,
    issues: list[AnalysisIssue],
) -> dict[str, Any]:
    output_file = reports_dir / f"{report_name}.csv"
    try:
        result = con.execute(sql)
        rows = result.fetchall()
        columns = [column[0] for column in result.description]
        write_csv(output_file, columns, rows)
        print(f"Wrote {output_file} ({len(rows)} rows)")
        return {"file": str(output_file), "row_count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        issues.append(AnalysisIssue(report_name, str(exc)))
        print(f"Warning: failed report {report_name}: {exc}", file=sys.stderr)
        return {"file": str(output_file), "row_count": 0}


def to_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def walk_dataset_query(node: Any, table_ids: set[int], field_ids: set[int]) -> None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "source-table":
                int_value = to_int(value)
                if int_value is not None:
                    table_ids.add(int_value)
            walk_dataset_query(value, table_ids, field_ids)
        return

    if isinstance(node, list):
        if len(node) >= 2 and isinstance(node[0], str):
            if node[0] == "field":
                int_value = to_int(node[1])
                if int_value is not None:
                    field_ids.add(int_value)
            if node[0] in {"field-id", "field_id"}:
                int_value = to_int(node[1])
                if int_value is not None:
                    field_ids.add(int_value)

        for item in node:
            walk_dataset_query(item, table_ids, field_ids)


def build_card_definition_rows(
    con: duckdb.DuckDBPyConnection,
    cards_dir: Path,
    ingested_at: str,
) -> list[tuple[Any, ...]]:
    rel_field_map: dict[int, list[int]] = {}
    try:
        rel_rows = con.execute(
            """
            SELECT card_id, LIST(DISTINCT field_id ORDER BY field_id) AS field_ids
            FROM rel_card_to_fields
            GROUP BY card_id
            """
        ).fetchall()
        for card_id, field_ids in rel_rows:
            if isinstance(card_id, int) and isinstance(field_ids, list):
                rel_field_map[card_id] = [fid for fid in field_ids if isinstance(fid, int)]
    except Exception:
        rel_field_map = {}

    rows: list[tuple[Any, ...]] = []

    for card_file in sorted(cards_dir.glob("*.json")):
        payload = json.loads(card_file.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue

        card_id = to_int(payload.get("id"))
        if card_id is None:
            continue

        card_name = payload.get("name")
        normalized_card_name = normalize_name(card_name)
        query_type = payload.get("query_type")

        dataset_query = payload.get("dataset_query")
        if not isinstance(dataset_query, dict):
            dataset_query = {}

        native = dataset_query.get("native")
        sql_text = None
        if isinstance(native, dict):
            sql_candidate = native.get("query")
            if isinstance(sql_candidate, str) and sql_candidate.strip():
                sql_text = sql_candidate

        has_sql = sql_text is not None

        stages = dataset_query.get("stages")
        legacy_query = dataset_query.get("query")
        has_notebook_structure = isinstance(stages, list) or isinstance(legacy_query, dict)

        notebook_stage_count = len(stages) if isinstance(stages, list) else 0
        notebook_aggregation_count = 0
        notebook_breakout_count = 0
        notebook_filter_count = 0

        if isinstance(stages, list):
            for stage in stages:
                if not isinstance(stage, dict):
                    continue
                agg = stage.get("aggregation")
                if isinstance(agg, list):
                    notebook_aggregation_count += len(agg)
                breakout = stage.get("breakout")
                if isinstance(breakout, list):
                    notebook_breakout_count += len(breakout)
                filters = stage.get("filters")
                if isinstance(filters, list):
                    notebook_filter_count += len(filters)

        if isinstance(legacy_query, dict):
            agg = legacy_query.get("aggregation")
            if isinstance(agg, list):
                notebook_aggregation_count += len(agg)
            breakout = legacy_query.get("breakout")
            if isinstance(breakout, list):
                notebook_breakout_count += len(breakout)
            legacy_filter = legacy_query.get("filter")
            if isinstance(legacy_filter, list):
                notebook_filter_count += 1

        table_ids: set[int] = set()
        field_ids: set[int] = set()

        walk_dataset_query(dataset_query, table_ids, field_ids)

        card_table_id = to_int(payload.get("table_id"))
        if card_table_id is not None:
            table_ids.add(card_table_id)

        for rel_field_id in rel_field_map.get(card_id, []):
            field_ids.add(rel_field_id)

        referenced_table_ids = sorted(table_ids)
        referenced_field_ids = sorted(field_ids)

        sql_normalized = normalize_sql(sql_text) if sql_text else None
        sql_hash = json_hash(sql_normalized) if sql_normalized else None

        notebook_structure = dataset_query if has_notebook_structure else None
        notebook_structure_json = json_text(notebook_structure) if notebook_structure is not None else None
        notebook_hash = json_hash(notebook_structure_json) if notebook_structure_json else None

        if has_sql:
            logic_type = "native_sql"
        elif has_notebook_structure:
            logic_type = "notebook"
        else:
            logic_type = "unknown"

        database_id = to_int(payload.get("database_id"))
        reference_signature = json_hash(
            json_text(
                {
                    "logic_type": logic_type,
                    "database_id": database_id,
                    "table_ids": referenced_table_ids,
                    "field_ids": referenced_field_ids,
                }
            )
        )

        rows.append(
            (
                card_id,
                card_name,
                normalized_card_name,
                query_type,
                logic_type,
                has_sql,
                sql_text,
                sql_normalized,
                sql_hash,
                has_notebook_structure,
                notebook_stage_count,
                notebook_aggregation_count,
                notebook_breakout_count,
                notebook_filter_count,
                notebook_structure_json,
                notebook_hash,
                database_id,
                card_table_id,
                json_text(referenced_table_ids),
                json_text(referenced_field_ids),
                reference_signature,
                str(card_file),
                ingested_at,
            )
        )

    return rows


def main() -> int:
    env = dotenv_values(".env")
    output_dir_raw = env.get("OUTPUT_DIR")
    if not output_dir_raw:
        print("Configuration error: OUTPUT_DIR is missing in .env", file=sys.stderr)
        return 1

    output_dir = Path(output_dir_raw)
    duckdb_path = output_dir / "analysis" / "metabase.duckdb"
    reports_dir = output_dir / "analysis" / "reports"
    queries_dir = output_dir / "analysis" / "queries"
    cards_dir = output_dir / "raw" / "card_details"

    reports_dir.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_path.exists():
        print(f"Error: DuckDB file not found: {duckdb_path}", file=sys.stderr)
        return 1

    if not cards_dir.exists():
        print(f"Error: card detail directory not found: {cards_dir}", file=sys.stderr)
        return 1

    issues: list[AnalysisIssue] = []
    ingested_at = utc_now_iso()

    con = duckdb.connect(str(duckdb_path))
    try:
        rows = build_card_definition_rows(con, cards_dir, ingested_at)

        con.execute("DROP TABLE IF EXISTS card_definitions")
        con.execute(
            """
            CREATE TABLE card_definitions (
                card_id BIGINT,
                card_name VARCHAR,
                normalized_card_name VARCHAR,
                query_type VARCHAR,
                logic_type VARCHAR,
                has_sql BOOLEAN,
                sql_text VARCHAR,
                sql_normalized VARCHAR,
                sql_hash VARCHAR,
                has_notebook_structure BOOLEAN,
                notebook_stage_count BIGINT,
                notebook_aggregation_count BIGINT,
                notebook_breakout_count BIGINT,
                notebook_filter_count BIGINT,
                notebook_structure_json VARCHAR,
                notebook_hash VARCHAR,
                database_id BIGINT,
                card_table_id BIGINT,
                referenced_table_ids_json VARCHAR,
                referenced_field_ids_json VARCHAR,
                reference_signature VARCHAR,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )

        if rows:
            con.executemany(
                "INSERT INTO card_definitions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

        queries: dict[str, str] = {
            "phase6_native_vs_notebook": """
                SELECT
                    logic_type,
                    query_type,
                    COUNT(*) AS card_count,
                    SUM(CASE WHEN has_sql THEN 1 ELSE 0 END) AS cards_with_sql,
                    SUM(CASE WHEN has_notebook_structure THEN 1 ELSE 0 END) AS cards_with_notebook_structure
                FROM card_definitions
                GROUP BY logic_type, query_type
                ORDER BY card_count DESC, logic_type, query_type;
            """,
            "phase6_similar_names_different_logic_types": """
                WITH name_groups AS (
                    SELECT
                        normalized_card_name,
                        COUNT(*) AS card_count,
                        COUNT(DISTINCT logic_type) AS distinct_logic_types
                    FROM card_definitions
                    WHERE normalized_card_name <> ''
                    GROUP BY normalized_card_name
                    HAVING COUNT(*) > 1 AND COUNT(DISTINCT logic_type) > 1
                )
                SELECT
                    ng.normalized_card_name,
                    cd.card_id,
                    cd.card_name,
                    cd.logic_type,
                    cd.query_type,
                    cd.database_id,
                    cd.card_table_id
                FROM name_groups ng
                JOIN card_definitions cd ON cd.normalized_card_name = ng.normalized_card_name
                ORDER BY ng.normalized_card_name, cd.logic_type, cd.card_id;
            """,
            "phase6_similar_names_similar_references": """
                WITH grouped AS (
                    SELECT
                        normalized_card_name,
                        database_id,
                        referenced_table_ids_json,
                        referenced_field_ids_json,
                        COUNT(*) AS card_count,
                        string_agg(card_id::VARCHAR, ', ') AS card_ids
                    FROM card_definitions
                    WHERE normalized_card_name <> ''
                    GROUP BY normalized_card_name, database_id, referenced_table_ids_json, referenced_field_ids_json
                    HAVING COUNT(*) > 1
                )
                SELECT *
                FROM grouped
                ORDER BY card_count DESC, normalized_card_name;
            """,
            "phase6_similar_names_divergent_definitions": """
                WITH name_groups AS (
                    SELECT
                        normalized_card_name,
                        COUNT(*) AS card_count,
                        COUNT(DISTINCT logic_type) AS distinct_logic_types,
                        COUNT(DISTINCT COALESCE(sql_hash, '')) AS distinct_sql_hashes,
                        COUNT(DISTINCT COALESCE(notebook_hash, '')) AS distinct_notebook_hashes,
                        COUNT(DISTINCT reference_signature) AS distinct_reference_signatures
                    FROM card_definitions
                    WHERE normalized_card_name <> ''
                    GROUP BY normalized_card_name
                    HAVING COUNT(*) > 1
                       AND (
                            COUNT(DISTINCT logic_type) > 1
                            OR COUNT(DISTINCT COALESCE(sql_hash, '')) > 1
                            OR COUNT(DISTINCT COALESCE(notebook_hash, '')) > 1
                            OR COUNT(DISTINCT reference_signature) > 1
                       )
                )
                SELECT
                    ng.normalized_card_name,
                    ng.card_count,
                    ng.distinct_logic_types,
                    ng.distinct_sql_hashes,
                    ng.distinct_notebook_hashes,
                    ng.distinct_reference_signatures,
                    cd.card_id,
                    cd.card_name,
                    cd.logic_type,
                    cd.database_id,
                    cd.card_table_id,
                    cd.referenced_table_ids_json,
                    cd.referenced_field_ids_json
                FROM name_groups ng
                JOIN card_definitions cd ON cd.normalized_card_name = ng.normalized_card_name
                ORDER BY ng.normalized_card_name, cd.card_id;
            """,
        }

        for name, sql in queries.items():
            (queries_dir / f"{name}.sql").write_text(sql.strip() + "\n", encoding="utf-8")

        outputs: dict[str, dict[str, Any]] = {}
        for report_name, sql in queries.items():
            outputs[report_name] = run_query_to_csv(con, report_name, sql, reports_dir, issues)

        summary = {
            "generated_at": utc_now_iso(),
            "duckdb_file": str(duckdb_path),
            "card_definition_rows": len(rows),
            "report_count": len(queries),
            "successful_report_count": len(queries) - len(issues),
            "issue_count": len(issues),
            "issues": [{"report_name": i.report_name, "error": i.error} for i in issues],
            "outputs": outputs,
        }

        summary_json_path = reports_dir / "phase6_summary_overview.json"
        summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"Wrote {summary_json_path}")

        summary_rows: list[tuple[Any, ...]] = []
        issue_map = {issue.report_name: issue.error for issue in issues}
        for report_name, output in outputs.items():
            summary_rows.append(
                (
                    report_name,
                    output.get("file"),
                    output.get("row_count"),
                    "error" if report_name in issue_map else "ok",
                    issue_map.get(report_name, ""),
                )
            )

        summary_csv_path = reports_dir / "phase6_summary_outputs.csv"
        write_csv(
            summary_csv_path,
            ["report_name", "file", "row_count", "status", "error"],
            summary_rows,
        )
        print(f"Wrote {summary_csv_path}")

    finally:
        con.close()

    if issues:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
