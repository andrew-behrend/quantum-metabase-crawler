from __future__ import annotations

import csv
import difflib
import json
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


def near_duplicate_rows(
    con: duckdb.DuckDBPyConnection,
    similarity_threshold: float = 0.88,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    candidates = con.execute(
        """
        WITH names AS (
            SELECT 'cards' AS entity_type, card_id::VARCHAR AS entity_id, name FROM cards
            UNION ALL
            SELECT 'dashboards', dashboard_id::VARCHAR, name FROM dashboards
            UNION ALL
            SELECT 'collections', collection_id, name FROM collections
            UNION ALL
            SELECT 'tables', table_id::VARCHAR, name FROM tables
        )
        SELECT entity_type, entity_id, name
        FROM names
        WHERE COALESCE(TRIM(name), '') <> ''
        ORDER BY entity_type, entity_id
        """
    ).fetchall()

    grouped: dict[str, list[tuple[str, str]]] = {}
    for entity_type, entity_id, name in candidates:
        if isinstance(entity_type, str) and isinstance(entity_id, str) and isinstance(name, str):
            grouped.setdefault(entity_type, []).append((entity_id, name))

    rows: list[tuple[Any, ...]] = []
    for entity_type, items in grouped.items():
        for i in range(len(items)):
            left_id, left_name = items[i]
            for j in range(i + 1, len(items)):
                right_id, right_name = items[j]
                ratio = difflib.SequenceMatcher(
                    None, left_name.lower().strip(), right_name.lower().strip()
                ).ratio()
                if ratio >= similarity_threshold and left_name != right_name:
                    rows.append(
                        (
                            entity_type,
                            left_id,
                            left_name,
                            right_id,
                            right_name,
                            round(ratio, 4),
                        )
                    )

    rows.sort(key=lambda r: (r[0], -float(r[5]), r[2], r[4]))
    columns = [
        "entity_type",
        "left_entity_id",
        "left_name",
        "right_entity_id",
        "right_name",
        "similarity_score",
    ]
    return columns, rows


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
    reports_dir.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_path.exists():
        print(f"Error: DuckDB file not found: {duckdb_path}", file=sys.stderr)
        return 1

    queries: dict[str, str] = {
        "entity_counts": """
            SELECT 'databases' AS entity_type, COUNT(*) AS entity_count FROM databases
            UNION ALL SELECT 'tables', COUNT(*) FROM tables
            UNION ALL SELECT 'fields', COUNT(*) FROM fields
            UNION ALL SELECT 'cards', COUNT(*) FROM cards
            UNION ALL SELECT 'dashboards', COUNT(*) FROM dashboards
            UNION ALL SELECT 'collections', COUNT(*) FROM collections
            UNION ALL SELECT 'rel_dashboard_to_cards', COUNT(*) FROM rel_dashboard_to_cards
            UNION ALL SELECT 'rel_collection_to_contents', COUNT(*) FROM rel_collection_to_contents
            UNION ALL SELECT 'rel_card_to_data_model', COUNT(*) FROM rel_card_to_data_model
            UNION ALL SELECT 'rel_card_to_fields', COUNT(*) FROM rel_card_to_fields
            ORDER BY entity_type;
        """,
        "relationship_coverage": """
            WITH
            cards_total AS (
                SELECT COUNT(*)::DOUBLE AS total FROM cards
            ),
            cards_on_dashboards AS (
                SELECT COUNT(DISTINCT card_id)::DOUBLE AS covered FROM rel_dashboard_to_cards
            ),
            cards_with_tables AS (
                SELECT COUNT(*)::DOUBLE AS covered FROM cards WHERE table_id IS NOT NULL
            ),
            cards_with_fields AS (
                SELECT COUNT(DISTINCT card_id)::DOUBLE AS covered FROM rel_card_to_fields
            ),
            dashboards_total AS (
                SELECT COUNT(*)::DOUBLE AS total FROM dashboards
            ),
            dashboards_with_cards AS (
                SELECT COUNT(DISTINCT dashboard_id)::DOUBLE AS covered FROM rel_dashboard_to_cards
            ),
            collections_total AS (
                SELECT COUNT(*)::DOUBLE AS total FROM collections
            ),
            collections_with_contents AS (
                SELECT COUNT(DISTINCT collection_id)::DOUBLE AS covered FROM rel_collection_to_contents
            ),
            tables_total AS (
                SELECT COUNT(*)::DOUBLE AS total FROM tables
            ),
            tables_used_in_cards AS (
                SELECT COUNT(DISTINCT table_id)::DOUBLE AS covered FROM cards WHERE table_id IS NOT NULL
            ),
            fields_total AS (
                SELECT COUNT(*)::DOUBLE AS total FROM fields
            ),
            fields_used_in_cards AS (
                SELECT COUNT(DISTINCT field_id)::DOUBLE AS covered FROM rel_card_to_fields
            )
            SELECT
                metric,
                covered::BIGINT AS covered_count,
                total::BIGINT AS total_count,
                CASE WHEN total = 0 THEN NULL ELSE ROUND((covered / total) * 100, 2) END AS covered_pct
            FROM (
                SELECT 'cards_on_dashboards' AS metric, cod.covered, ct.total FROM cards_on_dashboards cod CROSS JOIN cards_total ct
                UNION ALL
                SELECT 'cards_with_table_link', cwt.covered, ct.total FROM cards_with_tables cwt CROSS JOIN cards_total ct
                UNION ALL
                SELECT 'cards_with_field_link', cwf.covered, ct.total FROM cards_with_fields cwf CROSS JOIN cards_total ct
                UNION ALL
                SELECT 'dashboards_with_cards', dwc.covered, dt.total FROM dashboards_with_cards dwc CROSS JOIN dashboards_total dt
                UNION ALL
                SELECT 'collections_with_contents', cwc.covered, ct.total FROM collections_with_contents cwc CROSS JOIN collections_total ct
                UNION ALL
                SELECT 'tables_used_in_cards', tuc.covered, tt.total FROM tables_used_in_cards tuc CROSS JOIN tables_total tt
                UNION ALL
                SELECT 'fields_used_in_cards', fuc.covered, ft.total FROM fields_used_in_cards fuc CROSS JOIN fields_total ft
            ) s
            ORDER BY metric;
        """,
        "potential_name_duplicates": """
            WITH candidates AS (
                SELECT
                    'cards' AS entity_type,
                    card_id::VARCHAR AS entity_id,
                    name,
                    regexp_replace(
                        regexp_replace(lower(trim(coalesce(name, ''))), '[^a-z0-9]+', ' ', 'g'),
                        '\\\\s+',
                        ' ',
                        'g'
                    ) AS normalized_name
                FROM cards
                UNION ALL
                SELECT
                    'dashboards',
                    dashboard_id::VARCHAR,
                    name,
                    regexp_replace(
                        regexp_replace(lower(trim(coalesce(name, ''))), '[^a-z0-9]+', ' ', 'g'),
                        '\\\\s+',
                        ' ',
                        'g'
                    ) AS normalized_name
                FROM dashboards
                UNION ALL
                SELECT
                    'collections',
                    collection_id,
                    name,
                    regexp_replace(
                        regexp_replace(lower(trim(coalesce(name, ''))), '[^a-z0-9]+', ' ', 'g'),
                        '\\\\s+',
                        ' ',
                        'g'
                    ) AS normalized_name
                FROM collections
                UNION ALL
                SELECT
                    'tables',
                    table_id::VARCHAR,
                    name,
                    regexp_replace(
                        regexp_replace(lower(trim(coalesce(name, ''))), '[^a-z0-9]+', ' ', 'g'),
                        '\\\\s+',
                        ' ',
                        'g'
                    ) AS normalized_name
                FROM tables
            ),
            grouped AS (
                SELECT
                    entity_type,
                    normalized_name,
                    COUNT(*) AS duplicate_count,
                    string_agg(entity_id, ', ') AS entity_ids,
                    string_agg(name, ' | ') AS names
                FROM candidates
                WHERE normalized_name <> ''
                GROUP BY entity_type, normalized_name
                HAVING COUNT(*) > 1
            )
            SELECT *
            FROM grouped
            ORDER BY duplicate_count DESC, entity_type, normalized_name;
        """,
        "potential_orphans": """
            SELECT
                'card_not_on_dashboard' AS issue_type,
                c.card_id::VARCHAR AS object_id,
                c.name AS object_name,
                'No dashboard relationship found' AS detail
            FROM cards c
            LEFT JOIN rel_dashboard_to_cards rdc ON rdc.card_id = c.card_id
            WHERE rdc.card_id IS NULL
            UNION ALL
            SELECT
                'dashboard_without_cards',
                d.dashboard_id::VARCHAR,
                d.name,
                'No cards linked to dashboard'
            FROM dashboards d
            LEFT JOIN rel_dashboard_to_cards rdc ON rdc.dashboard_id = d.dashboard_id
            WHERE rdc.dashboard_id IS NULL
            UNION ALL
            SELECT
                'collection_without_contents',
                c.collection_id::VARCHAR,
                c.name,
                'No contents linked to collection'
            FROM collections c
            LEFT JOIN rel_collection_to_contents rcc ON rcc.collection_id = c.collection_id
            WHERE rcc.collection_id IS NULL
            UNION ALL
            SELECT
                'table_without_card_link',
                t.table_id::VARCHAR,
                t.name,
                'No cards linked to table'
            FROM tables t
            LEFT JOIN cards c ON c.table_id = t.table_id
            WHERE c.card_id IS NULL
            UNION ALL
            SELECT
                'field_without_card_link',
                f.field_id::VARCHAR,
                f.name,
                'No cards linked to field'
            FROM fields f
            LEFT JOIN rel_card_to_fields rcf ON rcf.field_id = f.field_id
            WHERE rcf.field_id IS NULL
            ORDER BY issue_type, object_name;
        """,
        "usage_concentration_by_database": """
            SELECT
                d.database_id,
                d.name AS database_name,
                COUNT(c.card_id) AS card_count
            FROM databases d
            LEFT JOIN cards c ON c.database_id = d.database_id
            GROUP BY d.database_id, d.name
            ORDER BY card_count DESC, database_name;
        """,
        "usage_concentration_by_table": """
            SELECT
                t.table_id,
                t.database_id,
                t.name AS table_name,
                COUNT(c.card_id) AS card_count
            FROM tables t
            LEFT JOIN cards c ON c.table_id = t.table_id
            GROUP BY t.table_id, t.database_id, t.name
            ORDER BY card_count DESC, table_name;
        """,
        "relationship_dashboards_to_cards": """
            SELECT
                r.dashboard_id,
                d.name AS dashboard_name,
                r.card_id,
                c.name AS card_name
            FROM rel_dashboard_to_cards r
            LEFT JOIN dashboards d ON d.dashboard_id = r.dashboard_id
            LEFT JOIN cards c ON c.card_id = r.card_id
            ORDER BY r.dashboard_id, r.card_id;
        """,
        "relationship_collections_to_contents": """
            SELECT
                r.collection_id,
                c.name AS collection_name,
                r.item_type,
                r.item_id
            FROM rel_collection_to_contents r
            LEFT JOIN collections c ON c.collection_id = r.collection_id
            ORDER BY r.collection_id, r.item_type, r.item_id;
        """,
        "relationship_cards_to_data_model": """
            SELECT
                r.card_id,
                c.name AS card_name,
                r.database_id,
                d.name AS database_name,
                r.table_id,
                t.name AS table_name,
                r.field_ids_json
            FROM rel_card_to_data_model r
            LEFT JOIN cards c ON c.card_id = r.card_id
            LEFT JOIN databases d ON d.database_id = r.database_id
            LEFT JOIN tables t ON t.table_id = r.table_id
            ORDER BY r.card_id;
        """,
    }

    for name, sql in queries.items():
        (queries_dir / f"{name}.sql").write_text(sql.strip() + "\n", encoding="utf-8")

    issues: list[AnalysisIssue] = []
    outputs: dict[str, dict[str, Any]] = {}
    con = duckdb.connect(str(duckdb_path), read_only=True)

    try:
        for report_name, sql in queries.items():
            outputs[report_name] = run_query_to_csv(con, report_name, sql, reports_dir, issues)

        near_dup_path = reports_dir / "potential_name_near_duplicates.csv"
        try:
            columns, rows = near_duplicate_rows(con)
            write_csv(near_dup_path, columns, rows)
            outputs["potential_name_near_duplicates"] = {
                "file": str(near_dup_path),
                "row_count": len(rows),
            }
            print(f"Wrote {near_dup_path} ({len(rows)} rows)")
        except Exception as exc:  # noqa: BLE001
            issues.append(AnalysisIssue("potential_name_near_duplicates", str(exc)))
            outputs["potential_name_near_duplicates"] = {
                "file": str(near_dup_path),
                "row_count": 0,
            }
            print(f"Warning: failed report potential_name_near_duplicates: {exc}", file=sys.stderr)
    finally:
        con.close()

    summary = {
        "generated_at": utc_now_iso(),
        "duckdb_file": str(duckdb_path),
        "report_count": len(queries),
        "successful_report_count": len(queries) - len(issues),
        "issue_count": len(issues),
        "issues": [{"report_name": i.report_name, "error": i.error} for i in issues],
        "outputs": outputs,
    }
    summary_path = reports_dir / "summary_overview.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary_path}")

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

    summary_outputs_path = reports_dir / "summary_outputs.csv"
    write_csv(
        summary_outputs_path,
        ["report_name", "file", "row_count", "status", "error"],
        summary_rows,
    )
    print(f"Wrote {summary_outputs_path}")

    if issues:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
