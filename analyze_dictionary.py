from __future__ import annotations

import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from dotenv import dotenv_values


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def slugify(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        return "unknown"
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower())
    return slug.strip("_") or "unknown"


def write_csv(path: Path, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def run_query(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    result = con.execute(sql)
    rows = result.fetchall()
    columns = [column[0] for column in result.description]
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
    dictionary_dir = reports_dir / "dictionary"
    per_database_dir = dictionary_dir / "per_database"

    reports_dir.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)
    dictionary_dir.mkdir(parents=True, exist_ok=True)
    per_database_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_path.exists():
        print(f"Error: DuckDB file not found: {duckdb_path}", file=sys.stderr)
        return 1

    queries: dict[str, str] = {
        "phase8_database_dictionary": """
            SELECT
                database_id,
                name AS database_name,
                engine,
                description,
                is_sample,
                is_audit,
                initial_sync_status,
                created_at,
                updated_at
            FROM databases
            ORDER BY database_id;
        """,
        "phase8_table_dictionary": """
            SELECT
                t.table_id,
                t.database_id,
                d.name AS database_name,
                t.schema_name,
                t.name AS table_name,
                t.display_name AS table_display_name,
                t.entity_type,
                t.description,
                t.active,
                t.visibility_type,
                t.created_at,
                t.updated_at
            FROM tables t
            LEFT JOIN databases d ON d.database_id = t.database_id
            ORDER BY t.database_id, t.table_id;
        """,
        "phase8_field_dictionary": """
            SELECT
                f.field_id,
                f.table_id,
                t.name AS table_name,
                t.display_name AS table_display_name,
                t.database_id,
                d.name AS database_name,
                f.name AS field_name,
                f.display_name AS field_display_name,
                f.base_type,
                f.effective_type,
                f.semantic_type,
                f.description,
                f.active,
                f.visibility_type,
                f.fk_target_field_id,
                f.created_at,
                f.updated_at
            FROM fields f
            LEFT JOIN tables t ON t.table_id = f.table_id
            LEFT JOIN databases d ON d.database_id = t.database_id
            ORDER BY t.database_id, f.table_id, f.field_id;
        """,
        "phase8_field_dictionary_expanded": """
            SELECT
                f.field_id,
                f.table_id,
                src_t.name AS table_name,
                src_t.display_name AS table_display_name,
                src_t.schema_name AS table_schema_name,
                src_t.database_id,
                src_d.name AS database_name,
                f.name AS field_name,
                f.display_name AS field_display_name,
                f.base_type,
                f.effective_type,
                f.semantic_type,
                f.description AS field_description,
                f.active AS field_active,
                f.visibility_type AS field_visibility_type,
                f.fk_target_field_id,
                tgt_f.table_id AS fk_target_table_id,
                tgt_t.name AS fk_target_table_name,
                tgt_f.name AS fk_target_field_name,
                tgt_f.display_name AS fk_target_field_display_name,
                f.created_at,
                f.updated_at
            FROM fields f
            LEFT JOIN tables src_t ON src_t.table_id = f.table_id
            LEFT JOIN databases src_d ON src_d.database_id = src_t.database_id
            LEFT JOIN fields tgt_f ON tgt_f.field_id = f.fk_target_field_id
            LEFT JOIN tables tgt_t ON tgt_t.table_id = tgt_f.table_id
            ORDER BY src_t.database_id, f.table_id, f.field_id;
        """,
    }

    for name, sql in queries.items():
        (queries_dir / f"{name}.sql").write_text(sql.strip() + "\n", encoding="utf-8")

    con = duckdb.connect(str(duckdb_path), read_only=True)
    outputs: dict[str, dict[str, Any]] = {}
    issues: list[dict[str, str]] = []
    try:
        # Global dictionary files
        for query_name, filename in [
            ("phase8_database_dictionary", "database_dictionary.csv"),
            ("phase8_table_dictionary", "table_dictionary.csv"),
            ("phase8_field_dictionary", "field_dictionary.csv"),
            ("phase8_field_dictionary_expanded", "field_dictionary_expanded.csv"),
        ]:
            try:
                columns, rows = run_query(con, queries[query_name])
                path = dictionary_dir / filename
                write_csv(path, columns, rows)
                outputs[query_name] = {"file": str(path), "row_count": len(rows)}
                print(f"Wrote {path} ({len(rows)} rows)")
            except Exception as exc:  # noqa: BLE001
                issues.append({"report_name": query_name, "error": str(exc)})
                print(f"Warning: failed {query_name}: {exc}", file=sys.stderr)

        # Per-database files with expanded variants.
        try:
            db_rows = con.execute(
                "SELECT database_id, name FROM databases ORDER BY database_id"
            ).fetchall()
        except Exception as exc:  # noqa: BLE001
            db_rows = []
            issues.append({"report_name": "phase8_per_database_init", "error": str(exc)})
            print(f"Warning: failed database listing for per-database outputs: {exc}", file=sys.stderr)

        per_database_manifest_rows: list[tuple[Any, ...]] = []

        table_sql = queries["phase8_table_dictionary"] + " "
        field_sql = queries["phase8_field_dictionary"] + " "
        expanded_sql = queries["phase8_field_dictionary_expanded"] + " "

        table_sql = table_sql.replace(
            "ORDER BY t.database_id, t.table_id;",
            "WHERE t.database_id = ? ORDER BY t.database_id, t.table_id;",
        )
        field_sql = field_sql.replace(
            "ORDER BY t.database_id, f.table_id, f.field_id;",
            "WHERE t.database_id = ? ORDER BY t.database_id, f.table_id, f.field_id;",
        )
        expanded_sql = expanded_sql.replace(
            "ORDER BY src_t.database_id, f.table_id, f.field_id;",
            "WHERE src_t.database_id = ? ORDER BY src_t.database_id, f.table_id, f.field_id;",
        )

        for database_id, database_name in db_rows:
            db_slug = slugify(database_name)
            db_prefix = f"database_{database_id}_{db_slug}"

            try:
                table_result = con.execute(table_sql, [database_id])
                table_rows = table_result.fetchall()
                table_cols = [col[0] for col in table_result.description]
                table_path = per_database_dir / f"{db_prefix}_tables.csv"
                write_csv(table_path, table_cols, table_rows)
            except Exception as exc:  # noqa: BLE001
                table_rows = []
                issues.append(
                    {
                        "report_name": f"phase8_per_database_tables_{database_id}",
                        "error": str(exc),
                    }
                )
                continue

            try:
                field_result = con.execute(field_sql, [database_id])
                field_rows = field_result.fetchall()
                field_cols = [col[0] for col in field_result.description]
                field_path = per_database_dir / f"{db_prefix}_fields.csv"
                write_csv(field_path, field_cols, field_rows)
            except Exception as exc:  # noqa: BLE001
                field_rows = []
                issues.append(
                    {
                        "report_name": f"phase8_per_database_fields_{database_id}",
                        "error": str(exc),
                    }
                )
                continue

            try:
                expanded_result = con.execute(expanded_sql, [database_id])
                expanded_rows = expanded_result.fetchall()
                expanded_cols = [col[0] for col in expanded_result.description]
                expanded_path = per_database_dir / f"{db_prefix}_fields_expanded.csv"
                write_csv(expanded_path, expanded_cols, expanded_rows)
            except Exception as exc:  # noqa: BLE001
                expanded_rows = []
                issues.append(
                    {
                        "report_name": f"phase8_per_database_fields_expanded_{database_id}",
                        "error": str(exc),
                    }
                )
                continue

            per_database_manifest_rows.append(
                (
                    database_id,
                    database_name,
                    str(table_path),
                    len(table_rows),
                    str(field_path),
                    len(field_rows),
                    str(expanded_path),
                    len(expanded_rows),
                )
            )

        per_database_manifest_path = dictionary_dir / "dictionary_per_database_manifest.csv"
        write_csv(
            per_database_manifest_path,
            [
                "database_id",
                "database_name",
                "tables_file",
                "tables_row_count",
                "fields_file",
                "fields_row_count",
                "fields_expanded_file",
                "fields_expanded_row_count",
            ],
            per_database_manifest_rows,
        )
        outputs["phase8_per_database_manifest"] = {
            "file": str(per_database_manifest_path),
            "row_count": len(per_database_manifest_rows),
        }
        print(f"Wrote {per_database_manifest_path} ({len(per_database_manifest_rows)} rows)")
    finally:
        con.close()

    summary = {
        "generated_at": utc_now_iso(),
        "duckdb_file": str(duckdb_path),
        "report_count": len(outputs),
        "successful_report_count": len(outputs),
        "issue_count": len(issues),
        "issues": issues,
        "outputs": outputs,
    }

    summary_json_path = reports_dir / "dictionary_summary_overview.json"
    summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary_json_path}")

    summary_rows: list[tuple[Any, ...]] = []
    issue_map = {issue["report_name"]: issue["error"] for issue in issues}
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

    summary_csv_path = reports_dir / "dictionary_summary_outputs.csv"
    write_csv(
        summary_csv_path,
        ["report_name", "file", "row_count", "status", "error"],
        summary_rows,
    )
    print(f"Wrote {summary_csv_path}")

    if issues:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
