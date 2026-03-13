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
    completeness_dir = reports_dir / "completeness"
    per_database_dir = completeness_dir / "per_database"

    reports_dir.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)
    completeness_dir.mkdir(parents=True, exist_ok=True)
    per_database_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_path.exists():
        print(f"Error: DuckDB file not found: {duckdb_path}", file=sys.stderr)
        return 1

    queries: dict[str, str] = {
        "completeness_field_profile": """
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
                f.active,
                f.visibility_type,
                f.has_field_values,
                f.fingerprint_distinct_count,
                f.fingerprint_nil_count,
                f.fingerprint_nil_pct,
                CASE WHEN COALESCE(trim(f.description), '') <> '' THEN TRUE ELSE FALSE END AS has_description,
                CASE
                    WHEN f.active = FALSE THEN 'inactive'
                    WHEN f.visibility_type IN ('hidden', 'retired') THEN 'not_visible'
                    WHEN f.fingerprint_nil_pct IS NULL THEN 'no_null_ratio_available'
                    WHEN f.fingerprint_nil_pct <= 0.05 THEN 'high_completeness'
                    WHEN f.fingerprint_nil_pct <= 0.20 THEN 'moderate_completeness'
                    ELSE 'low_completeness'
                END AS completeness_signal,
                CASE
                    WHEN f.fingerprint_nil_pct IS NULL THEN NULL
                    ELSE ROUND((1.0 - f.fingerprint_nil_pct) * 100.0, 2)
                END AS completeness_score_null_ratio
            FROM fields f
            LEFT JOIN tables t ON t.table_id = f.table_id
            LEFT JOIN databases d ON d.database_id = t.database_id
            ORDER BY t.database_id, t.table_id, f.field_id;
        """,
        "completeness_table_summary": """
            SELECT
                t.database_id,
                d.name AS database_name,
                t.table_id,
                t.name AS table_name,
                COUNT(*) AS field_count,
                SUM(CASE WHEN f.fingerprint_nil_pct IS NOT NULL THEN 1 ELSE 0 END) AS fields_with_null_ratio,
                ROUND(AVG(f.fingerprint_nil_pct), 4) AS avg_nil_pct,
                ROUND(MAX(f.fingerprint_nil_pct), 4) AS max_nil_pct,
                SUM(CASE WHEN f.fingerprint_nil_pct <= 0.05 THEN 1 ELSE 0 END) AS high_completeness_fields,
                SUM(CASE WHEN f.fingerprint_nil_pct > 0.20 THEN 1 ELSE 0 END) AS low_completeness_fields,
                SUM(CASE WHEN f.active = FALSE THEN 1 ELSE 0 END) AS inactive_fields,
                SUM(CASE WHEN f.visibility_type IN ('hidden', 'retired') THEN 1 ELSE 0 END) AS not_visible_fields
            FROM fields f
            LEFT JOIN tables t ON t.table_id = f.table_id
            LEFT JOIN databases d ON d.database_id = t.database_id
            GROUP BY t.database_id, d.name, t.table_id, t.name
            ORDER BY t.database_id, t.table_id;
        """,
        "completeness_field_values_profile": """
            SELECT
                f.field_id,
                f.table_id,
                t.name AS table_name,
                t.database_id,
                d.name AS database_name,
                f.name AS field_name,
                f.display_name AS field_display_name,
                f.base_type,
                f.effective_type,
                f.semantic_type,
                f.has_field_values,
                f.fingerprint_distinct_count,
                f.fingerprint_nil_count,
                f.fingerprint_nil_pct,
                CASE
                    WHEN f.fingerprint_distinct_count IS NULL THEN 'unknown'
                    WHEN f.fingerprint_distinct_count <= 20 THEN 'likely_categorical'
                    WHEN f.fingerprint_distinct_count <= 200 THEN 'possibly_categorical'
                    ELSE 'high_cardinality_or_free_text'
                END AS cardinality_signal,
                CASE
                    WHEN f.has_field_values IN ('list', 'search', 'none') THEN f.has_field_values
                    WHEN f.has_field_values IS NULL THEN 'unknown'
                    ELSE CAST(f.has_field_values AS VARCHAR)
                END AS observed_values_signal
            FROM fields f
            LEFT JOIN tables t ON t.table_id = f.table_id
            LEFT JOIN databases d ON d.database_id = t.database_id
            ORDER BY t.database_id, t.table_id, f.field_id;
        """,
        "completeness_field_values_profile_expanded": """
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
                f.has_field_values,
                f.fingerprint_distinct_count,
                f.fingerprint_nil_count,
                f.fingerprint_nil_pct,
                f.fk_target_field_id,
                tgt_f.table_id AS fk_target_table_id,
                tgt_t.name AS fk_target_table_name,
                tgt_f.name AS fk_target_field_name,
                tgt_f.display_name AS fk_target_field_display_name,
                CASE
                    WHEN f.fingerprint_distinct_count IS NULL THEN 'unknown'
                    WHEN f.fingerprint_distinct_count <= 20 THEN 'likely_categorical'
                    WHEN f.fingerprint_distinct_count <= 200 THEN 'possibly_categorical'
                    ELSE 'high_cardinality_or_free_text'
                END AS cardinality_signal
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
        global_files = [
            ("completeness_field_profile", "completeness_field_profile.csv"),
            ("completeness_table_summary", "completeness_table_summary.csv"),
            ("completeness_field_values_profile", "completeness_field_values_profile.csv"),
            (
                "completeness_field_values_profile_expanded",
                "completeness_field_values_profile_expanded.csv",
            ),
        ]

        for query_name, filename in global_files:
            try:
                columns, rows = run_query(con, queries[query_name])
                path = completeness_dir / filename
                write_csv(path, columns, rows)
                outputs[query_name] = {"file": str(path), "row_count": len(rows)}
                print(f"Wrote {path} ({len(rows)} rows)")
            except Exception as exc:  # noqa: BLE001
                issues.append({"report_name": query_name, "error": str(exc)})
                print(f"Warning: failed {query_name}: {exc}", file=sys.stderr)

        try:
            db_rows = con.execute(
                "SELECT database_id, name FROM databases ORDER BY database_id"
            ).fetchall()
        except Exception as exc:  # noqa: BLE001
            db_rows = []
            issues.append({"report_name": "completeness_per_database_init", "error": str(exc)})
            print(
                f"Warning: failed database listing for per-database outputs: {exc}",
                file=sys.stderr,
            )

        per_database_manifest_rows: list[tuple[Any, ...]] = []

        field_sql = queries["completeness_field_profile"] + " "
        table_sql = queries["completeness_table_summary"] + " "
        value_sql = queries["completeness_field_values_profile"] + " "
        value_expanded_sql = queries["completeness_field_values_profile_expanded"] + " "

        field_sql = field_sql.replace(
            "ORDER BY t.database_id, t.table_id, f.field_id;",
            "WHERE t.database_id = ? ORDER BY t.database_id, t.table_id, f.field_id;",
        )
        table_sql = table_sql.replace(
            "GROUP BY t.database_id, d.name, t.table_id, t.name\n            ORDER BY t.database_id, t.table_id;",
            "WHERE t.database_id = ? GROUP BY t.database_id, d.name, t.table_id, t.name\n            ORDER BY t.database_id, t.table_id;",
        )
        value_sql = value_sql.replace(
            "ORDER BY t.database_id, t.table_id, f.field_id;",
            "WHERE t.database_id = ? ORDER BY t.database_id, t.table_id, f.field_id;",
        )
        value_expanded_sql = value_expanded_sql.replace(
            "ORDER BY src_t.database_id, f.table_id, f.field_id;",
            "WHERE src_t.database_id = ? ORDER BY src_t.database_id, f.table_id, f.field_id;",
        )

        for database_id, database_name in db_rows:
            db_slug = slugify(database_name)
            db_prefix = f"database_{database_id}_{db_slug}"

            try:
                f_result = con.execute(field_sql, [database_id])
                f_rows = f_result.fetchall()
                f_cols = [col[0] for col in f_result.description]
                f_path = per_database_dir / f"{db_prefix}_completeness_field_profile.csv"
                write_csv(f_path, f_cols, f_rows)
            except Exception as exc:  # noqa: BLE001
                issues.append(
                    {
                        "report_name": f"completeness_per_database_fields_{database_id}",
                        "error": str(exc),
                    }
                )
                continue

            try:
                t_result = con.execute(table_sql, [database_id])
                t_rows = t_result.fetchall()
                t_cols = [col[0] for col in t_result.description]
                t_path = per_database_dir / f"{db_prefix}_completeness_table_summary.csv"
                write_csv(t_path, t_cols, t_rows)
            except Exception as exc:  # noqa: BLE001
                issues.append(
                    {
                        "report_name": f"completeness_per_database_tables_{database_id}",
                        "error": str(exc),
                    }
                )
                continue

            try:
                v_result = con.execute(value_sql, [database_id])
                v_rows = v_result.fetchall()
                v_cols = [col[0] for col in v_result.description]
                v_path = per_database_dir / f"{db_prefix}_completeness_field_values_profile.csv"
                write_csv(v_path, v_cols, v_rows)
            except Exception as exc:  # noqa: BLE001
                issues.append(
                    {
                        "report_name": f"completeness_per_database_values_{database_id}",
                        "error": str(exc),
                    }
                )
                continue

            try:
                ve_result = con.execute(value_expanded_sql, [database_id])
                ve_rows = ve_result.fetchall()
                ve_cols = [col[0] for col in ve_result.description]
                ve_path = (
                    per_database_dir / f"{db_prefix}_completeness_field_values_profile_expanded.csv"
                )
                write_csv(ve_path, ve_cols, ve_rows)
            except Exception as exc:  # noqa: BLE001
                issues.append(
                    {
                        "report_name": f"completeness_per_database_values_expanded_{database_id}",
                        "error": str(exc),
                    }
                )
                continue

            per_database_manifest_rows.append(
                (
                    database_id,
                    database_name,
                    str(f_path),
                    len(f_rows),
                    str(t_path),
                    len(t_rows),
                    str(v_path),
                    len(v_rows),
                    str(ve_path),
                    len(ve_rows),
                )
            )

        per_database_manifest_path = completeness_dir / "completeness_per_database_manifest.csv"
        write_csv(
            per_database_manifest_path,
            [
                "database_id",
                "database_name",
                "field_profile_file",
                "field_profile_row_count",
                "table_summary_file",
                "table_summary_row_count",
                "field_values_file",
                "field_values_row_count",
                "field_values_expanded_file",
                "field_values_expanded_row_count",
            ],
            per_database_manifest_rows,
        )
        outputs["completeness_per_database_manifest"] = {
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

    summary_json_path = reports_dir / "completeness_summary_overview.json"
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

    summary_csv_path = reports_dir / "completeness_summary_outputs.csv"
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
