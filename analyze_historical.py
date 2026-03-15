from __future__ import annotations

import csv
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


def parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip()
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "1", "yes"}:
            return True
        if lowered in {"false", "f", "0", "no"}:
            return False
    return None


def is_temporal_type(*values: Any) -> bool:
    for value in values:
        if not isinstance(value, str):
            continue
        lowered = value.lower()
        if "date" in lowered or "time" in lowered:
            return True
    return False


def extract_temporal_range_from_fingerprint(fingerprint_json: Any) -> tuple[str | None, str | None]:
    if not isinstance(fingerprint_json, str) or not fingerprint_json.strip():
        return None, None
    try:
        parsed = json.loads(fingerprint_json)
    except json.JSONDecodeError:
        return None, None
    if not isinstance(parsed, dict):
        return None, None

    type_section = parsed.get("type")
    if not isinstance(type_section, dict):
        return None, None

    best_earliest: datetime | None = None
    best_latest: datetime | None = None

    for metric in type_section.values():
        if not isinstance(metric, dict):
            continue
        earliest_dt = parse_iso_datetime(metric.get("earliest"))
        latest_dt = parse_iso_datetime(metric.get("latest"))
        if earliest_dt is None or latest_dt is None:
            continue
        if best_earliest is None or earliest_dt < best_earliest:
            best_earliest = earliest_dt
        if best_latest is None or latest_dt > best_latest:
            best_latest = latest_dt

    if best_earliest is None or best_latest is None:
        return None, None
    return best_earliest.isoformat(), best_latest.isoformat()


def span_days(earliest: str | None, latest: str | None) -> float | None:
    earliest_dt = parse_iso_datetime(earliest)
    latest_dt = parse_iso_datetime(latest)
    if earliest_dt is None or latest_dt is None:
        return None
    return round((latest_dt - earliest_dt).total_seconds() / 86400.0, 2)


def temporal_strength(span: float | None) -> str:
    if span is None:
        return "no_temporal_range"
    if span < 30:
        return "weak_span"
    if span < 365:
        return "moderate_span"
    return "strong_span"


def candidate_rank_key(row: dict[str, Any]) -> tuple[int, float, float]:
    has_range = 1 if row.get("oldest_observed_value") and row.get("newest_observed_value") else 0
    span = float(row.get("span_days") or 0.0)
    nil_pct = row.get("fingerprint_nil_pct")
    nil_score = 1.0 - float(nil_pct) if isinstance(nil_pct, (int, float)) else 0.0
    return has_range, span, nil_score


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
    historical_dir = reports_dir / "historical"
    per_database_dir = historical_dir / "per_database"

    reports_dir.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)
    historical_dir.mkdir(parents=True, exist_ok=True)
    per_database_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_path.exists():
        print(f"Error: DuckDB file not found: {duckdb_path}", file=sys.stderr)
        return 1

    queries: dict[str, str] = {
        "historical_base_fields": """
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
                f.fingerprint_distinct_count,
                f.fingerprint_nil_count,
                f.fingerprint_nil_pct,
                f.fingerprint_json
            FROM fields f
            LEFT JOIN tables t ON t.table_id = f.table_id
            LEFT JOIN databases d ON d.database_id = t.database_id
            ORDER BY t.database_id, t.table_id, f.field_id;
        """,
        "historical_base_tables": """
            SELECT
                t.table_id,
                t.database_id,
                d.name AS database_name,
                t.name AS table_name,
                t.display_name AS table_display_name,
                t.active,
                t.visibility_type
            FROM tables t
            LEFT JOIN databases d ON d.database_id = t.database_id
            ORDER BY t.database_id, t.table_id;
        """,
    }

    for name, sql in queries.items():
        (queries_dir / f"{name}.sql").write_text(sql.strip() + "\n", encoding="utf-8")

    issues: list[AnalysisIssue] = []
    outputs: dict[str, dict[str, Any]] = {}

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        fields_rows = con.execute(queries["historical_base_fields"]).fetchall()
        field_columns = [col[0] for col in con.description]
        tables_rows = con.execute(queries["historical_base_tables"]).fetchall()
        table_columns = [col[0] for col in con.description]
    except Exception as exc:  # noqa: BLE001
        con.close()
        print(f"Error: failed loading historical base data: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()

    fields: list[dict[str, Any]] = [dict(zip(field_columns, row)) for row in fields_rows]
    tables: list[dict[str, Any]] = [dict(zip(table_columns, row)) for row in tables_rows]

    temporal_field_rows: list[tuple[Any, ...]] = []
    table_depth_rows: list[tuple[Any, ...]] = []
    per_database_rows: list[tuple[Any, ...]] = []

    temporal_candidates_by_table: dict[int, list[dict[str, Any]]] = {}

    for field in fields:
        if not is_temporal_type(
            field.get("effective_type"), field.get("base_type"), field.get("semantic_type")
        ):
            continue

        active = as_bool(field.get("active"))
        visibility_type = field.get("visibility_type")
        if active is False or visibility_type in {"hidden", "retired"}:
            continue

        oldest, newest = extract_temporal_range_from_fingerprint(field.get("fingerprint_json"))
        span = span_days(oldest, newest)
        strength = temporal_strength(span)

        row = {
            "database_id": field.get("database_id"),
            "database_name": field.get("database_name"),
            "table_id": field.get("table_id"),
            "table_name": field.get("table_name"),
            "table_display_name": field.get("table_display_name"),
            "field_id": field.get("field_id"),
            "field_name": field.get("field_name"),
            "field_display_name": field.get("field_display_name"),
            "effective_type": field.get("effective_type"),
            "base_type": field.get("base_type"),
            "semantic_type": field.get("semantic_type"),
            "fingerprint_distinct_count": field.get("fingerprint_distinct_count"),
            "fingerprint_nil_count": field.get("fingerprint_nil_count"),
            "fingerprint_nil_pct": field.get("fingerprint_nil_pct"),
            "oldest_observed_value": oldest,
            "newest_observed_value": newest,
            "span_days": span,
            "temporal_signal": strength,
        }
        table_id = row.get("table_id")
        if isinstance(table_id, int):
            temporal_candidates_by_table.setdefault(table_id, []).append(row)

    for table_id, candidates in temporal_candidates_by_table.items():
        ranked = sorted(candidates, key=candidate_rank_key, reverse=True)
        for rank, candidate in enumerate(ranked, start=1):
            temporal_field_rows.append(
                (
                    candidate.get("database_id"),
                    candidate.get("database_name"),
                    candidate.get("table_id"),
                    candidate.get("table_name"),
                    candidate.get("table_display_name"),
                    candidate.get("field_id"),
                    candidate.get("field_name"),
                    candidate.get("field_display_name"),
                    candidate.get("effective_type"),
                    candidate.get("base_type"),
                    candidate.get("semantic_type"),
                    candidate.get("fingerprint_distinct_count"),
                    candidate.get("fingerprint_nil_count"),
                    candidate.get("fingerprint_nil_pct"),
                    candidate.get("oldest_observed_value"),
                    candidate.get("newest_observed_value"),
                    candidate.get("span_days"),
                    candidate.get("temporal_signal"),
                    rank,
                    rank == 1,
                )
            )

    table_lookup = {table["table_id"]: table for table in tables if isinstance(table.get("table_id"), int)}

    for table in tables:
        table_id = table.get("table_id")
        if not isinstance(table_id, int):
            continue
        candidates = temporal_candidates_by_table.get(table_id, [])
        if not candidates:
            table_depth_rows.append(
                (
                    table.get("database_id"),
                    table.get("database_name"),
                    table_id,
                    table.get("table_name"),
                    table.get("table_display_name"),
                    None,
                    None,
                    None,
                    None,
                    None,
                    "no_usable_temporal_field",
                    "not_suitable_for_historical_analysis",
                    "No active/visible temporal field found",
                )
            )
            continue

        best = sorted(candidates, key=candidate_rank_key, reverse=True)[0]
        best_span = best.get("span_days")
        signal = temporal_strength(best_span if isinstance(best_span, (int, float)) else None)

        if signal == "strong_span":
            suitability = "historically_strong"
            reason = "Temporal field shows >=365 day observed span"
        elif signal == "moderate_span":
            suitability = "historically_moderate"
            reason = "Temporal field shows 30-364 day observed span"
        elif signal == "weak_span":
            suitability = "historically_weak"
            reason = "Temporal field span is <30 days"
        else:
            suitability = "not_suitable_for_historical_analysis"
            reason = "Temporal field exists but range was not available"

        table_depth_rows.append(
            (
                best.get("database_id"),
                best.get("database_name"),
                table_id,
                best.get("table_name"),
                best.get("table_display_name"),
                best.get("field_id"),
                best.get("field_name"),
                best.get("oldest_observed_value"),
                best.get("newest_observed_value"),
                best.get("span_days"),
                signal,
                suitability,
                reason,
            )
        )

    database_ids = sorted(
        {
            table.get("database_id")
            for table in tables
            if isinstance(table.get("database_id"), int)
        }
    )

    table_depth_by_db: dict[int, list[tuple[Any, ...]]] = {}
    for row in table_depth_rows:
        db_id = row[0]
        if isinstance(db_id, int):
            table_depth_by_db.setdefault(db_id, []).append(row)

    for db_id in database_ids:
        db_rows = table_depth_by_db.get(db_id, [])
        total_tables = len(db_rows)
        with_temporal = sum(1 for row in db_rows if row[5] is not None)
        with_range = sum(1 for row in db_rows if row[7] is not None and row[8] is not None)
        strong = sum(1 for row in db_rows if row[10] == "strong_span")
        moderate = sum(1 for row in db_rows if row[10] == "moderate_span")
        weak = sum(1 for row in db_rows if row[10] == "weak_span")
        no_temporal = sum(1 for row in db_rows if row[10] == "no_usable_temporal_field")
        no_range = sum(1 for row in db_rows if row[10] == "no_temporal_range")

        db_name = None
        for row in db_rows:
            if isinstance(row[1], str):
                db_name = row[1]
                break

        per_database_rows.append(
            (
                db_id,
                db_name,
                total_tables,
                with_temporal,
                with_range,
                strong,
                moderate,
                weak,
                no_temporal,
                no_range,
                round((with_temporal / total_tables) * 100.0, 2) if total_tables else None,
                round((with_range / total_tables) * 100.0, 2) if total_tables else None,
            )
        )

    temporal_columns = [
        "database_id",
        "database_name",
        "table_id",
        "table_name",
        "table_display_name",
        "field_id",
        "field_name",
        "field_display_name",
        "effective_type",
        "base_type",
        "semantic_type",
        "fingerprint_distinct_count",
        "fingerprint_nil_count",
        "fingerprint_nil_pct",
        "oldest_observed_value",
        "newest_observed_value",
        "span_days",
        "temporal_signal",
        "candidate_rank",
        "is_likely_temporal_field",
    ]
    table_depth_columns = [
        "database_id",
        "database_name",
        "table_id",
        "table_name",
        "table_display_name",
        "likely_temporal_field_id",
        "likely_temporal_field_name",
        "oldest_observed_value",
        "newest_observed_value",
        "span_days",
        "temporal_signal",
        "historical_suitability_signal",
        "rationale",
    ]
    per_database_columns = [
        "database_id",
        "database_name",
        "table_count",
        "tables_with_temporal_field",
        "tables_with_temporal_range",
        "tables_strong_span",
        "tables_moderate_span",
        "tables_weak_span",
        "tables_no_temporal_field",
        "tables_no_temporal_range",
        "temporal_field_coverage_pct",
        "temporal_range_coverage_pct",
    ]

    field_candidates_path = historical_dir / "historical_candidate_temporal_fields.csv"
    write_csv(field_candidates_path, temporal_columns, temporal_field_rows)
    outputs["historical_candidate_temporal_fields"] = {
        "file": str(field_candidates_path),
        "row_count": len(temporal_field_rows),
    }
    print(f"Wrote {field_candidates_path} ({len(temporal_field_rows)} rows)")

    table_depth_path = historical_dir / "historical_table_depth.csv"
    write_csv(table_depth_path, table_depth_columns, table_depth_rows)
    outputs["historical_table_depth"] = {
        "file": str(table_depth_path),
        "row_count": len(table_depth_rows),
    }
    print(f"Wrote {table_depth_path} ({len(table_depth_rows)} rows)")

    per_database_path = historical_dir / "historical_per_database_coverage.csv"
    write_csv(per_database_path, per_database_columns, per_database_rows)
    outputs["historical_per_database_coverage"] = {
        "file": str(per_database_path),
        "row_count": len(per_database_rows),
    }
    print(f"Wrote {per_database_path} ({len(per_database_rows)} rows)")

    per_database_manifest_rows: list[tuple[Any, ...]] = []
    for row in per_database_rows:
        db_id, db_name = row[0], row[1]
        if not isinstance(db_id, int):
            continue
        db_slug = slugify(db_name)
        db_prefix = f"database_{db_id}_{db_slug}"

        db_table_rows = [r for r in table_depth_rows if r[0] == db_id]
        db_fields_rows = [r for r in temporal_field_rows if r[0] == db_id]

        db_table_path = per_database_dir / f"{db_prefix}_historical_table_depth.csv"
        db_fields_path = per_database_dir / f"{db_prefix}_historical_candidate_temporal_fields.csv"
        write_csv(db_table_path, table_depth_columns, db_table_rows)
        write_csv(db_fields_path, temporal_columns, db_fields_rows)

        per_database_manifest_rows.append(
            (
                db_id,
                db_name,
                str(db_table_path),
                len(db_table_rows),
                str(db_fields_path),
                len(db_fields_rows),
            )
        )

    per_database_manifest_path = historical_dir / "historical_per_database_manifest.csv"
    write_csv(
        per_database_manifest_path,
        [
            "database_id",
            "database_name",
            "table_depth_file",
            "table_depth_row_count",
            "temporal_fields_file",
            "temporal_fields_row_count",
        ],
        per_database_manifest_rows,
    )
    outputs["historical_per_database_manifest"] = {
        "file": str(per_database_manifest_path),
        "row_count": len(per_database_manifest_rows),
    }
    print(f"Wrote {per_database_manifest_path} ({len(per_database_manifest_rows)} rows)")

    summary = {
        "generated_at": utc_now_iso(),
        "duckdb_file": str(duckdb_path),
        "report_count": len(outputs),
        "successful_report_count": len(outputs),
        "issue_count": len(issues),
        "issues": [{"report_name": i.report_name, "error": i.error} for i in issues],
        "outputs": outputs,
    }

    summary_json_path = reports_dir / "historical_summary_overview.json"
    summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary_json_path}")

    summary_rows = [
        (name, item.get("file"), item.get("row_count"), "ok", "")
        for name, item in outputs.items()
    ]
    summary_csv_path = reports_dir / "historical_summary_outputs.csv"
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
