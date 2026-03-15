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


def category_from_score(score: float) -> str:
    if score >= 75:
        return "recommended_for_extraction"
    if score >= 50:
        return "possibly_useful_needs_review"
    if score >= 25:
        return "low_priority"
    return "not_recommended_based_on_current_evidence"


def parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "1", "yes"}:
            return True
        if lowered in {"false", "f", "0", "no"}:
            return False
    return None


def parse_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


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
    extraction_dir = reports_dir / "extraction"
    per_database_dir = extraction_dir / "per_database"

    reports_dir.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)
    extraction_dir.mkdir(parents=True, exist_ok=True)
    per_database_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_path.exists():
        print(f"Error: DuckDB file not found: {duckdb_path}", file=sys.stderr)
        return 1

    queries = {
        "extraction_table_base": """
            WITH cards_per_table AS (
                SELECT table_id, COUNT(*) AS cards_using_table
                FROM cards
                WHERE table_id IS NOT NULL
                GROUP BY table_id
            ),
            dashboards_per_table AS (
                SELECT c.table_id, COUNT(DISTINCT r.dashboard_id) AS dashboards_using_table
                FROM cards c
                JOIN rel_dashboard_to_cards r ON r.card_id = c.card_id
                WHERE c.table_id IS NOT NULL
                GROUP BY c.table_id
            ),
            completeness_per_table AS (
                SELECT
                    table_id,
                    AVG(completeness_score_null_ratio) AS avg_completeness_score,
                    MAX(
                        CASE completeness_signal
                            WHEN 'high_completeness' THEN 3
                            WHEN 'moderate_completeness' THEN 2
                            WHEN 'weak_completeness' THEN 1
                            ELSE 0
                        END
                    ) AS max_completeness_band
                FROM read_csv_auto('output/analysis/reports/completeness/completeness_field_profile.csv')
                GROUP BY table_id
            ),
            historical_per_table AS (
                SELECT
                    table_id,
                    historical_suitability_signal,
                    span_days
                FROM read_csv_auto('output/analysis/reports/historical/historical_table_depth.csv')
            ),
            table_flags AS (
                SELECT
                    t.table_id,
                    MAX(CASE WHEN cd.object_type = 'table' AND cd.table_id = t.table_id THEN 1 ELSE 0 END) AS has_duplicate_flag,
                    MAX(CASE WHEN cc.object_type = 'table' AND cc.table_id = t.table_id THEN 1 ELSE 0 END) AS has_conflict_flag
                FROM tables t
                LEFT JOIN read_csv_auto('output/analysis/reports/candidate_duplicates.csv') cd ON TRUE
                LEFT JOIN read_csv_auto('output/analysis/reports/candidate_conflicting_definitions.csv') cc ON TRUE
                GROUP BY t.table_id
            )
            SELECT
                t.database_id,
                d.name AS database_name,
                t.table_id,
                t.name AS table_name,
                t.display_name AS table_display_name,
                t.active,
                t.visibility_type,
                COALESCE(c.cards_using_table, 0) AS cards_using_table,
                COALESCE(dp.dashboards_using_table, 0) AS dashboards_using_table,
                cp.avg_completeness_score,
                h.historical_suitability_signal,
                h.span_days,
                COALESCE(tf.has_duplicate_flag, 0) AS has_duplicate_flag,
                COALESCE(tf.has_conflict_flag, 0) AS has_conflict_flag
            FROM tables t
            LEFT JOIN databases d ON d.database_id = t.database_id
            LEFT JOIN cards_per_table c ON c.table_id = t.table_id
            LEFT JOIN dashboards_per_table dp ON dp.table_id = t.table_id
            LEFT JOIN completeness_per_table cp ON cp.table_id = t.table_id
            LEFT JOIN historical_per_table h ON h.table_id = t.table_id
            LEFT JOIN table_flags tf ON tf.table_id = t.table_id
            ORDER BY t.database_id, t.table_id;
        """,
        "extraction_field_base": """
            WITH card_usage AS (
                SELECT field_id, COUNT(DISTINCT card_id) AS cards_using_field
                FROM rel_card_to_fields
                GROUP BY field_id
            ),
            completeness AS (
                SELECT
                    field_id,
                    completeness_score_null_ratio,
                    completeness_signal
                FROM read_csv_auto('output/analysis/reports/completeness/completeness_field_profile.csv')
            ),
            values_profile AS (
                SELECT
                    field_id,
                    observed_values_signal,
                    cardinality_signal
                FROM read_csv_auto('output/analysis/reports/completeness/completeness_field_values_profile.csv')
            ),
            temporal_candidates AS (
                SELECT
                    field_id,
                    is_likely_temporal_field,
                    span_days
                FROM read_csv_auto('output/analysis/reports/historical/historical_candidate_temporal_fields.csv')
            ),
            field_flags AS (
                SELECT
                    f.field_id,
                    MAX(CASE WHEN cd.object_type = 'field' AND cd.object_id = CAST(f.field_id AS VARCHAR) THEN 1 ELSE 0 END) AS has_duplicate_flag,
                    MAX(CASE WHEN cc.object_type = 'field' AND cc.object_id = CAST(f.field_id AS VARCHAR) THEN 1 ELSE 0 END) AS has_conflict_flag
                FROM fields f
                LEFT JOIN read_csv_auto('output/analysis/reports/candidate_duplicates.csv') cd ON TRUE
                LEFT JOIN read_csv_auto('output/analysis/reports/candidate_conflicting_definitions.csv') cc ON TRUE
                GROUP BY f.field_id
            )
            SELECT
                t.database_id,
                d.name AS database_name,
                f.table_id,
                t.name AS table_name,
                f.field_id,
                f.name AS field_name,
                f.display_name AS field_display_name,
                f.base_type,
                f.effective_type,
                f.semantic_type,
                f.active,
                f.visibility_type,
                COALESCE(u.cards_using_field, 0) AS cards_using_field,
                c.completeness_score_null_ratio,
                c.completeness_signal,
                v.observed_values_signal,
                v.cardinality_signal,
                tc.is_likely_temporal_field,
                tc.span_days AS temporal_span_days,
                COALESCE(ff.has_duplicate_flag, 0) AS has_duplicate_flag,
                COALESCE(ff.has_conflict_flag, 0) AS has_conflict_flag
            FROM fields f
            LEFT JOIN tables t ON t.table_id = f.table_id
            LEFT JOIN databases d ON d.database_id = t.database_id
            LEFT JOIN card_usage u ON u.field_id = f.field_id
            LEFT JOIN completeness c ON c.field_id = f.field_id
            LEFT JOIN values_profile v ON v.field_id = f.field_id
            LEFT JOIN temporal_candidates tc ON tc.field_id = f.field_id
            LEFT JOIN field_flags ff ON ff.field_id = f.field_id
            ORDER BY t.database_id, f.table_id, f.field_id;
        """,
    }

    for name, sql in queries.items():
        (queries_dir / f"{name}.sql").write_text(sql.strip() + "\n", encoding="utf-8")

    con = duckdb.connect(str(duckdb_path), read_only=True)
    issues: list[AnalysisIssue] = []
    outputs: dict[str, dict[str, Any]] = {}
    try:
        table_rows = con.execute(queries["extraction_table_base"]).fetchall()
        table_columns = [col[0] for col in con.description]
        field_rows = con.execute(queries["extraction_field_base"]).fetchall()
        field_columns = [col[0] for col in con.description]
    except Exception as exc:  # noqa: BLE001
        con.close()
        print(f"Error: failed loading extraction base data: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()

    tables = [dict(zip(table_columns, row)) for row in table_rows]
    fields = [dict(zip(field_columns, row)) for row in field_rows]

    table_output_rows: list[tuple[Any, ...]] = []
    field_output_rows: list[tuple[Any, ...]] = []

    for row in tables:
        score = 0.0
        evidence: list[str] = []

        active = parse_bool(row.get("active"))
        visible = row.get("visibility_type") not in {"hidden", "retired"}
        if active is not False and visible:
            score += 20
            evidence.append("table_active_visible")

        cards_using = int(row.get("cards_using_table") or 0)
        dashboards_using = int(row.get("dashboards_using_table") or 0)
        if cards_using > 0:
            score += min(20, 5 + (cards_using * 1.5))
            evidence.append("cards_usage")
        if dashboards_using > 0:
            score += min(20, 8 + (dashboards_using * 2.0))
            evidence.append("dashboard_usage")

        completeness = parse_float(row.get("avg_completeness_score"))
        if completeness is not None:
            completeness_points = min(20.0, max(0.0, completeness / 5.0))
            score += completeness_points
            evidence.append("completeness")

        historical_signal = row.get("historical_suitability_signal")
        if historical_signal == "historically_strong":
            score += 20
            evidence.append("historical_strong")
        elif historical_signal == "historically_moderate":
            score += 12
            evidence.append("historical_moderate")
        elif historical_signal == "historically_weak":
            score += 4
            evidence.append("historical_weak")

        score = round(min(score, 100.0), 2)
        category = category_from_score(score)

        has_duplicate_flag = int(row.get("has_duplicate_flag") or 0) == 1
        has_conflict_flag = int(row.get("has_conflict_flag") or 0) == 1

        table_output_rows.append(
            (
                row.get("database_id"),
                row.get("database_name"),
                row.get("table_id"),
                row.get("table_name"),
                row.get("table_display_name"),
                score,
                category,
                cards_using,
                dashboards_using,
                completeness,
                historical_signal,
                row.get("span_days"),
                has_duplicate_flag,
                has_conflict_flag,
                "|".join(evidence),
            )
        )

    for row in fields:
        score = 0.0
        evidence: list[str] = []

        active = parse_bool(row.get("active"))
        visible = row.get("visibility_type") not in {"hidden", "retired"}
        if active is not False and visible:
            score += 20
            evidence.append("field_active_visible")

        cards_using = int(row.get("cards_using_field") or 0)
        if cards_using > 0:
            score += min(20, 5 + (cards_using * 2.0))
            evidence.append("field_usage_in_cards")

        completeness = parse_float(row.get("completeness_score_null_ratio"))
        if completeness is not None:
            score += min(20.0, max(0.0, completeness / 5.0))
            evidence.append("field_completeness")

        observed_values_signal = row.get("observed_values_signal")
        cardinality_signal = row.get("cardinality_signal")
        if observed_values_signal == "list":
            score += 15
            evidence.append("observed_values_list")
        elif observed_values_signal == "search":
            score += 8
            evidence.append("observed_values_search")

        if cardinality_signal == "likely_categorical":
            score += 10
            evidence.append("likely_categorical")
        elif cardinality_signal == "possibly_categorical":
            score += 5
            evidence.append("possibly_categorical")

        if parse_bool(row.get("is_likely_temporal_field")):
            score += 10
            evidence.append("likely_temporal_field")

        score = round(min(score, 100.0), 2)
        category = category_from_score(score)

        has_duplicate_flag = int(row.get("has_duplicate_flag") or 0) == 1
        has_conflict_flag = int(row.get("has_conflict_flag") or 0) == 1

        field_output_rows.append(
            (
                row.get("database_id"),
                row.get("database_name"),
                row.get("table_id"),
                row.get("table_name"),
                row.get("field_id"),
                row.get("field_name"),
                row.get("field_display_name"),
                score,
                category,
                cards_using,
                completeness,
                row.get("completeness_signal"),
                observed_values_signal,
                cardinality_signal,
                row.get("is_likely_temporal_field"),
                row.get("temporal_span_days"),
                has_duplicate_flag,
                has_conflict_flag,
                "|".join(evidence),
            )
        )

    table_columns_out = [
        "database_id",
        "database_name",
        "table_id",
        "table_name",
        "table_display_name",
        "extraction_score",
        "extraction_category",
        "cards_using_table",
        "dashboards_using_table",
        "avg_completeness_score",
        "historical_suitability_signal",
        "historical_span_days",
        "has_duplicate_flag",
        "has_conflict_flag",
        "evidence_signals",
    ]
    field_columns_out = [
        "database_id",
        "database_name",
        "table_id",
        "table_name",
        "field_id",
        "field_name",
        "field_display_name",
        "extraction_score",
        "extraction_category",
        "cards_using_field",
        "completeness_score_null_ratio",
        "completeness_signal",
        "observed_values_signal",
        "cardinality_signal",
        "is_likely_temporal_field",
        "temporal_span_days",
        "has_duplicate_flag",
        "has_conflict_flag",
        "evidence_signals",
    ]

    table_path = extraction_dir / "extraction_table_candidates.csv"
    field_path = extraction_dir / "extraction_field_candidates.csv"
    write_csv(table_path, table_columns_out, table_output_rows)
    write_csv(field_path, field_columns_out, field_output_rows)
    outputs["extraction_table_candidates"] = {"file": str(table_path), "row_count": len(table_output_rows)}
    outputs["extraction_field_candidates"] = {"file": str(field_path), "row_count": len(field_output_rows)}
    print(f"Wrote {table_path} ({len(table_output_rows)} rows)")
    print(f"Wrote {field_path} ({len(field_output_rows)} rows)")

    # Global summaries
    table_summary = {}
    for row in table_output_rows:
        category = str(row[6])
        table_summary[category] = table_summary.get(category, 0) + 1
    table_summary_rows = [(k, v) for k, v in sorted(table_summary.items())]
    table_summary_path = extraction_dir / "extraction_table_priority_summary.csv"
    write_csv(table_summary_path, ["extraction_category", "table_count"], table_summary_rows)
    outputs["extraction_table_priority_summary"] = {
        "file": str(table_summary_path),
        "row_count": len(table_summary_rows),
    }
    print(f"Wrote {table_summary_path} ({len(table_summary_rows)} rows)")

    field_summary = {}
    for row in field_output_rows:
        category = str(row[8])
        field_summary[category] = field_summary.get(category, 0) + 1
    field_summary_rows = [(k, v) for k, v in sorted(field_summary.items())]
    field_summary_path = extraction_dir / "extraction_field_priority_summary.csv"
    write_csv(field_summary_path, ["extraction_category", "field_count"], field_summary_rows)
    outputs["extraction_field_priority_summary"] = {
        "file": str(field_summary_path),
        "row_count": len(field_summary_rows),
    }
    print(f"Wrote {field_summary_path} ({len(field_summary_rows)} rows)")

    # Per-database outputs
    db_ids = sorted({row[0] for row in table_output_rows if isinstance(row[0], int)})
    db_manifest_rows: list[tuple[Any, ...]] = []
    for db_id in db_ids:
        db_table_rows = [row for row in table_output_rows if row[0] == db_id]
        db_field_rows = [row for row in field_output_rows if row[0] == db_id]
        db_name = next((row[1] for row in db_table_rows if isinstance(row[1], str)), "unknown")
        db_slug = slugify(db_name)
        prefix = f"database_{db_id}_{db_slug}"

        db_table_path = per_database_dir / f"{prefix}_extraction_table_candidates.csv"
        db_field_path = per_database_dir / f"{prefix}_extraction_field_candidates.csv"
        write_csv(db_table_path, table_columns_out, db_table_rows)
        write_csv(db_field_path, field_columns_out, db_field_rows)

        db_manifest_rows.append(
            (
                db_id,
                db_name,
                str(db_table_path),
                len(db_table_rows),
                str(db_field_path),
                len(db_field_rows),
            )
        )

    db_manifest_path = extraction_dir / "extraction_per_database_manifest.csv"
    write_csv(
        db_manifest_path,
        [
            "database_id",
            "database_name",
            "table_candidates_file",
            "table_candidates_row_count",
            "field_candidates_file",
            "field_candidates_row_count",
        ],
        db_manifest_rows,
    )
    outputs["extraction_per_database_manifest"] = {
        "file": str(db_manifest_path),
        "row_count": len(db_manifest_rows),
    }
    print(f"Wrote {db_manifest_path} ({len(db_manifest_rows)} rows)")

    summary = {
        "generated_at": utc_now_iso(),
        "duckdb_file": str(duckdb_path),
        "report_count": len(outputs),
        "successful_report_count": len(outputs),
        "issue_count": len(issues),
        "issues": [{"report_name": i.report_name, "error": i.error} for i in issues],
        "outputs": outputs,
    }
    summary_json_path = reports_dir / "extraction_summary_overview.json"
    summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary_json_path}")

    summary_rows = [(k, v.get("file"), v.get("row_count"), "ok", "") for k, v in outputs.items()]
    summary_csv_path = reports_dir / "extraction_summary_outputs.csv"
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
