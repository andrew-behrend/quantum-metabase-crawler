from __future__ import annotations

import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
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


def normalize_name(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    lowered = value.strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def write_csv(path: Path, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def read_csv_map(path: Path, key_col: str) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    if not path.exists():
        return result
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = row.get(key_col)
            if key:
                result[key] = row
    return result


def parse_keywords(raw: str) -> list[str]:
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def any_keyword_match(value: str, keywords: list[str]) -> bool:
    lowered = value.lower()
    return any(keyword in lowered for keyword in keywords)


def to_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def to_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def get_setting(name: str, env_settings: dict[str, str]) -> str | None:
    value = os.getenv(name)
    if value is not None and value.strip():
        return value
    value = env_settings.get(name)
    if isinstance(value, str) and value.strip():
        return value
    return None


def objective_keywords(objective: str, env_settings: dict[str, str]) -> list[str]:
    env_name = f"TENANT_{objective.upper()}_KEYWORDS"
    defaults = {
        "conversion": "convert,conversion,trial_converted,active_subscription,order,purchase",
        "upsell": "upsell,upgrade,plan,seats,invoice,payment,total",
        "churn": "churn,cancel,canceled,cancelled,inactive,trial_ends",
    }
    raw = get_setting(env_name, env_settings) or defaults[objective]
    return parse_keywords(raw)


def objective_target_types(objective: str) -> set[str]:
    if objective == "conversion":
        return {"type/Boolean", "type/DateTime", "type/Date", "type/Integer", "type/Float"}
    if objective == "upsell":
        return {"type/Float", "type/Integer", "type/DateTime", "type/Date", "type/Text"}
    return {"type/Boolean", "type/DateTime", "type/Date", "type/Text"}


def score_to_strength(score: float) -> str:
    if score >= 75:
        return "strong"
    if score >= 50:
        return "moderate"
    if score >= 25:
        return "weak"
    return "very_weak"


def main() -> int:
    env_values = dotenv_values(".env")
    env_settings = {k: v for k, v in env_values.items() if isinstance(v, str)}
    output_dir_raw = env_settings.get("OUTPUT_DIR")
    if not output_dir_raw:
        print("Configuration error: OUTPUT_DIR is missing in .env", file=sys.stderr)
        return 1

    output_dir = Path(output_dir_raw)
    duckdb_path = output_dir / "analysis" / "metabase.duckdb"
    reports_dir = output_dir / "analysis" / "reports"
    queries_dir = output_dir / "analysis" / "queries"
    modeling_dir = reports_dir / "modeling"
    per_database_dir = modeling_dir / "per_database"

    reports_dir.mkdir(parents=True, exist_ok=True)
    queries_dir.mkdir(parents=True, exist_ok=True)
    modeling_dir.mkdir(parents=True, exist_ok=True)
    per_database_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_path.exists():
        print(f"Error: DuckDB file not found: {duckdb_path}", file=sys.stderr)
        return 1

    queries = {
        "modeling_base_fields": """
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
                f.visibility_type
            FROM fields f
            LEFT JOIN tables t ON t.table_id = f.table_id
            LEFT JOIN databases d ON d.database_id = t.database_id
            ORDER BY t.database_id, t.table_id, f.field_id;
        """,
        "modeling_base_tables": """
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

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        field_rows = con.execute(queries["modeling_base_fields"]).fetchall()
        field_columns = [col[0] for col in con.description]
        table_rows = con.execute(queries["modeling_base_tables"]).fetchall()
        table_columns = [col[0] for col in con.description]
    except Exception as exc:  # noqa: BLE001
        con.close()
        print(f"Error: failed loading modeling base data: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()

    fields = [dict(zip(field_columns, row)) for row in field_rows]
    tables = [dict(zip(table_columns, row)) for row in table_rows]

    completeness_map = read_csv_map(
        output_dir / "analysis" / "reports" / "completeness" / "completeness_field_profile.csv",
        "field_id",
    )
    historical_table_map = read_csv_map(
        output_dir / "analysis" / "reports" / "historical" / "historical_table_depth.csv",
        "table_id",
    )
    extraction_field_map = read_csv_map(
        output_dir / "analysis" / "reports" / "extraction" / "extraction_field_candidates.csv",
        "field_id",
    )
    extraction_table_map = read_csv_map(
        output_dir / "analysis" / "reports" / "extraction" / "extraction_table_candidates.csv",
        "table_id",
    )

    objectives = ["conversion", "upsell", "churn"]
    objective_adequacy_rows: list[tuple[Any, ...]] = []
    predictor_rows_all: list[tuple[Any, ...]] = []
    association_rows_all: list[tuple[Any, ...]] = []
    redundancy_rows: list[tuple[Any, ...]] = []

    fields_by_table: dict[int, list[dict[str, Any]]] = {}
    for field in fields:
        table_id = to_int(field.get("table_id"))
        if table_id is not None:
            fields_by_table.setdefault(table_id, []).append(field)

    for objective in objectives:
        keywords = objective_keywords(objective, env_settings)
        target_types = objective_target_types(objective)

        target_candidates: list[dict[str, Any]] = []
        predictor_candidates: list[dict[str, Any]] = []
        blockers: list[str] = []

        for field in fields:
            field_name = str(field.get("field_name") or "")
            display_name = str(field.get("field_display_name") or "")
            semantic_type = str(field.get("semantic_type") or "")
            base_type = str(field.get("base_type") or "")
            effective_type = str(field.get("effective_type") or "")
            joined_name = f"{field_name} {display_name} {semantic_type}".lower()

            is_target_name_match = any_keyword_match(joined_name, keywords)
            is_target_type_match = any(
                token.lower() in (base_type + " " + effective_type + " " + semantic_type).lower()
                for token in target_types
            )

            extraction_score = to_float(
                extraction_field_map.get(str(field.get("field_id")), {}).get("extraction_score")
            )
            completeness_score = to_float(
                completeness_map.get(str(field.get("field_id")), {}).get("completeness_score_null_ratio")
            )

            if is_target_name_match and is_target_type_match:
                target_candidates.append(field)
            else:
                predictor_score = 0.0
                if extraction_score is not None:
                    predictor_score += min(50.0, extraction_score * 0.5)
                if completeness_score is not None:
                    predictor_score += min(30.0, completeness_score * 0.3)
                if any_keyword_match(joined_name, keywords):
                    predictor_score += 20.0
                predictor_score = round(min(predictor_score, 100.0), 2)
                if predictor_score > 0:
                    predictor = dict(field)
                    predictor["predictor_score"] = predictor_score
                    predictor_candidates.append(predictor)

        temporal_fields = [
            field
            for field in fields
            if "date" in str(field.get("base_type") or "").lower()
            or "time" in str(field.get("base_type") or "").lower()
            or "date" in str(field.get("effective_type") or "").lower()
            or "time" in str(field.get("effective_type") or "").lower()
        ]
        entity_fields = [
            field
            for field in fields
            if "type/fk" in str(field.get("semantic_type") or "").lower()
            or "type/email" in str(field.get("semantic_type") or "").lower()
            or "type/pk" in str(field.get("semantic_type") or "").lower()
            or str(field.get("field_name") or "").lower().endswith("_id")
        ]

        if not target_candidates:
            blockers.append("no_target_candidate_fields_found")
        if not temporal_fields:
            blockers.append("no_temporal_fields_found")
        if not entity_fields:
            blockers.append("no_entity_key_fields_found")

        if not blockers:
            definability = "definable"
            conclusion = "enough_here_to_continue"
        elif len(blockers) <= 2 and target_candidates:
            definability = "partially_definable"
            conclusion = "enough_here_to_continue_with_cautions"
        else:
            definability = "not_yet_definable"
            conclusion = "major_issues_should_be_explored_before_modeling"

        objective_adequacy_rows.append(
            (
                objective,
                get_setting("TENANT_ENTITY_GRAIN", env_settings) or "user",
                definability,
                len(target_candidates),
                len(predictor_candidates),
                len(temporal_fields),
                len(entity_fields),
                "|".join(blockers),
                conclusion,
            )
        )

        top_predictors = sorted(
            predictor_candidates,
            key=lambda row: float(row.get("predictor_score", 0.0)),
            reverse=True,
        )[:200]

        for rank, predictor in enumerate(top_predictors, start=1):
            predictor_rows_all.append(
                (
                    objective,
                    predictor.get("database_id"),
                    predictor.get("database_name"),
                    predictor.get("table_id"),
                    predictor.get("table_name"),
                    predictor.get("field_id"),
                    predictor.get("field_name"),
                    predictor.get("field_display_name"),
                    predictor.get("base_type"),
                    predictor.get("effective_type"),
                    predictor.get("semantic_type"),
                    predictor.get("predictor_score"),
                    rank,
                )
            )

            association_rows_all.append(
                (
                    objective,
                    predictor.get("field_id"),
                    predictor.get("field_name"),
                    predictor.get("table_id"),
                    predictor.get("table_name"),
                    predictor.get("predictor_score"),
                    score_to_strength(float(predictor.get("predictor_score", 0.0))),
                    "heuristic_signal_from_completeness_extraction_and_objective_keyword_alignment",
                )
            )

    # Variable co-movement / redundancy heuristic.
    for table_id, table_fields in fields_by_table.items():
        for left, right in combinations(table_fields, 2):
            left_name = normalize_name(left.get("field_name") or left.get("field_display_name"))
            right_name = normalize_name(right.get("field_name") or right.get("field_display_name"))
            if not left_name or not right_name:
                continue

            same_semantic = (
                str(left.get("semantic_type") or "").lower()
                == str(right.get("semantic_type") or "").lower()
                and str(left.get("semantic_type") or "").strip() != ""
            )
            same_effective = (
                str(left.get("effective_type") or "").lower()
                == str(right.get("effective_type") or "").lower()
                and str(left.get("effective_type") or "").strip() != ""
            )
            close_name = left_name == right_name or left_name in right_name or right_name in left_name

            if not (same_semantic or same_effective or close_name):
                continue

            redundancy_strength = "moderate_proxy_risk"
            if same_semantic and same_effective and close_name:
                redundancy_strength = "high_proxy_risk"

            redundancy_rows.append(
                (
                    left.get("database_id"),
                    left.get("database_name"),
                    table_id,
                    left.get("table_name"),
                    left.get("field_id"),
                    left.get("field_name"),
                    right.get("field_id"),
                    right.get("field_name"),
                    same_semantic,
                    same_effective,
                    close_name,
                    redundancy_strength,
                )
            )

    adequacy_columns = [
        "objective",
        "tenant_entity_grain",
        "definability",
        "target_candidate_count",
        "predictor_candidate_count",
        "temporal_field_count",
        "entity_key_field_count",
        "blockers",
        "high_level_conclusion",
    ]
    predictor_columns = [
        "objective",
        "database_id",
        "database_name",
        "table_id",
        "table_name",
        "field_id",
        "field_name",
        "field_display_name",
        "base_type",
        "effective_type",
        "semantic_type",
        "predictor_strength_score",
        "predictor_rank_within_objective",
    ]
    association_columns = [
        "objective",
        "field_id",
        "field_name",
        "table_id",
        "table_name",
        "association_signal_score",
        "association_strength_label",
        "association_note",
    ]
    redundancy_columns = [
        "database_id",
        "database_name",
        "table_id",
        "table_name",
        "left_field_id",
        "left_field_name",
        "right_field_id",
        "right_field_name",
        "same_semantic_type",
        "same_effective_type",
        "name_overlap_signal",
        "redundancy_signal",
    ]

    outputs: dict[str, dict[str, Any]] = {}

    # Objective-level adequacy outputs.
    adequacy_path = modeling_dir / "modeling_objective_adequacy.csv"
    write_csv(adequacy_path, adequacy_columns, objective_adequacy_rows)
    outputs["modeling_objective_adequacy"] = {"file": str(adequacy_path), "row_count": len(objective_adequacy_rows)}
    print(f"Wrote {adequacy_path} ({len(objective_adequacy_rows)} rows)")

    # Split objective reports.
    for objective in ["conversion", "upsell", "churn"]:
        objective_row = [row for row in objective_adequacy_rows if row[0] == objective]
        objective_predictors = [row for row in predictor_rows_all if row[0] == objective]
        objective_assoc = [row for row in association_rows_all if row[0] == objective]

        adequacy_obj_path = modeling_dir / f"modeling_{objective}_adequacy.csv"
        predictors_obj_path = modeling_dir / f"modeling_{objective}_predictor_candidates.csv"
        assoc_obj_path = modeling_dir / f"modeling_{objective}_variable_association.csv"

        write_csv(adequacy_obj_path, adequacy_columns, objective_row)
        write_csv(predictors_obj_path, predictor_columns, objective_predictors)
        write_csv(assoc_obj_path, association_columns, objective_assoc)

        outputs[f"modeling_{objective}_adequacy"] = {
            "file": str(adequacy_obj_path),
            "row_count": len(objective_row),
        }
        outputs[f"modeling_{objective}_predictor_candidates"] = {
            "file": str(predictors_obj_path),
            "row_count": len(objective_predictors),
        }
        outputs[f"modeling_{objective}_variable_association"] = {
            "file": str(assoc_obj_path),
            "row_count": len(objective_assoc),
        }

        print(f"Wrote {adequacy_obj_path} ({len(objective_row)} rows)")
        print(f"Wrote {predictors_obj_path} ({len(objective_predictors)} rows)")
        print(f"Wrote {assoc_obj_path} ({len(objective_assoc)} rows)")

    redundancy_path = modeling_dir / "modeling_variable_redundancy.csv"
    write_csv(redundancy_path, redundancy_columns, redundancy_rows)
    outputs["modeling_variable_redundancy"] = {
        "file": str(redundancy_path),
        "row_count": len(redundancy_rows),
    }
    print(f"Wrote {redundancy_path} ({len(redundancy_rows)} rows)")

    # Per-database variants.
    db_ids = sorted({row[1] for row in predictor_rows_all if isinstance(row[1], int)})
    per_db_manifest_rows: list[tuple[Any, ...]] = []
    for db_id in db_ids:
        db_name = next((row[2] for row in predictor_rows_all if row[1] == db_id), "unknown")
        db_slug = slugify(db_name)
        prefix = f"database_{db_id}_{db_slug}"

        db_predictors = [row for row in predictor_rows_all if row[1] == db_id]
        db_assoc = [row for row in association_rows_all if row[3] is not None and row[3] in {
            p[3] for p in db_predictors
        }]
        db_redundancy = [row for row in redundancy_rows if row[0] == db_id]

        predictors_path = per_database_dir / f"{prefix}_modeling_predictor_candidates.csv"
        assoc_path = per_database_dir / f"{prefix}_modeling_variable_association.csv"
        redundancy_db_path = per_database_dir / f"{prefix}_modeling_variable_redundancy.csv"

        write_csv(predictors_path, predictor_columns, db_predictors)
        write_csv(assoc_path, association_columns, db_assoc)
        write_csv(redundancy_db_path, redundancy_columns, db_redundancy)

        per_db_manifest_rows.append(
            (
                db_id,
                db_name,
                str(predictors_path),
                len(db_predictors),
                str(assoc_path),
                len(db_assoc),
                str(redundancy_db_path),
                len(db_redundancy),
            )
        )

    per_db_manifest_path = modeling_dir / "modeling_per_database_manifest.csv"
    write_csv(
        per_db_manifest_path,
        [
            "database_id",
            "database_name",
            "predictors_file",
            "predictors_row_count",
            "association_file",
            "association_row_count",
            "redundancy_file",
            "redundancy_row_count",
        ],
        per_db_manifest_rows,
    )
    outputs["modeling_per_database_manifest"] = {
        "file": str(per_db_manifest_path),
        "row_count": len(per_db_manifest_rows),
    }
    print(f"Wrote {per_db_manifest_path} ({len(per_db_manifest_rows)} rows)")

    summary = {
        "generated_at": utc_now_iso(),
        "duckdb_file": str(duckdb_path),
        "report_count": len(outputs),
        "successful_report_count": len(outputs),
        "issue_count": 0,
        "issues": [],
        "outputs": outputs,
    }
    summary_json_path = reports_dir / "modeling_summary_overview.json"
    summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary_json_path}")

    summary_rows = [(k, v.get("file"), v.get("row_count"), "ok", "") for k, v in outputs.items()]
    summary_csv_path = reports_dir / "modeling_summary_outputs.csv"
    write_csv(
        summary_csv_path,
        ["report_name", "file", "row_count", "status", "error"],
        summary_rows,
    )
    print(f"Wrote {summary_csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
