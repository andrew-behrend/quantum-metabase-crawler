from __future__ import annotations

import csv
import hashlib
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


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def confidence_from_score(score: int) -> str:
    if score >= 85:
        return "high"
    if score >= 65:
        return "medium"
    return "low"


def write_csv(path: Path, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def to_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def fetch_rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
    return con.execute(sql).fetchall()


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

    issues: list[AnalysisIssue] = []

    queries: dict[str, str] = {
        "phase7_base_cards": """
            SELECT
                cd.card_id,
                cd.card_name,
                cd.normalized_card_name,
                cd.logic_type,
                cd.query_type,
                cd.database_id,
                cd.card_table_id,
                cd.reference_signature,
                cd.sql_hash,
                cd.notebook_hash,
                c.collection_id,
                c.archived,
                COUNT(DISTINCT rdc.dashboard_id) AS dashboard_count
            FROM card_definitions cd
            LEFT JOIN cards c ON c.card_id = cd.card_id
            LEFT JOIN rel_dashboard_to_cards rdc ON rdc.card_id = cd.card_id
            GROUP BY
                cd.card_id,
                cd.card_name,
                cd.normalized_card_name,
                cd.logic_type,
                cd.query_type,
                cd.database_id,
                cd.card_table_id,
                cd.reference_signature,
                cd.sql_hash,
                cd.notebook_hash,
                c.collection_id,
                c.archived
            ORDER BY cd.card_id;
        """,
        "phase7_base_dashboards": """
            SELECT
                d.dashboard_id,
                d.name,
                d.collection_id,
                d.archived,
                COUNT(DISTINCT rdc.card_id) AS card_count
            FROM dashboards d
            LEFT JOIN rel_dashboard_to_cards rdc ON rdc.dashboard_id = d.dashboard_id
            GROUP BY d.dashboard_id, d.name, d.collection_id, d.archived
            ORDER BY d.dashboard_id;
        """,
        "phase7_base_collections": """
            SELECT
                c.collection_id,
                c.name,
                c.parent_id,
                c.is_personal,
                c.archived,
                COUNT(DISTINCT rcc.item_id) AS content_count
            FROM collections c
            LEFT JOIN rel_collection_to_contents rcc ON rcc.collection_id = c.collection_id
            GROUP BY c.collection_id, c.name, c.parent_id, c.is_personal, c.archived
            ORDER BY c.collection_id;
        """,
        "phase7_name_groups": """
            SELECT
                normalized_card_name,
                COUNT(*) AS card_count,
                COUNT(DISTINCT reference_signature) AS distinct_reference_signatures,
                COUNT(DISTINCT logic_type) AS distinct_logic_types,
                COUNT(DISTINCT COALESCE(sql_hash, '')) AS distinct_sql_hashes,
                COUNT(DISTINCT COALESCE(notebook_hash, '')) AS distinct_notebook_hashes
            FROM card_definitions
            WHERE normalized_card_name <> ''
            GROUP BY normalized_card_name
            HAVING COUNT(*) > 1
            ORDER BY card_count DESC, normalized_card_name;
        """,
    }

    for name, sql in queries.items():
        (queries_dir / f"{name}.sql").write_text(sql.strip() + "\n", encoding="utf-8")

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        cards_rows = fetch_rows(con, queries["phase7_base_cards"])
        dashboards_rows = fetch_rows(con, queries["phase7_base_dashboards"])
        collections_rows = fetch_rows(con, queries["phase7_base_collections"])
    except Exception as exc:  # noqa: BLE001
        con.close()
        print(f"Error: failed to load base data: {exc}", file=sys.stderr)
        return 1

    card_columns = [
        "card_id",
        "card_name",
        "normalized_card_name",
        "logic_type",
        "query_type",
        "database_id",
        "card_table_id",
        "reference_signature",
        "sql_hash",
        "notebook_hash",
        "collection_id",
        "archived",
        "dashboard_count",
    ]

    cards: list[dict[str, Any]] = [dict(zip(card_columns, row)) for row in cards_rows]
    dashboards: list[dict[str, Any]] = [
        dict(zip(["dashboard_id", "name", "collection_id", "archived", "card_count"], row))
        for row in dashboards_rows
    ]
    collections: list[dict[str, Any]] = [
        dict(zip(["collection_id", "name", "parent_id", "is_personal", "archived", "content_count"], row))
        for row in collections_rows
    ]

    cards_by_name: dict[str, list[dict[str, Any]]] = {}
    for card in cards:
        normalized_name = card.get("normalized_card_name")
        if not isinstance(normalized_name, str) or not normalized_name:
            continue
        cards_by_name.setdefault(normalized_name, []).append(card)

    duplicate_rows: list[tuple[Any, ...]] = []
    conflict_rows: list[tuple[Any, ...]] = []
    retirement_rows: list[tuple[Any, ...]] = []

    # Duplicate and conflict candidate groups from cards with same normalized name.
    for normalized_name, group_cards in sorted(cards_by_name.items()):
        if len(group_cards) < 2:
            continue

        reference_signatures = {card.get("reference_signature") for card in group_cards}
        logic_types = {card.get("logic_type") for card in group_cards}
        sql_hashes = {card.get("sql_hash") for card in group_cards if card.get("sql_hash")}
        notebook_hashes = {
            card.get("notebook_hash") for card in group_cards if card.get("notebook_hash")
        }

        # Duplicate signal group: same normalized name plus at least one strong definition match.
        if (
            len(reference_signatures) == 1
            or len(sql_hashes) == 1 and len(sql_hashes) > 0
            or len(notebook_hashes) == 1 and len(notebook_hashes) > 0
        ):
            signal_types: list[str] = ["same_normalized_name"]
            score = 40

            if len(reference_signatures) == 1:
                signal_types.append("same_reference_signature")
                score += 25
            if len(logic_types) == 1:
                signal_types.append("same_logic_type")
                score += 10
            if len(sql_hashes) == 1 and len(sql_hashes) > 0:
                signal_types.append("same_sql_hash")
                score += 15
            if len(notebook_hashes) == 1 and len(notebook_hashes) > 0:
                signal_types.append("same_notebook_hash")
                score += 10

            score = min(score, 100)
            confidence = confidence_from_score(score)
            candidate_group_id = stable_hash(
                f"duplicate|{normalized_name}|{sorted(reference_signatures)}|{sorted(logic_types)}"
            )

            for card in sorted(group_cards, key=lambda c: c.get("card_id") or 0):
                duplicate_rows.append(
                    (
                        candidate_group_id,
                        "duplicate",
                        "card",
                        card.get("card_id"),
                        card.get("card_name"),
                        normalized_name,
                        "|".join(signal_types),
                        score,
                        confidence,
                        len(group_cards),
                        card.get("database_id"),
                        card.get("card_table_id"),
                        card.get("dashboard_count"),
                        card.get("collection_id"),
                    )
                )

        # Conflicting-definition group: same normalized name but materially different logic/definitions.
        conflicting_signals: list[str] = []
        if len(logic_types) > 1:
            conflicting_signals.append("same_name_different_logic_type")
        if len(reference_signatures) > 1:
            conflicting_signals.append("same_name_different_reference_signature")
        if len(sql_hashes) > 1:
            conflicting_signals.append("same_name_different_sql_hash")
        if len(notebook_hashes) > 1:
            conflicting_signals.append("same_name_different_notebook_hash")

        if conflicting_signals:
            score = 50
            score += min(40, 12 * len(conflicting_signals))
            score = min(score, 100)
            confidence = confidence_from_score(score)
            candidate_group_id = stable_hash(
                f"conflict|{normalized_name}|{sorted(reference_signatures)}|{sorted(logic_types)}"
            )

            for card in sorted(group_cards, key=lambda c: c.get("card_id") or 0):
                conflict_rows.append(
                    (
                        candidate_group_id,
                        "conflicting_definition",
                        "card",
                        card.get("card_id"),
                        card.get("card_name"),
                        normalized_name,
                        "|".join(conflicting_signals),
                        score,
                        confidence,
                        len(group_cards),
                        card.get("logic_type"),
                        card.get("database_id"),
                        card.get("card_table_id"),
                        card.get("dashboard_count"),
                        card.get("collection_id"),
                    )
                )

    # Retirement/cleanup candidates for cards.
    for card in cards:
        card_id = card.get("card_id")
        card_name = card.get("card_name")
        dashboard_count = to_int(card.get("dashboard_count")) or 0
        collection_id = card.get("collection_id")

        if dashboard_count == 0:
            group_id = stable_hash(f"retire|card|no_dashboard_usage|{card_id}")
            score = 70
            retirement_rows.append(
                (
                    group_id,
                    "retirement_cleanup",
                    "card",
                    card_id,
                    card_name,
                    "no_dashboard_usage",
                    score,
                    confidence_from_score(score),
                    "Card has no dashboard relationships",
                    card.get("normalized_card_name"),
                    card.get("logic_type"),
                    card.get("database_id"),
                    card.get("card_table_id"),
                    dashboard_count,
                    collection_id,
                )
            )

        if collection_id is None:
            group_id = stable_hash(f"retire|card|no_collection_usage|{card_id}")
            score = 55
            retirement_rows.append(
                (
                    group_id,
                    "retirement_cleanup",
                    "card",
                    card_id,
                    card_name,
                    "no_collection_usage",
                    score,
                    confidence_from_score(score),
                    "Card is not placed in a collection",
                    card.get("normalized_card_name"),
                    card.get("logic_type"),
                    card.get("database_id"),
                    card.get("card_table_id"),
                    dashboard_count,
                    collection_id,
                )
            )

    # Superseded card candidates: same normalized name and signature, but unused compared to a used peer.
    for normalized_name, group_cards in sorted(cards_by_name.items()):
        if len(group_cards) < 2:
            continue

        subgroups: dict[tuple[Any, Any], list[dict[str, Any]]] = {}
        for card in group_cards:
            key = (card.get("reference_signature"), card.get("logic_type"))
            subgroups.setdefault(key, []).append(card)

        for (reference_signature, logic_type), members in subgroups.items():
            if len(members) < 2:
                continue

            used = [m for m in members if (to_int(m.get("dashboard_count")) or 0) > 0]
            unused = [m for m in members if (to_int(m.get("dashboard_count")) or 0) == 0]
            if not used or not unused:
                continue

            peer_ids = ",".join(str(m.get("card_id")) for m in sorted(used, key=lambda c: c.get("card_id")))
            for card in unused:
                group_id = stable_hash(
                    f"retire|card|superseded_by_used_peer|{normalized_name}|{reference_signature}|{card.get('card_id')}"
                )
                score = 85
                retirement_rows.append(
                    (
                        group_id,
                        "retirement_cleanup",
                        "card",
                        card.get("card_id"),
                        card.get("card_name"),
                        "superseded_by_used_peer",
                        score,
                        confidence_from_score(score),
                        f"Similar card(s) with same definition have dashboard usage (peer_card_ids={peer_ids})",
                        normalized_name,
                        logic_type,
                        card.get("database_id"),
                        card.get("card_table_id"),
                        card.get("dashboard_count"),
                        card.get("collection_id"),
                    )
                )

    # Retirement/cleanup candidates for dashboards.
    for dash in dashboards:
        dashboard_id = dash.get("dashboard_id")
        card_count = to_int(dash.get("card_count")) or 0

        if card_count == 0:
            group_id = stable_hash(f"retire|dashboard|no_card_usage|{dashboard_id}")
            score = 80
            retirement_rows.append(
                (
                    group_id,
                    "retirement_cleanup",
                    "dashboard",
                    dashboard_id,
                    dash.get("name"),
                    "no_card_usage",
                    score,
                    confidence_from_score(score),
                    "Dashboard has no linked cards",
                    None,
                    None,
                    None,
                    None,
                    card_count,
                    dash.get("collection_id"),
                )
            )

        if dash.get("collection_id") is None:
            group_id = stable_hash(f"retire|dashboard|no_collection_usage|{dashboard_id}")
            score = 50
            retirement_rows.append(
                (
                    group_id,
                    "retirement_cleanup",
                    "dashboard",
                    dashboard_id,
                    dash.get("name"),
                    "no_collection_usage",
                    score,
                    confidence_from_score(score),
                    "Dashboard is not placed in a collection",
                    None,
                    None,
                    None,
                    None,
                    card_count,
                    dash.get("collection_id"),
                )
            )

    # Retirement/cleanup candidates for collections.
    for collection in collections:
        collection_id = collection.get("collection_id")
        content_count = to_int(collection.get("content_count")) or 0

        if content_count == 0:
            group_id = stable_hash(f"retire|collection|no_content_usage|{collection_id}")
            score = 80
            retirement_rows.append(
                (
                    group_id,
                    "retirement_cleanup",
                    "collection",
                    collection_id,
                    collection.get("name"),
                    "no_content_usage",
                    score,
                    confidence_from_score(score),
                    "Collection has no linked contents",
                    None,
                    None,
                    None,
                    None,
                    content_count,
                    collection_id,
                )
            )

        if bool(collection.get("is_personal")) and content_count == 0:
            group_id = stable_hash(f"retire|collection|empty_personal_collection|{collection_id}")
            score = 60
            retirement_rows.append(
                (
                    group_id,
                    "retirement_cleanup",
                    "collection",
                    collection_id,
                    collection.get("name"),
                    "empty_personal_collection",
                    score,
                    confidence_from_score(score),
                    "Personal collection has no linked contents",
                    None,
                    None,
                    None,
                    None,
                    content_count,
                    collection_id,
                )
            )

    duplicate_path = reports_dir / "candidate_duplicates.csv"
    conflict_path = reports_dir / "candidate_conflicting_definitions.csv"
    retirement_path = reports_dir / "candidate_retirement_cleanup.csv"

    duplicate_columns = [
        "candidate_group_id",
        "candidate_type",
        "object_type",
        "object_id",
        "object_name",
        "normalized_name",
        "signal_types",
        "score",
        "confidence",
        "group_size",
        "database_id",
        "table_id",
        "dashboard_count",
        "collection_id",
    ]
    conflict_columns = [
        "candidate_group_id",
        "candidate_type",
        "object_type",
        "object_id",
        "object_name",
        "normalized_name",
        "signal_types",
        "score",
        "confidence",
        "group_size",
        "logic_type",
        "database_id",
        "table_id",
        "dashboard_count",
        "collection_id",
    ]
    retirement_columns = [
        "candidate_group_id",
        "candidate_type",
        "object_type",
        "object_id",
        "object_name",
        "signal_type",
        "score",
        "confidence",
        "evidence",
        "normalized_name",
        "logic_type",
        "database_id",
        "table_id",
        "usage_count",
        "collection_id",
    ]

    write_csv(duplicate_path, duplicate_columns, duplicate_rows)
    print(f"Wrote {duplicate_path} ({len(duplicate_rows)} rows)")

    write_csv(conflict_path, conflict_columns, conflict_rows)
    print(f"Wrote {conflict_path} ({len(conflict_rows)} rows)")

    write_csv(retirement_path, retirement_columns, retirement_rows)
    print(f"Wrote {retirement_path} ({len(retirement_rows)} rows)")

    signal_counts: dict[tuple[str, str], int] = {}

    for row in duplicate_rows:
        candidate_type = str(row[1])
        signal_types = str(row[6]).split("|")
        for signal in signal_types:
            key = (candidate_type, signal)
            signal_counts[key] = signal_counts.get(key, 0) + 1

    for row in conflict_rows:
        candidate_type = str(row[1])
        signal_types = str(row[6]).split("|")
        for signal in signal_types:
            key = (candidate_type, signal)
            signal_counts[key] = signal_counts.get(key, 0) + 1

    for row in retirement_rows:
        candidate_type = str(row[1])
        signal_type = str(row[5])
        key = (candidate_type, signal_type)
        signal_counts[key] = signal_counts.get(key, 0) + 1

    signal_summary_rows = [
        (candidate_type, signal_type, count)
        for (candidate_type, signal_type), count in sorted(signal_counts.items())
    ]
    signal_summary_path = reports_dir / "candidate_signal_summary.csv"
    write_csv(signal_summary_path, ["candidate_type", "signal_type", "row_count"], signal_summary_rows)
    print(f"Wrote {signal_summary_path} ({len(signal_summary_rows)} rows)")

    outputs = {
        "candidate_duplicates": {"file": str(duplicate_path), "row_count": len(duplicate_rows)},
        "candidate_conflicting_definitions": {
            "file": str(conflict_path),
            "row_count": len(conflict_rows),
        },
        "candidate_retirement_cleanup": {
            "file": str(retirement_path),
            "row_count": len(retirement_rows),
        },
        "candidate_signal_summary": {
            "file": str(signal_summary_path),
            "row_count": len(signal_summary_rows),
        },
    }

    summary_json = {
        "generated_at": utc_now_iso(),
        "duckdb_file": str(duckdb_path),
        "candidate_counts": {
            "duplicate_rows": len(duplicate_rows),
            "conflicting_definition_rows": len(conflict_rows),
            "retirement_cleanup_rows": len(retirement_rows),
        },
        "signal_summary_count": len(signal_summary_rows),
        "issue_count": len(issues),
        "issues": [{"report_name": i.report_name, "error": i.error} for i in issues],
        "outputs": outputs,
    }

    summary_json_path = reports_dir / "candidate_summary_overview.json"
    summary_json_path.write_text(json.dumps(summary_json, indent=2), encoding="utf-8")
    print(f"Wrote {summary_json_path}")

    summary_outputs_rows = []
    for report_name, output in outputs.items():
        summary_outputs_rows.append((report_name, output["file"], output["row_count"], "ok", ""))

    summary_outputs_path = reports_dir / "candidate_summary_outputs.csv"
    write_csv(
        summary_outputs_path,
        ["report_name", "file", "row_count", "status", "error"],
        summary_outputs_rows,
    )
    print(f"Wrote {summary_outputs_path}")

    con.close()

    if issues:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
