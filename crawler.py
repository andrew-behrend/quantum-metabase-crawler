from __future__ import annotations

import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


@dataclass(frozen=True)
class EndpointSpec:
    path: str
    raw_filename: str
    metadata_filename: str


@dataclass(frozen=True)
class CrawlIssue:
    scope: str
    target: str
    error: str


@dataclass(frozen=True)
class RequestEvent:
    phase: str
    target: str
    status: str
    attempts: int
    status_code: int | None
    error_kind: str | None
    error_message: str | None


@dataclass(frozen=True)
class CrawlError(Exception):
    kind: str
    message: str
    status_code: int | None = None
    attempts: int = 1

    def __str__(self) -> str:
        return f"{self.kind}: {self.message}"


class ExitCode:
    OK = 0
    CONFIG = 2
    AUTH = 3
    NETWORK = 4
    API = 5
    WRITE = 6
    DATA = 7
    PARTIAL = 11


TOP_LEVEL_ENDPOINTS: tuple[EndpointSpec, ...] = (
    EndpointSpec("/api/database", "databases.json", "databases.meta.json"),
    EndpointSpec("/api/collection", "collections.json", "collections.meta.json"),
    EndpointSpec("/api/dashboard", "dashboards.json", "dashboards.meta.json"),
    EndpointSpec("/api/card", "cards.json", "cards.meta.json"),
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise CrawlError("CONFIG", f"Missing required environment variable: {name}")
    return value


def write_json(path: Path, payload: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as exc:
        raise CrawlError("WRITE", f"Failed writing {path}: {exc}") from exc


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_optional_int_env(name: str, default: int, min_value: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise CrawlError("CONFIG", f"{name} must be an integer, got: {raw}") from exc
    if value < min_value:
        raise CrawlError("CONFIG", f"{name} must be >= {min_value}, got: {value}")
    return value


def parse_optional_float_env(name: str, default: float, min_value: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise CrawlError("CONFIG", f"{name} must be a number, got: {raw}") from exc
    if value < min_value:
        raise CrawlError("CONFIG", f"{name} must be >= {min_value}, got: {value}")
    return value


def authenticate(
    base_url: str,
    username: str,
    password: str,
    auth_timeout_seconds: int,
) -> str:
    auth_url = f"{base_url}/api/session"
    print(f"Authenticating to {auth_url}...")

    try:
        response = requests.post(
            auth_url,
            json={"username": username, "password": password},
            timeout=auth_timeout_seconds,
        )
    except requests.RequestException as exc:
        raise CrawlError("NETWORK", f"Authentication request failed: {exc}") from exc

    if response.status_code != 200:
        raise CrawlError(
            "AUTH",
            "Authentication failed "
            f"(status={response.status_code}, body={response.text[:500]})",
            status_code=response.status_code,
        )

    try:
        body = response.json()
    except ValueError as exc:
        raise CrawlError("AUTH", "Authentication response was not valid JSON") from exc

    token = body.get("id")
    if not token:
        raise CrawlError("AUTH", "Authentication succeeded but no session token was returned")

    print("Authentication successful.")
    return token


def get_json(
    url: str,
    headers: dict[str, str],
    timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
) -> tuple[Any, requests.Response, int]:
    attempts = 0
    last_error: CrawlError | None = None

    for attempt in range(max_retries + 1):
        attempts = attempt + 1
        try:
            response = requests.get(url, headers=headers, timeout=timeout_seconds)
        except requests.RequestException as exc:
            last_error = CrawlError("NETWORK", f"Request failed: {exc}", attempts=attempts)
            if attempt < max_retries:
                time.sleep(backoff_seconds * (2**attempt))
                continue
            raise last_error from exc

        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError as exc:
                raise CrawlError("API", "Response was not valid JSON", attempts=attempts) from exc
            return payload, response, attempts

        if response.status_code == 429 or 500 <= response.status_code <= 599:
            last_error = CrawlError(
                "API",
                f"Transient API response (status={response.status_code}, body={response.text[:500]})",
                status_code=response.status_code,
                attempts=attempts,
            )
            if attempt < max_retries:
                time.sleep(backoff_seconds * (2**attempt))
                continue
            raise last_error

        raise CrawlError(
            "API",
            f"API request failed (status={response.status_code}, body={response.text[:500]})",
            status_code=response.status_code,
            attempts=attempts,
        )

    if last_error:
        raise last_error
    raise CrawlError("UNKNOWN", "Unexpected request failure path")


def extract_entity_list(payload: Any) -> list[dict[str, Any]] | None:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        nested = payload.get("data")
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]

    return None


def fetch_top_level_inventory(
    base_url: str,
    session_token: str,
    raw_dir: Path,
    metadata_dir: Path,
    issues: list[CrawlIssue],
    request_events: list[RequestEvent],
    request_timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
) -> dict[str, Any]:
    headers = {"X-Metabase-Session": session_token}
    payloads: dict[str, Any] = {}

    for endpoint in TOP_LEVEL_ENDPOINTS:
        started_at = utc_now_iso()
        url = f"{base_url}{endpoint.path}"
        print(f"Fetching {endpoint.path}...")

        try:
            payload, response, attempts = get_json(
                url=url,
                headers=headers,
                timeout_seconds=request_timeout_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            request_events.append(
                RequestEvent(
                    phase="top_level",
                    target=endpoint.path,
                    status="success",
                    attempts=attempts,
                    status_code=response.status_code,
                    error_kind=None,
                    error_message=None,
                )
            )
        except CrawlError as exc:
            issues.append(CrawlIssue("top_level", endpoint.path, str(exc)))
            request_events.append(
                RequestEvent(
                    phase="top_level",
                    target=endpoint.path,
                    status="failed",
                    attempts=exc.attempts,
                    status_code=exc.status_code,
                    error_kind=exc.kind,
                    error_message=exc.message,
                )
            )
            print(f"Warning: {endpoint.path} failed: {exc}", file=sys.stderr)
            continue

        raw_path = raw_dir / endpoint.raw_filename
        write_json(raw_path, payload)

        item_count = None
        items = extract_entity_list(payload)
        if items is not None:
            item_count = len(items)

        metadata = {
            "endpoint": endpoint.path,
            "url": url,
            "fetched_at": utc_now_iso(),
            "started_at": started_at,
            "status_code": response.status_code,
            "content_type": response.headers.get("Content-Type"),
            "item_count": item_count,
            "byte_size": len(response.content),
            "raw_file": str(raw_path),
        }
        write_json(metadata_dir / endpoint.metadata_filename, metadata)
        payloads[endpoint.path] = payload

        print(f"Saved {endpoint.raw_filename} and {endpoint.metadata_filename}.")

    return payloads


def is_hidden_or_archived(entity: dict[str, Any]) -> bool:
    if entity.get("archived") is True or entity.get("is_archived") is True:
        return True

    if entity.get("active") is False:
        return True

    visibility_type = entity.get("visibility_type")
    if isinstance(visibility_type, str) and visibility_type in {"hidden", "retired"}:
        return True

    return False


def fetch_phase2_metadata(
    base_url: str,
    session_token: str,
    databases: Any,
    output_dir: Path,
    issues: list[CrawlIssue],
    request_events: list[RequestEvent],
    request_timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
) -> dict[str, int]:
    headers = {"X-Metabase-Session": session_token}

    database_details_dir = output_dir / "raw" / "database_details"
    table_metadata_dir = output_dir / "raw" / "table_metadata"
    field_metadata_dir = output_dir / "raw" / "field_metadata"

    counts = {
        "database_detail_files": 0,
        "table_metadata_files": 0,
        "field_metadata_files": 0,
    }

    database_list = extract_entity_list(databases)
    if database_list is None:
        issues.append(CrawlIssue("phase2", "/api/database", "Expected list of databases"))
        return counts

    for database in database_list:
        database_id = database.get("id")
        if database_id is None:
            issues.append(CrawlIssue("phase2", "database", "Database entry missing id"))
            continue

        path = f"/api/database/{database_id}/metadata"
        url = f"{base_url}{path}"
        print(f"Fetching {path}...")

        try:
            db_payload, _, attempts = get_json(
                url=url,
                headers=headers,
                timeout_seconds=request_timeout_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            request_events.append(
                RequestEvent(
                    phase="phase2",
                    target=path,
                    status="success",
                    attempts=attempts,
                    status_code=200,
                    error_kind=None,
                    error_message=None,
                )
            )
        except CrawlError as exc:
            issues.append(CrawlIssue("phase2", path, str(exc)))
            request_events.append(
                RequestEvent(
                    phase="phase2",
                    target=path,
                    status="failed",
                    attempts=exc.attempts,
                    status_code=exc.status_code,
                    error_kind=exc.kind,
                    error_message=exc.message,
                )
            )
            print(f"Warning: {path} failed: {exc}", file=sys.stderr)
            continue

        write_json(database_details_dir / f"{database_id}.json", db_payload)
        counts["database_detail_files"] += 1

        tables = db_payload.get("tables") if isinstance(db_payload, dict) else None
        if not isinstance(tables, list):
            continue

        for table in tables:
            if not isinstance(table, dict):
                continue

            if is_hidden_or_archived(table):
                continue

            table_id = table.get("id")
            if table_id is None:
                issues.append(
                    CrawlIssue("phase2", path, "Table entry missing id in database metadata")
                )
                continue

            write_json(table_metadata_dir / f"{table_id}.json", table)
            counts["table_metadata_files"] += 1

            fields = table.get("fields")
            if not isinstance(fields, list):
                fields = []

            visible_fields = [
                field
                for field in fields
                if isinstance(field, dict) and not is_hidden_or_archived(field)
            ]

            write_json(field_metadata_dir / f"{table_id}.json", visible_fields)
            counts["field_metadata_files"] += 1

    return counts


def normalize_id(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (str, int)):
        return str(value)
    return None


def extract_field_ids_from_result_metadata(card_payload: dict[str, Any]) -> list[int]:
    field_ids: list[int] = []
    result_metadata = card_payload.get("result_metadata")

    if not isinstance(result_metadata, list):
        return field_ids

    for column in result_metadata:
        if not isinstance(column, dict):
            continue
        field_id = column.get("id")
        if isinstance(field_id, int):
            field_ids.append(field_id)

    return sorted(set(field_ids))


def fetch_phase3_analytical_metadata(
    base_url: str,
    session_token: str,
    cards: Any,
    dashboards: Any,
    collections: Any,
    output_dir: Path,
    issues: list[CrawlIssue],
    request_events: list[RequestEvent],
    request_timeout_seconds: int,
    max_retries: int,
    backoff_seconds: float,
) -> dict[str, int]:
    headers = {"X-Metabase-Session": session_token}

    card_details_dir = output_dir / "raw" / "card_details"
    dashboard_details_dir = output_dir / "raw" / "dashboard_details"
    collection_details_dir = output_dir / "raw" / "collection_details"
    collection_items_dir = output_dir / "raw" / "collection_items"
    relationships_dir = output_dir / "raw" / "relationships"

    dashboard_to_cards: list[dict[str, Any]] = []
    collection_to_contents: list[dict[str, Any]] = []
    card_to_data_model: list[dict[str, Any]] = []

    counts = {
        "card_detail_files": 0,
        "dashboard_detail_files": 0,
        "collection_detail_files": 0,
        "collection_items_files": 0,
        "dashboard_to_cards_rows": 0,
        "collection_to_contents_rows": 0,
        "card_to_data_model_rows": 0,
    }

    card_list = extract_entity_list(cards)
    dashboard_list = extract_entity_list(dashboards)
    collection_list = extract_entity_list(collections)

    if card_list is None:
        issues.append(CrawlIssue("phase3", "/api/card", "Expected list of cards"))
        card_list = []

    if dashboard_list is None:
        issues.append(CrawlIssue("phase3", "/api/dashboard", "Expected list of dashboards"))
        dashboard_list = []

    if collection_list is None:
        issues.append(CrawlIssue("phase3", "/api/collection", "Expected list of collections"))
        collection_list = []

    for card in card_list:
        if is_hidden_or_archived(card):
            continue

        card_id = normalize_id(card.get("id"))
        if card_id is None:
            issues.append(CrawlIssue("phase3", "card", "Card entry missing id"))
            continue

        path = f"/api/card/{card_id}"
        print(f"Fetching {path}...")

        try:
            card_payload, _, attempts = get_json(
                url=f"{base_url}{path}",
                headers=headers,
                timeout_seconds=request_timeout_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            request_events.append(
                RequestEvent(
                    phase="phase3",
                    target=path,
                    status="success",
                    attempts=attempts,
                    status_code=200,
                    error_kind=None,
                    error_message=None,
                )
            )
        except CrawlError as exc:
            issues.append(CrawlIssue("phase3", path, str(exc)))
            request_events.append(
                RequestEvent(
                    phase="phase3",
                    target=path,
                    status="failed",
                    attempts=exc.attempts,
                    status_code=exc.status_code,
                    error_kind=exc.kind,
                    error_message=exc.message,
                )
            )
            print(f"Warning: {path} failed: {exc}", file=sys.stderr)
            continue

        if isinstance(card_payload, dict) and is_hidden_or_archived(card_payload):
            continue

        write_json(card_details_dir / f"{card_id}.json", card_payload)
        counts["card_detail_files"] += 1

        if isinstance(card_payload, dict):
            relation = {
                "card_id": card_payload.get("id", card.get("id")),
                "database_id": card_payload.get("database_id"),
                "table_id": card_payload.get("table_id"),
                "field_ids": extract_field_ids_from_result_metadata(card_payload),
            }
            card_to_data_model.append(relation)

    for dashboard in dashboard_list:
        if is_hidden_or_archived(dashboard):
            continue

        dashboard_id = normalize_id(dashboard.get("id"))
        if dashboard_id is None:
            issues.append(CrawlIssue("phase3", "dashboard", "Dashboard entry missing id"))
            continue

        path = f"/api/dashboard/{dashboard_id}"
        print(f"Fetching {path}...")

        try:
            dashboard_payload, _, attempts = get_json(
                url=f"{base_url}{path}",
                headers=headers,
                timeout_seconds=request_timeout_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            request_events.append(
                RequestEvent(
                    phase="phase3",
                    target=path,
                    status="success",
                    attempts=attempts,
                    status_code=200,
                    error_kind=None,
                    error_message=None,
                )
            )
        except CrawlError as exc:
            issues.append(CrawlIssue("phase3", path, str(exc)))
            request_events.append(
                RequestEvent(
                    phase="phase3",
                    target=path,
                    status="failed",
                    attempts=exc.attempts,
                    status_code=exc.status_code,
                    error_kind=exc.kind,
                    error_message=exc.message,
                )
            )
            print(f"Warning: {path} failed: {exc}", file=sys.stderr)
            continue

        if isinstance(dashboard_payload, dict) and is_hidden_or_archived(dashboard_payload):
            continue

        write_json(dashboard_details_dir / f"{dashboard_id}.json", dashboard_payload)
        counts["dashboard_detail_files"] += 1

        if isinstance(dashboard_payload, dict):
            dashcards = dashboard_payload.get("dashcards")
            if isinstance(dashcards, list):
                for dashcard in dashcards:
                    if not isinstance(dashcard, dict):
                        continue

                    card_id = dashcard.get("card_id")
                    if card_id is None and isinstance(dashcard.get("card"), dict):
                        card_id = dashcard["card"].get("id")

                    if card_id is None:
                        continue

                    dashboard_to_cards.append(
                        {
                            "dashboard_id": dashboard_payload.get("id", dashboard.get("id")),
                            "card_id": card_id,
                        }
                    )

    for collection in collection_list:
        if is_hidden_or_archived(collection):
            continue

        collection_id = normalize_id(collection.get("id"))
        if collection_id is None:
            issues.append(CrawlIssue("phase3", "collection", "Collection entry missing id"))
            continue

        detail_path = f"/api/collection/{collection_id}"
        items_path = f"/api/collection/{collection_id}/items"

        print(f"Fetching {detail_path}...")
        try:
            collection_payload, _, attempts = get_json(
                url=f"{base_url}{detail_path}",
                headers=headers,
                timeout_seconds=request_timeout_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            request_events.append(
                RequestEvent(
                    phase="phase3",
                    target=detail_path,
                    status="success",
                    attempts=attempts,
                    status_code=200,
                    error_kind=None,
                    error_message=None,
                )
            )
        except CrawlError as exc:
            issues.append(CrawlIssue("phase3", detail_path, str(exc)))
            request_events.append(
                RequestEvent(
                    phase="phase3",
                    target=detail_path,
                    status="failed",
                    attempts=exc.attempts,
                    status_code=exc.status_code,
                    error_kind=exc.kind,
                    error_message=exc.message,
                )
            )
            print(f"Warning: {detail_path} failed: {exc}", file=sys.stderr)
            continue

        if isinstance(collection_payload, dict) and is_hidden_or_archived(collection_payload):
            continue

        write_json(collection_details_dir / f"{collection_id}.json", collection_payload)
        counts["collection_detail_files"] += 1

        print(f"Fetching {items_path}...")
        try:
            items_payload, _, attempts = get_json(
                url=f"{base_url}{items_path}",
                headers=headers,
                timeout_seconds=request_timeout_seconds,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
            )
            request_events.append(
                RequestEvent(
                    phase="phase3",
                    target=items_path,
                    status="success",
                    attempts=attempts,
                    status_code=200,
                    error_kind=None,
                    error_message=None,
                )
            )
        except CrawlError as exc:
            issues.append(CrawlIssue("phase3", items_path, str(exc)))
            request_events.append(
                RequestEvent(
                    phase="phase3",
                    target=items_path,
                    status="failed",
                    attempts=exc.attempts,
                    status_code=exc.status_code,
                    error_kind=exc.kind,
                    error_message=exc.message,
                )
            )
            print(f"Warning: {items_path} failed: {exc}", file=sys.stderr)
            continue

        write_json(collection_items_dir / f"{collection_id}.json", items_payload)
        counts["collection_items_files"] += 1

        items = extract_entity_list(items_payload)
        if items is None:
            continue

        for item in items:
            if is_hidden_or_archived(item):
                continue

            item_id = item.get("id")
            item_type = item.get("model") or item.get("type") or item.get("collection_type")
            if item_id is None or item_type is None:
                continue

            collection_to_contents.append(
                {
                    "collection_id": collection_payload.get("id", collection.get("id")),
                    "item_id": item_id,
                    "item_type": item_type,
                }
            )

    write_json(relationships_dir / "dashboard_to_cards.json", dashboard_to_cards)
    write_json(relationships_dir / "collection_to_contents.json", collection_to_contents)
    write_json(relationships_dir / "card_to_data_model.json", card_to_data_model)

    counts["dashboard_to_cards_rows"] = len(dashboard_to_cards)
    counts["collection_to_contents_rows"] = len(collection_to_contents)
    counts["card_to_data_model_rows"] = len(card_to_data_model)

    return counts


def _to_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _json_text(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _read_json_or_issue(path: Path, issues: list[CrawlIssue], scope: str) -> Any | None:
    try:
        return read_json(path)
    except Exception as exc:  # noqa: BLE001
        issues.append(CrawlIssue(scope, str(path), f"Failed to read JSON: {exc}"))
        print(f"Warning: failed to read {path}: {exc}", file=sys.stderr)
        return None


def ingest_phase4_duckdb(output_dir: Path, issues: list[CrawlIssue]) -> dict[str, int]:
    counts = {
        "duckdb_tables_created": 0,
        "duckdb_databases_rows": 0,
        "duckdb_tables_rows": 0,
        "duckdb_fields_rows": 0,
        "duckdb_cards_rows": 0,
        "duckdb_dashboards_rows": 0,
        "duckdb_collections_rows": 0,
        "duckdb_rel_dashboard_to_cards_rows": 0,
        "duckdb_rel_collection_to_contents_rows": 0,
        "duckdb_rel_card_to_data_model_rows": 0,
        "duckdb_rel_card_to_fields_rows": 0,
    }

    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        issues.append(CrawlIssue("phase4", "duckdb", f"DuckDB import failed: {exc}"))
        print(f"Warning: DuckDB import failed: {exc}", file=sys.stderr)
        return counts

    raw_dir = output_dir / "raw"
    analysis_dir = output_dir / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    db_path = analysis_dir / "metabase.duckdb"

    ingested_at = utc_now_iso()

    databases_rows: list[tuple[Any, ...]] = []
    tables_rows: list[tuple[Any, ...]] = []
    fields_rows: list[tuple[Any, ...]] = []
    cards_rows: list[tuple[Any, ...]] = []
    dashboards_rows: list[tuple[Any, ...]] = []
    collections_rows: list[tuple[Any, ...]] = []
    rel_dashboard_to_cards_rows: list[tuple[Any, ...]] = []
    rel_collection_to_contents_rows: list[tuple[Any, ...]] = []
    rel_card_to_data_model_rows: list[tuple[Any, ...]] = []
    rel_card_to_fields_rows: list[tuple[Any, ...]] = []

    databases_payload = _read_json_or_issue(raw_dir / "databases.json", issues, "phase4")
    database_list = extract_entity_list(databases_payload) if databases_payload is not None else None
    if database_list is None:
        issues.append(CrawlIssue("phase4", str(raw_dir / "databases.json"), "Expected list of databases"))
        database_list = []

    for database in database_list:
        databases_rows.append(
            (
                _to_int(database.get("id")),
                database.get("name"),
                database.get("engine"),
                database.get("description"),
                database.get("initial_sync_status"),
                database.get("is_sample"),
                database.get("is_audit"),
                database.get("created_at"),
                database.get("updated_at"),
                str(raw_dir / "databases.json"),
                ingested_at,
            )
        )

    for table_file in sorted((raw_dir / "table_metadata").glob("*.json")):
        payload = _read_json_or_issue(table_file, issues, "phase4")
        if not isinstance(payload, dict):
            continue

        tables_rows.append(
            (
                _to_int(payload.get("id")),
                _to_int(payload.get("db_id")),
                payload.get("schema"),
                payload.get("name"),
                payload.get("display_name"),
                payload.get("entity_type"),
                payload.get("description"),
                payload.get("active"),
                payload.get("visibility_type"),
                payload.get("created_at"),
                payload.get("updated_at"),
                str(table_file),
                ingested_at,
            )
        )

    for field_file in sorted((raw_dir / "field_metadata").glob("*.json")):
        payload = _read_json_or_issue(field_file, issues, "phase4")
        if not isinstance(payload, list):
            continue

        table_id_from_file = _to_int(field_file.stem)
        for field in payload:
            if not isinstance(field, dict):
                continue

            fingerprint = field.get("fingerprint")
            fingerprint_json = _json_text(fingerprint) if isinstance(fingerprint, dict) else None
            fingerprint_global = fingerprint.get("global") if isinstance(fingerprint, dict) else None
            fingerprint_distinct_count = (
                float(fingerprint_global.get("distinct-count"))
                if isinstance(fingerprint_global, dict)
                and isinstance(fingerprint_global.get("distinct-count"), (int, float))
                else None
            )
            fingerprint_nil_count = (
                float(fingerprint_global.get("nil-count"))
                if isinstance(fingerprint_global, dict)
                and isinstance(fingerprint_global.get("nil-count"), (int, float))
                else None
            )
            fingerprint_nil_pct = (
                float(fingerprint_global.get("nil%"))
                if isinstance(fingerprint_global, dict)
                and isinstance(fingerprint_global.get("nil%"), (int, float))
                else None
            )

            fields_rows.append(
                (
                    _to_int(field.get("id")),
                    _to_int(field.get("table_id")) or table_id_from_file,
                    _to_int(field.get("fk_target_field_id")),
                    field.get("name"),
                    field.get("display_name"),
                    field.get("base_type"),
                    field.get("effective_type"),
                    field.get("semantic_type"),
                    field.get("description"),
                    field.get("active"),
                    field.get("visibility_type"),
                    field.get("has_field_values"),
                    fingerprint_distinct_count,
                    fingerprint_nil_count,
                    fingerprint_nil_pct,
                    fingerprint_json,
                    field.get("created_at"),
                    field.get("updated_at"),
                    str(field_file),
                    ingested_at,
                )
            )

    for card_file in sorted((raw_dir / "card_details").glob("*.json")):
        payload = _read_json_or_issue(card_file, issues, "phase4")
        if not isinstance(payload, dict):
            continue

        cards_rows.append(
            (
                _to_int(payload.get("id")),
                payload.get("entity_id"),
                payload.get("name"),
                payload.get("description"),
                payload.get("type"),
                payload.get("query_type"),
                _to_int(payload.get("database_id")),
                _to_int(payload.get("table_id")),
                normalize_id(payload.get("collection_id")),
                _to_int(payload.get("dashboard_id")),
                payload.get("archived"),
                payload.get("created_at"),
                payload.get("updated_at"),
                str(card_file),
                ingested_at,
            )
        )

    for dashboard_file in sorted((raw_dir / "dashboard_details").glob("*.json")):
        payload = _read_json_or_issue(dashboard_file, issues, "phase4")
        if not isinstance(payload, dict):
            continue

        dashcards = payload.get("dashcards")
        dashcard_count = len(dashcards) if isinstance(dashcards, list) else None

        dashboards_rows.append(
            (
                _to_int(payload.get("id")),
                payload.get("entity_id"),
                payload.get("name"),
                payload.get("description"),
                normalize_id(payload.get("collection_id")),
                payload.get("archived"),
                dashcard_count,
                payload.get("created_at"),
                payload.get("updated_at"),
                str(dashboard_file),
                ingested_at,
            )
        )

    for collection_file in sorted((raw_dir / "collection_details").glob("*.json")):
        payload = _read_json_or_issue(collection_file, issues, "phase4")
        if not isinstance(payload, dict):
            continue

        collections_rows.append(
            (
                normalize_id(payload.get("id")),
                payload.get("entity_id"),
                payload.get("name"),
                payload.get("description"),
                normalize_id(payload.get("parent_id")),
                payload.get("location"),
                payload.get("is_personal"),
                payload.get("archived"),
                payload.get("created_at"),
                str(collection_file),
                ingested_at,
            )
        )

    dash_to_cards_payload = _read_json_or_issue(
        raw_dir / "relationships" / "dashboard_to_cards.json", issues, "phase4"
    )
    if isinstance(dash_to_cards_payload, list):
        for row in dash_to_cards_payload:
            if not isinstance(row, dict):
                continue
            rel_dashboard_to_cards_rows.append(
                (
                    _to_int(row.get("dashboard_id")),
                    _to_int(row.get("card_id")),
                    str(raw_dir / "relationships" / "dashboard_to_cards.json"),
                    ingested_at,
                )
            )

    coll_to_contents_payload = _read_json_or_issue(
        raw_dir / "relationships" / "collection_to_contents.json", issues, "phase4"
    )
    if isinstance(coll_to_contents_payload, list):
        for row in coll_to_contents_payload:
            if not isinstance(row, dict):
                continue
            rel_collection_to_contents_rows.append(
                (
                    normalize_id(row.get("collection_id")),
                    normalize_id(row.get("item_id")),
                    row.get("item_type"),
                    str(raw_dir / "relationships" / "collection_to_contents.json"),
                    ingested_at,
                )
            )

    card_to_model_payload = _read_json_or_issue(
        raw_dir / "relationships" / "card_to_data_model.json", issues, "phase4"
    )
    if isinstance(card_to_model_payload, list):
        for row in card_to_model_payload:
            if not isinstance(row, dict):
                continue

            card_id = _to_int(row.get("card_id"))
            database_id = _to_int(row.get("database_id"))
            table_id = _to_int(row.get("table_id"))
            field_ids = row.get("field_ids")
            if not isinstance(field_ids, list):
                field_ids = []

            rel_card_to_data_model_rows.append(
                (
                    card_id,
                    database_id,
                    table_id,
                    _json_text(field_ids),
                    str(raw_dir / "relationships" / "card_to_data_model.json"),
                    ingested_at,
                )
            )

            for field_id in field_ids:
                field_id_int = _to_int(field_id)
                if field_id_int is None or card_id is None:
                    continue
                rel_card_to_fields_rows.append(
                    (
                        card_id,
                        field_id_int,
                        str(raw_dir / "relationships" / "card_to_data_model.json"),
                        ingested_at,
                    )
                )

    con = duckdb.connect(str(db_path))
    try:
        con.execute("DROP TABLE IF EXISTS databases")
        con.execute("DROP TABLE IF EXISTS tables")
        con.execute("DROP TABLE IF EXISTS fields")
        con.execute("DROP TABLE IF EXISTS cards")
        con.execute("DROP TABLE IF EXISTS dashboards")
        con.execute("DROP TABLE IF EXISTS collections")
        con.execute("DROP TABLE IF EXISTS rel_dashboard_to_cards")
        con.execute("DROP TABLE IF EXISTS rel_collection_to_contents")
        con.execute("DROP TABLE IF EXISTS rel_card_to_data_model")
        con.execute("DROP TABLE IF EXISTS rel_card_to_fields")

        con.execute(
            """
            CREATE TABLE databases (
                database_id BIGINT,
                name VARCHAR,
                engine VARCHAR,
                description VARCHAR,
                initial_sync_status VARCHAR,
                is_sample BOOLEAN,
                is_audit BOOLEAN,
                created_at VARCHAR,
                updated_at VARCHAR,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE tables (
                table_id BIGINT,
                database_id BIGINT,
                schema_name VARCHAR,
                name VARCHAR,
                display_name VARCHAR,
                entity_type VARCHAR,
                description VARCHAR,
                active BOOLEAN,
                visibility_type VARCHAR,
                created_at VARCHAR,
                updated_at VARCHAR,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE fields (
                field_id BIGINT,
                table_id BIGINT,
                fk_target_field_id BIGINT,
                name VARCHAR,
                display_name VARCHAR,
                base_type VARCHAR,
                effective_type VARCHAR,
                semantic_type VARCHAR,
                description VARCHAR,
                active BOOLEAN,
                visibility_type VARCHAR,
                has_field_values VARCHAR,
                fingerprint_distinct_count DOUBLE,
                fingerprint_nil_count DOUBLE,
                fingerprint_nil_pct DOUBLE,
                fingerprint_json VARCHAR,
                created_at VARCHAR,
                updated_at VARCHAR,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE cards (
                card_id BIGINT,
                entity_id VARCHAR,
                name VARCHAR,
                description VARCHAR,
                card_type VARCHAR,
                query_type VARCHAR,
                database_id BIGINT,
                table_id BIGINT,
                collection_id VARCHAR,
                dashboard_id BIGINT,
                archived BOOLEAN,
                created_at VARCHAR,
                updated_at VARCHAR,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE dashboards (
                dashboard_id BIGINT,
                entity_id VARCHAR,
                name VARCHAR,
                description VARCHAR,
                collection_id VARCHAR,
                archived BOOLEAN,
                dashcard_count BIGINT,
                created_at VARCHAR,
                updated_at VARCHAR,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE collections (
                collection_id VARCHAR,
                entity_id VARCHAR,
                name VARCHAR,
                description VARCHAR,
                parent_id VARCHAR,
                location VARCHAR,
                is_personal BOOLEAN,
                archived BOOLEAN,
                created_at VARCHAR,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE rel_dashboard_to_cards (
                dashboard_id BIGINT,
                card_id BIGINT,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE rel_collection_to_contents (
                collection_id VARCHAR,
                item_id VARCHAR,
                item_type VARCHAR,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE rel_card_to_data_model (
                card_id BIGINT,
                database_id BIGINT,
                table_id BIGINT,
                field_ids_json VARCHAR,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )
        con.execute(
            """
            CREATE TABLE rel_card_to_fields (
                card_id BIGINT,
                field_id BIGINT,
                source_file VARCHAR,
                ingested_at VARCHAR
            )
            """
        )

        counts["duckdb_tables_created"] = 10

        if databases_rows:
            con.executemany(
                "INSERT INTO databases VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                databases_rows,
            )
        if tables_rows:
            con.executemany(
                "INSERT INTO tables VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tables_rows,
            )
        if fields_rows:
            con.executemany(
                "INSERT INTO fields VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                fields_rows,
            )
        if cards_rows:
            con.executemany(
                "INSERT INTO cards VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                cards_rows,
            )
        if dashboards_rows:
            con.executemany(
                "INSERT INTO dashboards VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                dashboards_rows,
            )
        if collections_rows:
            con.executemany(
                "INSERT INTO collections VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                collections_rows,
            )
        if rel_dashboard_to_cards_rows:
            con.executemany(
                "INSERT INTO rel_dashboard_to_cards VALUES (?, ?, ?, ?)",
                rel_dashboard_to_cards_rows,
            )
        if rel_collection_to_contents_rows:
            con.executemany(
                "INSERT INTO rel_collection_to_contents VALUES (?, ?, ?, ?, ?)",
                rel_collection_to_contents_rows,
            )
        if rel_card_to_data_model_rows:
            con.executemany(
                "INSERT INTO rel_card_to_data_model VALUES (?, ?, ?, ?, ?, ?)",
                rel_card_to_data_model_rows,
            )
        if rel_card_to_fields_rows:
            con.executemany(
                "INSERT INTO rel_card_to_fields VALUES (?, ?, ?, ?)",
                rel_card_to_fields_rows,
            )

    except Exception as exc:  # noqa: BLE001
        issues.append(CrawlIssue("phase4", str(db_path), f"DuckDB ingestion failed: {exc}"))
        print(f"Warning: DuckDB ingestion failed: {exc}", file=sys.stderr)
    finally:
        con.close()

    counts["duckdb_databases_rows"] = len(databases_rows)
    counts["duckdb_tables_rows"] = len(tables_rows)
    counts["duckdb_fields_rows"] = len(fields_rows)
    counts["duckdb_cards_rows"] = len(cards_rows)
    counts["duckdb_dashboards_rows"] = len(dashboards_rows)
    counts["duckdb_collections_rows"] = len(collections_rows)
    counts["duckdb_rel_dashboard_to_cards_rows"] = len(rel_dashboard_to_cards_rows)
    counts["duckdb_rel_collection_to_contents_rows"] = len(rel_collection_to_contents_rows)
    counts["duckdb_rel_card_to_data_model_rows"] = len(rel_card_to_data_model_rows)
    counts["duckdb_rel_card_to_fields_rows"] = len(rel_card_to_fields_rows)

    return counts


def write_run_report(
    metadata_dir: Path,
    issues: list[CrawlIssue],
    counts: dict[str, int],
    run_id: str,
    started_at: str,
    finished_at: str,
    request_events: list[RequestEvent],
) -> None:
    duration_seconds = (
        datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)
    ).total_seconds()
    report = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration_seconds,
        "generated_at": utc_now_iso(),
        "error_count": len(issues),
        "errors": [
            {"scope": issue.scope, "target": issue.target, "error": issue.error}
            for issue in issues
        ],
        "request_status_table": [
            {
                "phase": event.phase,
                "target": event.target,
                "status": event.status,
                "attempts": event.attempts,
                "status_code": event.status_code,
                "error_kind": event.error_kind,
                "error_message": event.error_message,
            }
            for event in request_events
        ],
        "counts": counts,
    }
    write_json(metadata_dir / "crawl-report.json", report)


def main() -> int:
    load_dotenv()
    run_id = str(uuid.uuid4())
    started_at = utc_now_iso()

    try:
        base_url = require_env("METABASE_BASE_URL").rstrip("/")
        username = require_env("METABASE_USERNAME")
        password = require_env("METABASE_PASSWORD")
        output_dir = Path(require_env("OUTPUT_DIR"))
        request_timeout_seconds = parse_optional_int_env(
            "METABASE_REQUEST_TIMEOUT_SECONDS", default=60, min_value=1
        )
        auth_timeout_seconds = parse_optional_int_env(
            "METABASE_AUTH_TIMEOUT_SECONDS", default=30, min_value=1
        )
        max_retries = parse_optional_int_env("METABASE_MAX_RETRIES", default=2, min_value=0)
        backoff_seconds = parse_optional_float_env(
            "METABASE_BACKOFF_SECONDS", default=1.0, min_value=0.0
        )
    except CrawlError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return ExitCode.CONFIG

    raw_dir = output_dir / "raw"
    metadata_dir = output_dir / "metadata"
    try:
        raw_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"Filesystem error: {exc}", file=sys.stderr)
        return ExitCode.WRITE

    try:
        session_token = authenticate(
            base_url=base_url,
            username=username,
            password=password,
            auth_timeout_seconds=auth_timeout_seconds,
        )
    except CrawlError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if exc.kind == "AUTH":
            return ExitCode.AUTH
        if exc.kind == "NETWORK":
            return ExitCode.NETWORK
        return ExitCode.API

    issues: list[CrawlIssue] = []
    request_events: list[RequestEvent] = []

    top_level_payloads = fetch_top_level_inventory(
        base_url=base_url,
        session_token=session_token,
        raw_dir=raw_dir,
        metadata_dir=metadata_dir,
        issues=issues,
        request_events=request_events,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )

    phase2_counts = fetch_phase2_metadata(
        base_url=base_url,
        session_token=session_token,
        databases=top_level_payloads.get("/api/database"),
        output_dir=output_dir,
        issues=issues,
        request_events=request_events,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )

    phase3_counts = fetch_phase3_analytical_metadata(
        base_url=base_url,
        session_token=session_token,
        cards=top_level_payloads.get("/api/card"),
        dashboards=top_level_payloads.get("/api/dashboard"),
        collections=top_level_payloads.get("/api/collection"),
        output_dir=output_dir,
        issues=issues,
        request_events=request_events,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )

    phase4_counts = ingest_phase4_duckdb(output_dir=output_dir, issues=issues)

    total_counts = {
        "run_id": run_id,
        "top_level_success": len(top_level_payloads),
        **phase2_counts,
        **phase3_counts,
        **phase4_counts,
    }
    finished_at = utc_now_iso()
    write_run_report(
        metadata_dir=metadata_dir,
        issues=issues,
        counts=total_counts,
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        request_events=request_events,
    )

    if issues:
        print(f"Crawl complete with {len(issues)} issue(s).", file=sys.stderr)
        print(f"See {metadata_dir / 'crawl-report.json'} for details.", file=sys.stderr)
        error_kinds = {
            issue.error.split(":", 1)[0].strip() for issue in issues if ":" in issue.error
        }
        if "WRITE" in error_kinds:
            return ExitCode.WRITE
        if "NETWORK" in error_kinds:
            return ExitCode.NETWORK
        if "API" in error_kinds:
            return ExitCode.API
        return ExitCode.PARTIAL

    print("Crawl complete.")
    return ExitCode.OK


if __name__ == "__main__":
    raise SystemExit(main())
