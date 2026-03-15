from __future__ import annotations

import csv
import html
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import dotenv_values


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def write_csv(path: Path, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def rel_path(from_dir: Path, to_file: Path) -> str:
    return str(to_file.relative_to(from_dir))


def table_html(
    columns: list[str],
    rows: list[dict[str, str]],
    *,
    max_rows: int = 15,
) -> str:
    if not columns:
        return "<p class='muted'>No columns available.</p>"
    if not rows:
        return "<p class='muted'>No rows available.</p>"

    sampled = rows[:max_rows]
    lines = ["<div class='table-wrap'><table><thead><tr>"]
    for column in columns:
        lines.append(f"<th>{html.escape(column)}</th>")
    lines.append("</tr></thead><tbody>")

    for row in sampled:
        lines.append("<tr>")
        for column in columns:
            value = row.get(column, "")
            lines.append(f"<td>{html.escape(value)}</td>")
        lines.append("</tr>")

    lines.append("</tbody></table></div>")
    if len(rows) > max_rows:
        lines.append(
            f"<p class='muted'>Showing first {max_rows} of {len(rows)} rows.</p>"
        )
    return "".join(lines)


def derive_overall_assessment(
    modeling_rows: list[dict[str, str]],
    extraction_rows: list[dict[str, str]],
) -> tuple[str, str]:
    has_major_gap = any(
        "major_issues_should_be_explored_before_modeling"
        in row.get("high_level_conclusion", "")
        for row in modeling_rows
    )
    has_caution = any(
        row.get("definability") in {"partially_definable", "not_yet_definable"}
        or bool(row.get("blockers"))
        for row in modeling_rows
    )

    recommended_tables = 0
    for row in extraction_rows:
        if row.get("extraction_category") == "recommended_for_extraction":
            try:
                recommended_tables = int(row.get("table_count", "0"))
            except ValueError:
                recommended_tables = 0
            break

    if has_major_gap:
        return "major_gaps", "One or more modeling objectives show major blockers."
    if has_caution or recommended_tables == 0:
        return (
            "proceed_with_cautions",
            "There is enough signal to continue, but with caution flags to review.",
        )
    return "ready_to_proceed", "Current signals suggest readiness to proceed."


def section_html(
    section_id: str,
    title: str,
    description: str,
    blocks: list[str],
    source_links: list[tuple[str, str]],
) -> str:
    lines = [
        f"<section id='{html.escape(section_id)}'>",
        f"<h2>{html.escape(title)}</h2>",
        f"<p>{html.escape(description)}</p>",
    ]
    lines.extend(blocks)
    if source_links:
        lines.append("<h3>Source files</h3><ul>")
        for label, link in source_links:
            lines.append(
                f"<li><a href='{html.escape(link)}'>{html.escape(label)}</a></li>"
            )
        lines.append("</ul>")
    lines.append("</section>")
    return "".join(lines)


def main() -> int:
    env_values = dotenv_values(".env")
    output_dir_raw = env_values.get("OUTPUT_DIR")
    if not output_dir_raw:
        print("Configuration error: OUTPUT_DIR is missing in .env", file=sys.stderr)
        return 1

    output_dir = Path(output_dir_raw)
    reports_dir = output_dir / "analysis" / "reports"
    report_path = reports_dir / "final_audit_report.html"
    summary_json_path = reports_dir / "final_audit_summary_overview.json"
    summary_csv_path = reports_dir / "final_audit_summary_outputs.csv"

    reports_dir.mkdir(parents=True, exist_ok=True)

    files = {
        "entity_counts": reports_dir / "entity_counts.csv",
        "relationship_coverage": reports_dir / "relationship_coverage.csv",
        "dictionary_table": reports_dir / "dictionary" / "table_dictionary.csv",
        "dictionary_field": reports_dir / "dictionary" / "field_dictionary.csv",
        "completeness_table": reports_dir / "completeness" / "completeness_table_summary.csv",
        "completeness_values": reports_dir / "completeness" / "completeness_field_values_profile.csv",
        "historical_table": reports_dir / "historical" / "historical_table_depth.csv",
        "candidate_duplicates": reports_dir / "candidate_duplicates.csv",
        "candidate_conflicts": reports_dir / "candidate_conflicting_definitions.csv",
        "candidate_retirement": reports_dir / "candidate_retirement_cleanup.csv",
        "extraction_priority": reports_dir / "extraction" / "extraction_table_priority_summary.csv",
        "extraction_tables": reports_dir / "extraction" / "extraction_table_candidates.csv",
        "modeling_objectives": reports_dir / "modeling" / "modeling_objective_adequacy.csv",
        "modeling_redundancy": reports_dir / "modeling" / "modeling_variable_redundancy.csv",
    }

    loaded: dict[str, tuple[list[str], list[dict[str, str]]]] = {
        key: read_csv_rows(path) for key, path in files.items()
    }

    _, modeling_rows = loaded["modeling_objectives"]
    _, extraction_priority_rows = loaded["extraction_priority"]
    overall_label, overall_note = derive_overall_assessment(
        modeling_rows,
        extraction_priority_rows,
    )

    section_index = [
        ("inventory-structure", "Inventory and Structural Overview"),
        ("dictionary", "Data Dictionary"),
        ("completeness", "Completeness"),
        ("historical-depth", "Historical Depth"),
        ("distribution", "Distributions/Frequency Profiling"),
        ("duplicates-conflicts", "Duplicate/Conflict Candidates"),
        ("retirement", "Retirement/Cleanup Candidates"),
        ("extraction", "Extraction Readiness"),
        ("modeling", "Modeling Adequacy"),
        ("conclusion", "Conclusion"),
    ]

    sections: list[str] = []

    entity_cols, entity_rows = loaded["entity_counts"]
    rel_cols, rel_rows = loaded["relationship_coverage"]
    sections.append(
        section_html(
            "inventory-structure",
            "Inventory and Structural Overview",
            "This section summarizes core object counts and relationship coverage.",
            [
                "<h3>Entity counts</h3>",
                table_html(entity_cols, entity_rows, max_rows=20),
                "<h3>Relationship coverage</h3>",
                table_html(rel_cols, rel_rows, max_rows=20),
            ],
            [
                ("entity_counts.csv", rel_path(reports_dir, files["entity_counts"])),
                (
                    "relationship_coverage.csv",
                    rel_path(reports_dir, files["relationship_coverage"]),
                ),
            ],
        )
    )

    table_cols, table_rows = loaded["dictionary_table"]
    field_cols, field_rows = loaded["dictionary_field"]
    sections.append(
        section_html(
            "dictionary",
            "Data Dictionary",
            "Dictionary coverage for tables and fields from the generated metadata dictionary.",
            [
                "<h3>Table dictionary</h3>",
                table_html(table_cols, table_rows, max_rows=20),
                "<h3>Field dictionary</h3>",
                table_html(field_cols, field_rows, max_rows=20),
            ],
            [
                (
                    "table_dictionary.csv",
                    rel_path(reports_dir, files["dictionary_table"]),
                ),
                (
                    "field_dictionary.csv",
                    rel_path(reports_dir, files["dictionary_field"]),
                ),
            ],
        )
    )

    comp_table_cols, comp_table_rows = loaded["completeness_table"]
    sections.append(
        section_html(
            "completeness",
            "Completeness",
            "Completeness profiling summarizes null-ratio and related quality signals.",
            [table_html(comp_table_cols, comp_table_rows, max_rows=20)],
            [
                (
                    "completeness_table_summary.csv",
                    rel_path(reports_dir, files["completeness_table"]),
                )
            ],
        )
    )

    hist_cols, hist_rows = loaded["historical_table"]
    sections.append(
        section_html(
            "historical-depth",
            "Historical Depth",
            "Historical coverage indicates table-level time span and suitability signals.",
            [table_html(hist_cols, hist_rows, max_rows=20)],
            [
                (
                    "historical_table_depth.csv",
                    rel_path(reports_dir, files["historical_table"]),
                )
            ],
        )
    )

    values_cols, values_rows = loaded["completeness_values"]
    sections.append(
        section_html(
            "distribution",
            "Distributions/Frequency Profiling",
            "Observed-value and cardinality signals provide lightweight profiling of field distributions.",
            [table_html(values_cols, values_rows, max_rows=20)],
            [
                (
                    "completeness_field_values_profile.csv",
                    rel_path(reports_dir, files["completeness_values"]),
                )
            ],
        )
    )

    dup_cols, dup_rows = loaded["candidate_duplicates"]
    conflict_cols, conflict_rows = loaded["candidate_conflicts"]
    sections.append(
        section_html(
            "duplicates-conflicts",
            "Duplicate/Conflict Candidates",
            "Potential duplicate and conflicting definitions are surfaced as review flags.",
            [
                "<h3>Potential duplicates</h3>",
                table_html(dup_cols, dup_rows, max_rows=20),
                "<h3>Potential conflicting definitions</h3>",
                table_html(conflict_cols, conflict_rows, max_rows=20),
            ],
            [
                (
                    "candidate_duplicates.csv",
                    rel_path(reports_dir, files["candidate_duplicates"]),
                ),
                (
                    "candidate_conflicting_definitions.csv",
                    rel_path(reports_dir, files["candidate_conflicts"]),
                ),
            ],
        )
    )

    retire_cols, retire_rows = loaded["candidate_retirement"]
    sections.append(
        section_html(
            "retirement",
            "Retirement/Cleanup Candidates",
            "Low-signal and low-usage assets are listed as cleanup candidates for review.",
            [table_html(retire_cols, retire_rows, max_rows=20)],
            [
                (
                    "candidate_retirement_cleanup.csv",
                    rel_path(reports_dir, files["candidate_retirement"]),
                )
            ],
        )
    )

    ext_pri_cols, ext_pri_rows = loaded["extraction_priority"]
    ext_tbl_cols, ext_tbl_rows = loaded["extraction_tables"]
    sections.append(
        section_html(
            "extraction",
            "Extraction Readiness",
            "Extraction readiness synthesizes structural, quality, and usage signals for planning.",
            [
                "<h3>Category summary</h3>",
                table_html(ext_pri_cols, ext_pri_rows, max_rows=20),
                "<h3>Top table candidates</h3>",
                table_html(ext_tbl_cols, ext_tbl_rows, max_rows=20),
            ],
            [
                (
                    "extraction_table_priority_summary.csv",
                    rel_path(reports_dir, files["extraction_priority"]),
                ),
                (
                    "extraction_table_candidates.csv",
                    rel_path(reports_dir, files["extraction_tables"]),
                ),
            ],
        )
    )

    model_obj_cols, model_obj_rows = loaded["modeling_objectives"]
    model_red_cols, model_red_rows = loaded["modeling_redundancy"]
    sections.append(
        section_html(
            "modeling",
            "Modeling Adequacy",
            "Modeling adequacy summarizes objective definability and key variable overlap risks.",
            [
                "<h3>Objective adequacy</h3>",
                table_html(model_obj_cols, model_obj_rows, max_rows=20),
                "<h3>Variable redundancy</h3>",
                table_html(model_red_cols, model_red_rows, max_rows=20),
            ],
            [
                (
                    "modeling_objective_adequacy.csv",
                    rel_path(reports_dir, files["modeling_objectives"]),
                ),
                (
                    "modeling_variable_redundancy.csv",
                    rel_path(reports_dir, files["modeling_redundancy"]),
                ),
            ],
        )
    )

    sections.append(
        section_html(
            "conclusion",
            "Conclusion",
            "Overall synthesis based on extraction readiness and modeling adequacy signals.",
            [
                "<div class='summary-card'>"
                f"<p><strong>overall_assessment_label:</strong> {html.escape(overall_label)}</p>"
                f"<p>{html.escape(overall_note)}</p>"
                "</div>"
            ],
            [],
        )
    )

    toc_items = "".join(
        f"<li><a href='#{html.escape(section_id)}'>{html.escape(title)}</a></li>"
        for section_id, title in section_index
    )
    document = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Metabase Crawler Audit Report</title>
  <style>
    :root {{
      --bg: #f4f5f7;
      --surface: #ffffff;
      --ink: #1f2933;
      --muted: #5f6c7b;
      --line: #d8dee6;
      --accent: #0f5ea8;
    }}
    body {{
      margin: 0;
      padding: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
      line-height: 1.4;
    }}
    main {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 24px;
    }}
    header, section {{
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
      margin-bottom: 16px;
    }}
    h1, h2, h3 {{
      margin: 0 0 12px;
    }}
    h1 {{
      font-size: 1.8rem;
    }}
    h2 {{
      font-size: 1.25rem;
      color: var(--accent);
    }}
    .muted {{
      color: var(--muted);
    }}
    .summary-card {{
      border: 1px solid var(--line);
      border-left: 4px solid var(--accent);
      background: #fbfdff;
      padding: 12px;
      border-radius: 6px;
    }}
    ul {{
      margin: 0;
      padding-left: 20px;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-bottom: 8px;
      font-size: 0.92rem;
    }}
    th, td {{
      border: 1px solid var(--line);
      padding: 6px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: #eef3f8;
    }}
    a {{
      color: var(--accent);
    }}
    @media (max-width: 768px) {{
      main {{
        padding: 12px;
      }}
      th, td {{
        font-size: 0.84rem;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Metabase Crawler Audit Report</h1>
      <p class="muted">Generated at {html.escape(utc_now_iso())}</p>
      <p>This report synthesizes outputs from phases 5-12 into a single global review document.</p>
      <div class="summary-card">
        <p><strong>overall_assessment_label:</strong> {html.escape(overall_label)}</p>
        <p>{html.escape(overall_note)}</p>
      </div>
    </header>
    <section id="toc">
      <h2>Table of Contents</h2>
      <ul>{toc_items}</ul>
    </section>
    {''.join(sections)}
  </main>
</body>
</html>
"""

    report_path.write_text(document, encoding="utf-8")

    outputs = {
        "final_audit_report": {
            "file": str(report_path),
            "row_count": 1,
            "overall_assessment_label": overall_label,
        }
    }
    summary = {
        "generated_at": utc_now_iso(),
        "report_count": len(outputs),
        "successful_report_count": len(outputs),
        "issue_count": 0,
        "issues": [],
        "outputs": outputs,
    }
    summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_csv(
        summary_csv_path,
        ["report_name", "file", "row_count", "status", "error"],
        [("final_audit_report", str(report_path), 1, "ok", "")],
    )

    print(f"Wrote {report_path}")
    print(f"Wrote {summary_json_path}")
    print(f"Wrote {summary_csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
