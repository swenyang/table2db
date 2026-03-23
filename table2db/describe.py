"""Generate a Markdown summary of a ConversionResult database."""
from __future__ import annotations

import sqlite3
from table2db.models import ConversionResult


def generate_db_summary(result: ConversionResult, sample_rows: int = 3) -> str:
    """Return a Markdown summary of the SQLite database in *result*."""
    conn = sqlite3.connect(result.db_path)
    conn.row_factory = sqlite3.Row
    try:
        return _build_summary(conn, result, sample_rows)
    finally:
        conn.close()


def _build_summary(
    conn: sqlite3.Connection,
    result: ConversionResult,
    sample_rows: int,
) -> str:
    source = result.metadata.get("source_file", "unknown")
    lines: list[str] = [
        "# Database Summary",
        "",
        f"**Source:** {source}",
        f"**Tables:** {len(result.tables)}",
        "",
        "---",
    ]

    for table in result.tables:
        tname = table.name
        row_count = table.row_count
        pk = table.primary_key or "None"

        lines.append("")
        lines.append(f"## Table: {tname}")
        lines.append("")
        lines.append(
            f"**Rows:** {row_count} | **Source Sheet:** {table.source_sheet} "
            f"| **Primary Key:** {pk}"
        )

        # --- Columns ---
        lines.append("")
        lines.append("### Columns")
        lines.append("")
        lines.append("| Column | Type |")
        lines.append("|--------|------|")
        for col in table.columns:
            lines.append(f"| {col['name']} | {col['type']} |")

        # --- Sample Data ---
        lines.append("")
        lines.append(f"### Sample Data (first {sample_rows} rows)")
        lines.append("")
        col_names = [c["name"] for c in table.columns]
        lines.append("| " + " | ".join(col_names) + " |")
        lines.append("| " + " | ".join("---" for _ in col_names) + " |")

        quoted_cols = ", ".join(f'"{c}"' for c in col_names)
        cur = conn.execute(
            f'SELECT {quoted_cols} FROM "{tname}" LIMIT ?', (sample_rows,)
        )
        for row in cur:
            cells = [_fmt(row[i]) for i in range(len(col_names))]
            lines.append("| " + " | ".join(cells) + " |")

        # --- Column Statistics ---
        lines.append("")
        lines.append("### Column Statistics")
        lines.append("")
        _append_stats(conn, tname, table, lines, row_count)

    # --- Relationships ---
    if result.relationships:
        lines.append("")
        lines.append("### Relationships")
        lines.append("")
        lines.append("| From | → | To |")
        lines.append("|------|---|----|")
        for fk in result.relationships:
            lines.append(
                f"| {fk.from_table}.{fk.from_column} | → "
                f"| {fk.to_table}.{fk.to_column} |"
            )

    return "\n".join(lines)


def _append_stats(
    conn: sqlite3.Connection,
    tname: str,
    table,
    lines: list[str],
    row_count: int,
) -> None:
    lines.append(
        "| Column | Type | Null % | Min | Max | Avg | Distinct |"
    )
    lines.append(
        "|--------|------|--------|-----|-----|-----|----------|"
    )

    for col in table.columns:
        cname = col["name"]
        ctype = col["type"]

        # Null rate
        null_count = conn.execute(
            f'SELECT COUNT(*) FROM "{tname}" WHERE "{cname}" IS NULL'
        ).fetchone()[0]
        null_pct = (
            f"{null_count / row_count * 100:.0f}%" if row_count else "N/A"
        )

        distinct = conn.execute(
            f'SELECT COUNT(DISTINCT "{cname}") FROM "{tname}"'
        ).fetchone()[0]

        if ctype in ("INTEGER", "REAL"):
            row = conn.execute(
                f'SELECT MIN("{cname}"), MAX("{cname}"), AVG("{cname}") '
                f'FROM "{tname}"'
            ).fetchone()
            mn, mx, avg = (_fmt(row[0]), _fmt(row[1]), _fmt(row[2]))
            lines.append(
                f"| {cname} | {ctype} | {null_pct} | {mn} | {mx} | {avg} | {distinct} |"
            )
        else:
            # Text stats: top 3 values
            top_rows = conn.execute(
                f'SELECT "{cname}", COUNT(*) as cnt FROM "{tname}" '
                f'WHERE "{cname}" IS NOT NULL '
                f'GROUP BY "{cname}" ORDER BY cnt DESC LIMIT 3'
            ).fetchall()
            top_vals = ", ".join(f"{r[0]}({r[1]})" for r in top_rows)
            lines.append(
                f"| {cname} | {ctype} | {null_pct} | - | - | - | {distinct} |"
            )
            if top_vals:
                lines.append(f"|  | Top values: {top_vals} |||||")


def _fmt(value) -> str:
    if value is None:
        return "NULL"
    return str(value)
