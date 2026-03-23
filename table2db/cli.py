"""Command-line interface for table2db."""
from __future__ import annotations

import argparse
import sys
import os


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="table2db",
        description="Convert Excel files into clean SQLite databases.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # convert command
    convert_parser = subparsers.add_parser(
        "convert", help="Convert an Excel file to a SQLite database."
    )
    convert_parser.add_argument("input", help="Path to the Excel file (.xlsx or .xls)")
    convert_parser.add_argument(
        "-o", "--output", default=None,
        help="Output .db file path (default: <input_name>.db in current directory)"
    )
    convert_parser.add_argument(
        "--summary", action="store_true",
        help="Also generate a Markdown summary file (<output>_summary.md)"
    )
    convert_parser.add_argument(
        "--sample-rows", type=int, default=3,
        help="Number of sample rows in summary (default: 3)"
    )
    convert_parser.add_argument(
        "--type-threshold", type=float, default=0.8,
        help="Type inference majority threshold (default: 0.8)"
    )
    convert_parser.add_argument(
        "--fk-threshold", type=float, default=0.8,
        help="Foreign key confidence threshold (default: 0.8)"
    )

    # describe command
    describe_parser = subparsers.add_parser(
        "describe", help="Generate a Markdown summary of an existing .db file."
    )
    describe_parser.add_argument("db_path", help="Path to the SQLite .db file")
    describe_parser.add_argument(
        "-o", "--output", default=None,
        help="Output .md file path (default: print to stdout)"
    )
    describe_parser.add_argument(
        "--sample-rows", type=int, default=3,
        help="Number of sample rows (default: 3)"
    )

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "convert":
        return _cmd_convert(args)
    elif args.command == "describe":
        return _cmd_describe(args)
    return 1


def _cmd_convert(args) -> int:
    from .converter import TableConverter
    from .loaders import SqliteLoader
    from .describe import generate_db_summary

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        base = os.path.splitext(os.path.basename(args.input))[0]
        output_path = f"{base}.db"

    converter = TableConverter(
        type_threshold=args.type_threshold,
        fk_confidence_threshold=args.fk_threshold,
    )
    loader = SqliteLoader(output_path=output_path)

    try:
        result = converter.convert(args.input, loader=loader)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    table_summary = ", ".join(f"{t.name}({t.row_count} rows)" for t in result.tables)
    print(f"Created {output_path}: {len(result.tables)} tables [{table_summary}]")

    if result.warnings:
        for w in result.warnings:
            print(f"  Warning: {w}")

    if args.summary:
        summary = generate_db_summary(result, sample_rows=args.sample_rows)
        summary_path = output_path.replace(".db", "_summary.md")
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(summary)
        print(f"Summary written to {summary_path}")

    return 0


def _cmd_describe(args) -> int:
    from .models import ConversionResult, TableInfo, ForeignKey
    from .describe import generate_db_summary
    import sqlite3

    if not os.path.exists(args.db_path):
        print(f"Error: File not found: {args.db_path}", file=sys.stderr)
        return 1

    # Build a minimal ConversionResult from the .db file
    conn = sqlite3.connect(args.db_path)
    try:
        tables_raw = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != '_meta'"
        ).fetchall()

        tables = []
        relationships = []
        source_file = args.db_path

        # Try to read metadata
        try:
            meta = dict(conn.execute("SELECT key, value FROM _meta").fetchall())
            source_file = meta.get("source_file", args.db_path)
        except Exception:
            meta = {}

        for (tbl_name,) in tables_raw:
            row_count = conn.execute(f'SELECT COUNT(*) FROM "{tbl_name}"').fetchone()[0]
            cols_info = conn.execute(f'PRAGMA table_info("{tbl_name}")').fetchall()
            columns = [{"name": c[1], "type": c[2] or "TEXT"} for c in cols_info]
            pk_cols = [c[1] for c in cols_info if c[5] > 0]
            source_sheet = meta.get(f"table:{tbl_name}:source_sheet", tbl_name)
            tables.append(TableInfo(
                name=tbl_name,
                columns=columns,
                row_count=row_count,
                source_sheet=source_sheet,
                primary_key=pk_cols[0] if pk_cols else None,
            ))

        # Read FK relationships from _meta
        for key, value in meta.items():
            if key.startswith("fk:"):
                parts = key[3:]  # remove "fk:"
                from_part, to_part = parts.split("->")
                from_tbl, from_col = from_part.rsplit(".", 1)
                to_tbl, to_col = to_part.rsplit(".", 1)
                relationships.append(ForeignKey(
                    from_table=from_tbl, from_column=from_col,
                    to_table=to_tbl, to_column=to_col,
                    confidence=float(value),
                ))
    finally:
        conn.close()

    result = ConversionResult(
        db_path=args.db_path,
        tables=tables,
        relationships=relationships,
        warnings=[],
        metadata={"source_file": source_file},
    )

    summary = generate_db_summary(result, sample_rows=args.sample_rows)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(summary)
        print(f"Summary written to {args.output}")
    else:
        print(summary)

    return 0


if __name__ == "__main__":
    sys.exit(main())
