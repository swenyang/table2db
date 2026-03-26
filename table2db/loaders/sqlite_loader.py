"""SQLite loader — the default loader for table2db."""
from __future__ import annotations

import os
import re
import sqlite3
import tempfile
from table2db.models import WorkbookData, ConversionResult, TableInfo, ForeignKey
from .base import BaseLoader

_CTRL_CHARS = re.compile(r'[\x00-\x1f\x7f]')
_SQLITE_TYPE_MAP = {"INTEGER": "INTEGER", "REAL": "REAL", "TEXT": "TEXT", "DATE": "TEXT"}


def _normalize_table_name(name: str) -> str:
    name = re.sub(r'[^\w]+', '_', name).strip('_').lower()
    return name or "table"


def _sanitize(name: str) -> str:
    return _CTRL_CHARS.sub('', name)


def _deduplicate_names(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result = []
    for n in names:
        if n in seen:
            seen[n] += 1
            result.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 0
            result.append(n)
    return result


class SqliteLoader(BaseLoader):
    """Load WorkbookData into a SQLite database."""

    def __init__(self, output_path: str | None = None):
        """
        Args:
            output_path: Optional path for the .db file. If None, creates a temp file.
        """
        self.output_path = output_path

    def load(self, wb: WorkbookData) -> ConversionResult:
        """Create a SQLite database from WorkbookData and return ConversionResult."""
        if self.output_path is not None:
            db_path = self.output_path
            parent = os.path.dirname(db_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        else:
            tmp_dir = tempfile.mkdtemp(prefix="table2db_")
            db_path = os.path.join(tmp_dir, "data.db")

        raw_names = [_normalize_table_name(s.name) for s in wb.sheets]
        table_names = _deduplicate_names(raw_names)

        # Build sheet-name → normalized-table-name mapping for FK resolution
        sheet_to_table = {s.name: t for s, t in zip(wb.sheets, table_names)}

        conn = sqlite3.connect(db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")

            tables: list[TableInfo] = []
            warnings: list[str] = []

            # Collect FK constraints per table (need normalized names)
            fk_clauses: dict[str, list[str]] = {}  # table_name → [FK clause, ...]
            for fk in wb.relationships:
                from_tbl = sheet_to_table.get(fk.from_table, _normalize_table_name(fk.from_table))
                to_tbl = sheet_to_table.get(fk.to_table, _normalize_table_name(fk.to_table))
                clause = (
                    f'FOREIGN KEY ("{_sanitize(fk.from_column)}") '
                    f'REFERENCES "{to_tbl}" ("{_sanitize(fk.to_column)}")'
                )
                fk_clauses.setdefault(from_tbl, []).append(clause)

            # Determine creation order: referenced tables first
            referenced_tables = {sheet_to_table.get(fk.to_table, _normalize_table_name(fk.to_table))
                                 for fk in wb.relationships}
            ordered_pairs = sorted(
                zip(wb.sheets, table_names),
                key=lambda pair: (0 if pair[1] in referenced_tables else 1, pair[1]),
            )

            # Create tables and insert data
            for sheet, tbl_name in ordered_pairs:
                tbl_name = _sanitize(tbl_name)
                cols_sql = []
                col_defs = []
                for h in sheet.headers:
                    safe_h = _sanitize(h)
                    sql_type = _SQLITE_TYPE_MAP.get(
                        sheet.column_types.get(h, "TEXT"), "TEXT"
                    )
                    cols_sql.append(f'"{safe_h}" {sql_type}')
                    col_defs.append({"name": safe_h, "type": sql_type})

                if sheet.primary_key and sheet.primary_key in sheet.headers:
                    pk_safe = _sanitize(sheet.primary_key)
                    cols_sql.append(f'PRIMARY KEY ("{pk_safe}")')

                # Add FK constraints
                for fk_clause in fk_clauses.get(tbl_name, []):
                    cols_sql.append(fk_clause)

                create_sql = (
                    f'CREATE TABLE IF NOT EXISTS "{tbl_name}" ({", ".join(cols_sql)})'
                )
                conn.execute(create_sql)

                # Insert rows
                if sheet.rows:
                    placeholders = ", ".join(["?"] * len(sheet.headers))
                    insert_sql = f'INSERT INTO "{tbl_name}" VALUES ({placeholders})'
                    conn.executemany(insert_sql, [tuple(row) for row in sheet.rows])

                tables.append(TableInfo(
                    name=tbl_name,
                    columns=col_defs,
                    row_count=len(sheet.rows),
                    source_sheet=sheet.name,
                    primary_key=sheet.primary_key,
                    confidence=sheet.metadata.get("island_confidence", 1.0),
                ))

            # Create _meta table
            conn.execute("CREATE TABLE _meta (key TEXT, value TEXT)")
            conn.execute(
                "INSERT INTO _meta VALUES (?, ?)",
                ("source_file", wb.source_file),
            )
            for tbl_info in tables:
                sheet = next(s for s in wb.sheets if s.name == tbl_info.source_sheet)
                conn.execute(
                    "INSERT INTO _meta VALUES (?, ?)",
                    (f"table:{tbl_info.name}:source_sheet", sheet.name),
                )
                conn.execute(
                    "INSERT INTO _meta VALUES (?, ?)",
                    (f"table:{tbl_info.name}:row_count", str(tbl_info.row_count)),
                )
                col_types = ",".join(
                    f"{k}={v}" for k, v in sheet.column_types.items()
                )
                conn.execute(
                    "INSERT INTO _meta VALUES (?, ?)",
                    (f"table:{tbl_info.name}:column_types", col_types),
                )

            # Write FK relationships to _meta
            for fk in wb.relationships:
                from_tbl = sheet_to_table.get(fk.from_table, _normalize_table_name(fk.from_table))
                to_tbl = sheet_to_table.get(fk.to_table, _normalize_table_name(fk.to_table))
                conn.execute(
                    "INSERT INTO _meta VALUES (?, ?)",
                    (f"fk:{from_tbl}.{fk.from_column}->{to_tbl}.{fk.to_column}",
                     f"{fk.confidence:.2f}"),
                )

            conn.commit()
        finally:
            conn.close()

        # Normalize FK table names in relationships for the result
        normalized_rels = []
        for fk in wb.relationships:
            normalized_rels.append(ForeignKey(
                from_table=sheet_to_table.get(fk.from_table, _normalize_table_name(fk.from_table)),
                from_column=fk.from_column,
                to_table=sheet_to_table.get(fk.to_table, _normalize_table_name(fk.to_table)),
                to_column=fk.to_column,
                confidence=fk.confidence,
            ))

        # Build column mappings for sidecar JSON
        column_mappings = []
        for sheet, tbl_name in ordered_pairs:
            tbl_mapping = {
                "table_name": _sanitize(tbl_name),
                "source_sheet": sheet.name,
                "columns": [],
            }
            for i, h in enumerate(sheet.headers):
                orig_col = sheet.original_col_indices[i] if i < len(sheet.original_col_indices) else i
                tbl_mapping["columns"].append({
                    "column_name": _sanitize(h),
                    "source_col": orig_col,
                })
            column_mappings.append(tbl_mapping)

        return ConversionResult(
            db_path=db_path,
            tables=tables,
            relationships=normalized_rels,
            warnings=warnings,
            metadata={
                "source_file": wb.source_file,
                "table_count": len(tables),
                "column_mappings": column_mappings,
            },
        )
