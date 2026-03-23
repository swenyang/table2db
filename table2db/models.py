from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
import os


@dataclass
class ForeignKey:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    confidence: float


@dataclass
class SheetData:
    name: str
    header_row_start: int = 0
    header_row_end: int = 0
    headers: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    column_types: dict[str, str] = field(default_factory=dict)
    primary_key: str | None = None
    excluded_rows: list[int] = field(default_factory=list)
    merge_map: dict[tuple, Any] = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    row_styles: dict[int, dict] = field(default_factory=dict)


@dataclass
class WorkbookData:
    source_file: str
    sheets: list[SheetData] = field(default_factory=list)
    relationships: list[ForeignKey] = field(default_factory=list)


@dataclass
class TableInfo:
    name: str
    columns: list[dict] = field(default_factory=list)
    row_count: int = 0
    source_sheet: str = ""
    primary_key: str | None = None
    confidence: float = 1.0


@dataclass
class ConversionResult:
    db_path: str
    tables: list[TableInfo] = field(default_factory=list)
    relationships: list[ForeignKey] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def cleanup(self):
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        db_dir = os.path.dirname(self.db_path)
        if db_dir and os.path.isdir(db_dir):
            try:
                os.rmdir(db_dir)  # only removes if empty
            except OSError:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cleanup()
