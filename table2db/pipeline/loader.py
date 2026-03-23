"""Stage 6: Load WorkbookData into a SQLite database.

This module provides backward-compatible access to the SQLite loader.
For new code, prefer using table2db.loaders.SqliteLoader directly.
"""
from table2db.loaders.sqlite_loader import SqliteLoader


def load_to_sqlite(wb):
    """Load WorkbookData into a SQLite database (convenience wrapper)."""
    return SqliteLoader().load(wb)
