import sqlite3
import pytest
from table2db.models import SheetData, WorkbookData, ForeignKey
from table2db.pipeline.loader import load_to_sqlite


def _make_sheet(name, headers, rows, column_types=None, primary_key=None):
    s = SheetData(name=name, headers=headers, rows=rows)
    s.column_types = column_types or {h: "TEXT" for h in headers}
    s.primary_key = primary_key
    return s


def _make_wb(sheets, relationships=None):
    wb = WorkbookData(source_file="test.xlsx", sheets=sheets)
    wb.relationships = relationships or []
    return wb


class TestLoader:
    def test_simple_table(self):
        sheet = _make_sheet("Orders", ["id", "name"], [
            [1, "Alice"], [2, "Bob"]
        ], {"id": "INTEGER", "name": "TEXT"})
        result = load_to_sqlite(_make_wb([sheet]))
        try:
            conn = sqlite3.connect(result.db_path)
            rows = conn.execute("SELECT * FROM orders").fetchall()
            conn.close()
            assert len(rows) == 2
            assert rows[0] == (1, "Alice")
        finally:
            result.cleanup()

    def test_table_name_normalization(self):
        sheet = _make_sheet("Sales Data", ["id"], [[1]], {"id": "INTEGER"})
        result = load_to_sqlite(_make_wb([sheet]))
        try:
            conn = sqlite3.connect(result.db_path)
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name != '_meta'"
            ).fetchall()]
            conn.close()
            assert "sales_data" in tables
        finally:
            result.cleanup()

    def test_duplicate_table_names(self):
        s1 = _make_sheet("Sales Data", ["id"], [[1]], {"id": "INTEGER"})
        s2 = _make_sheet("Sales-Data", ["id"], [[2]], {"id": "INTEGER"})
        result = load_to_sqlite(_make_wb([s1, s2]))
        try:
            conn = sqlite3.connect(result.db_path)
            tables = sorted([r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name != '_meta'"
            ).fetchall()])
            conn.close()
            assert len(tables) == 2
            assert tables[0] != tables[1]
        finally:
            result.cleanup()

    def test_primary_key(self):
        sheet = _make_sheet("T", ["id", "val"], [
            [1, "a"], [2, "b"]
        ], {"id": "INTEGER", "val": "TEXT"}, primary_key="id")
        result = load_to_sqlite(_make_wb([sheet]))
        try:
            conn = sqlite3.connect(result.db_path)
            info = conn.execute("PRAGMA table_info(t)").fetchall()
            conn.close()
            pk_cols = [col[1] for col in info if col[5] > 0]
            assert "id" in pk_cols
        finally:
            result.cleanup()

    def test_reserved_word_column(self):
        sheet = _make_sheet("T", ["select", "from"], [
            ["a", "b"]
        ], {"select": "TEXT", "from": "TEXT"})
        result = load_to_sqlite(_make_wb([sheet]))
        try:
            conn = sqlite3.connect(result.db_path)
            rows = conn.execute('SELECT "select", "from" FROM t').fetchall()
            conn.close()
            assert rows[0] == ("a", "b")
        finally:
            result.cleanup()

    def test_meta_table(self):
        sheet = _make_sheet("T", ["id"], [[1]], {"id": "INTEGER"})
        result = load_to_sqlite(_make_wb([sheet]))
        try:
            conn = sqlite3.connect(result.db_path)
            meta = dict(conn.execute("SELECT key, value FROM _meta").fetchall())
            conn.close()
            assert "source_file" in meta
        finally:
            result.cleanup()

    def test_null_handling(self):
        sheet = _make_sheet("T", ["id", "val"], [
            [1, None], [2, "ok"]
        ], {"id": "INTEGER", "val": "TEXT"})
        result = load_to_sqlite(_make_wb([sheet]))
        try:
            conn = sqlite3.connect(result.db_path)
            rows = conn.execute("SELECT * FROM t").fetchall()
            conn.close()
            assert rows[0] == (1, None)
        finally:
            result.cleanup()

    def test_data_integrity(self):
        sheet = _make_sheet("T", ["a", "b", "c"], [
            [1, "hello", 3.14],
            [2, "world", 2.72],
        ], {"a": "INTEGER", "b": "TEXT", "c": "REAL"})
        result = load_to_sqlite(_make_wb([sheet]))
        try:
            conn = sqlite3.connect(result.db_path)
            rows = conn.execute("SELECT * FROM t").fetchall()
            conn.close()
            assert len(rows) == 2
            assert rows[0] == (1, "hello", 3.14)
        finally:
            result.cleanup()

    def test_relationships_in_result(self):
        s1 = _make_sheet("Customers", ["id", "name"], [
            [1, "A"], [2, "B"]
        ], {"id": "INTEGER", "name": "TEXT"}, primary_key="id")
        s2 = _make_sheet("Orders", ["oid", "id"], [
            [1, 1], [2, 2]
        ], {"oid": "INTEGER", "id": "INTEGER"})
        fk = ForeignKey("orders", "id", "customers", "id", 0.95)
        result = load_to_sqlite(_make_wb([s1, s2], [fk]))
        try:
            assert len(result.relationships) == 1
            assert result.relationships[0].from_table == "orders"
        finally:
            result.cleanup()

    def test_table_info_in_result(self):
        sheet = _make_sheet("MySheet", ["id", "val"], [
            [1, "a"], [2, "b"], [3, "c"]
        ], {"id": "INTEGER", "val": "TEXT"})
        result = load_to_sqlite(_make_wb([sheet]))
        try:
            assert len(result.tables) == 1
            t = result.tables[0]
            assert t.row_count == 3
            assert t.source_sheet == "MySheet"
            assert len(t.columns) == 2
        finally:
            result.cleanup()
