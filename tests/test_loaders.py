import os
import sqlite3
import pytest
from table2db.models import SheetData, WorkbookData
from table2db.loaders import BaseLoader, SqliteLoader


class TestSqliteLoader:
    def _make_wb(self):
        s = SheetData(
            name="Test",
            headers=["id", "val"],
            rows=[[1, "a"], [2, "b"]],
            column_types={"id": "INTEGER", "val": "TEXT"},
        )
        return WorkbookData(source_file="test.xlsx", sheets=[s])

    def test_default_temp_path(self):
        loader = SqliteLoader()
        result = loader.load(self._make_wb())
        try:
            assert os.path.exists(result.db_path)
            assert "table2db_" in result.db_path
        finally:
            result.cleanup()

    def test_custom_output_path(self, tmp_path):
        out = str(tmp_path / "custom.db")
        loader = SqliteLoader(output_path=out)
        result = loader.load(self._make_wb())
        assert result.db_path == out
        assert os.path.exists(out)
        conn = sqlite3.connect(out)
        rows = conn.execute("SELECT * FROM test").fetchall()
        assert len(rows) == 2
        conn.close()

    def test_implements_base(self):
        assert isinstance(SqliteLoader(), BaseLoader)


class TestConverterWithLoader:
    def test_process_returns_workbook_data(self, fixture_path):
        from table2db import TableConverter
        converter = TableConverter()
        wb, warnings = converter.process(fixture_path("simple.xlsx"))
        assert len(wb.sheets) == 1
        assert wb.sheets[0].headers  # has headers
        assert wb.sheets[0].column_types  # has types

    def test_convert_with_custom_loader(self, fixture_path, tmp_path):
        from table2db import TableConverter
        out = str(tmp_path / "custom.db")
        converter = TableConverter()
        loader = SqliteLoader(output_path=out)
        result = converter.convert(fixture_path("simple.xlsx"), loader=loader)
        assert result.db_path == out
        assert os.path.exists(out)
