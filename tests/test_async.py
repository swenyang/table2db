import asyncio
import pytest
from table2db import TableConverter


class TestAsync:
    def test_convert_async(self, fixture_path):
        converter = TableConverter()
        result = asyncio.run(converter.convert_async(fixture_path("simple.xlsx")))
        try:
            assert len(result.tables) == 1
            assert result.tables[0].row_count == 20
        finally:
            result.cleanup()

    def test_process_async(self, fixture_path):
        converter = TableConverter()
        wb, warnings = asyncio.run(converter.process_async(fixture_path("simple.xlsx")))
        assert len(wb.sheets) == 1
        assert wb.sheets[0].headers
