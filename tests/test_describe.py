import pytest
from table2db.models import SheetData, WorkbookData, ForeignKey
from table2db.pipeline.loader import load_to_sqlite
from table2db.describe import generate_db_summary


def _make_result():
    """Create a small result for testing."""
    s = SheetData(
        name="Products",
        headers=["id", "name", "price"],
        rows=[[1, "Widget", 9.99], [2, "Gadget", 19.99], [3, "Doohickey", None]],
        column_types={"id": "INTEGER", "name": "TEXT", "price": "REAL"},
        primary_key="id",
    )
    wb = WorkbookData(source_file="test.xlsx", sheets=[s])
    return load_to_sqlite(wb)


class TestDescribe:
    def test_contains_table_name(self):
        result = _make_result()
        try:
            summary = generate_db_summary(result)
            assert "products" in summary.lower()
        finally:
            result.cleanup()

    def test_contains_columns(self):
        result = _make_result()
        try:
            summary = generate_db_summary(result)
            assert "id" in summary
            assert "name" in summary
            assert "price" in summary
        finally:
            result.cleanup()

    def test_contains_sample_data(self):
        result = _make_result()
        try:
            summary = generate_db_summary(result, sample_rows=2)
            assert "Widget" in summary
            assert "Gadget" in summary
        finally:
            result.cleanup()

    def test_contains_stats(self):
        result = _make_result()
        try:
            summary = generate_db_summary(result)
            # Should contain numeric stats for price column
            assert "9.99" in summary or "19.99" in summary
        finally:
            result.cleanup()

    def test_contains_null_rate(self):
        result = _make_result()
        try:
            summary = generate_db_summary(result)
            # price has 1 null out of 3 → ~33%
            assert "33" in summary or "null" in summary.lower()
        finally:
            result.cleanup()

    def test_contains_relationships(self):
        s1 = SheetData(
            name="A", headers=["id", "name"],
            rows=[[1, "x"], [2, "y"]],
            column_types={"id": "INTEGER", "name": "TEXT"}, primary_key="id",
        )
        s2 = SheetData(
            name="B", headers=["bid", "id"],
            rows=[[1, 1], [2, 2]],
            column_types={"bid": "INTEGER", "id": "INTEGER"},
        )
        wb = WorkbookData(source_file="test.xlsx", sheets=[s1, s2],
                          relationships=[ForeignKey("b", "id", "a", "id", 0.9)])
        result = load_to_sqlite(wb)
        try:
            summary = generate_db_summary(result)
            assert "→" in summary or "->" in summary or "Relationship" in summary.lower() or "relationship" in summary.lower()
        finally:
            result.cleanup()

    def test_custom_sample_rows(self):
        result = _make_result()
        try:
            summary = generate_db_summary(result, sample_rows=1)
            assert "Widget" in summary
        finally:
            result.cleanup()
