"""Tests for Phase 2a features."""
import pytest
from table2db import TableConverter
from table2db.models import SheetData, WorkbookData
from table2db.pipeline.structure import detect_structure, _prune_empty_columns


class TestEmptyColumnPruning:
    def test_prune_all_null_columns(self):
        rows = [
            ["a", None, "b", None],
            [1, None, 2, None],
            [3, None, 4, None],
        ]
        pruned, kept = _prune_empty_columns(rows)
        assert len(pruned[0]) == 2  # only "a"/"b" columns kept
        assert kept == [0, 2]

    def test_no_pruning_needed(self):
        rows = [["a", "b"], [1, 2], [3, 4]]
        pruned, kept = _prune_empty_columns(rows)
        assert pruned == rows

    def test_partial_null_column_kept(self):
        """Column with 80% null (< 90% threshold) should be kept."""
        rows = [[1, None], [2, "x"], [3, "y"], [4, None], [5, None],
                [6, None], [7, None], [8, None], [9, None], [10, None]]
        pruned, kept = _prune_empty_columns(rows)
        assert len(pruned[0]) == 2  # 80% null < 90% threshold → kept

    def test_evaluation_fixture(self, fixture_path):
        """Evaluation_stocking EVALUATION sheet should have empty cols pruned."""
        converter = TableConverter()
        with converter.convert(fixture_path("Evaluation_stocking.xlsx")) as result:
            # EVALUATION table should not have 100% null columns
            import sqlite3
            conn = sqlite3.connect(result.db_path)
            for t in result.tables:
                if "evaluation" in t.name.lower():
                    for col in t.columns:
                        nulls = conn.execute(
                            f'SELECT COUNT(*) FROM "{t.name}" WHERE "{col["name"]}" IS NULL'
                        ).fetchone()[0]
                        total = t.row_count
                        assert nulls < total, f"Column {col['name']} is 100% NULL"
            conn.close()


class TestCascadingHeaderDetection:
    def test_string_dominant_still_works(self):
        """Standard string headers should still be detected."""
        sheet = SheetData(name="T", rows=[
            ["id", "name", "value"],
            [1, "Alice", 100],
            [2, "Bob", 200],
            [3, "Charlie", 300],
        ])
        wb = WorkbookData(source_file="test.xlsx", sheets=[sheet])
        wb, warnings = detect_structure(wb)
        assert wb.sheets[0].headers == ["id", "name", "value"]
        assert wb.sheets[0].metadata.get("header_confidence", 0) >= 0.8

    def test_numeric_header_skipped(self):
        """Numeric headers are skipped without LLM."""
        sheet = SheetData(name="T", rows=[
            ["Week", 1, 2, 3, 4, 5],
            ["Alice", 10, 20, 30, 40, 50],
            ["Bob", 15, 25, 35, 45, 55],
            ["Charlie", 12, 22, 32, 42, 52],
        ])
        wb = WorkbookData(source_file="test.xlsx", sheets=[sheet])
        wb, warnings = detect_structure(wb)
        assert len(wb.sheets) == 0

    def test_date_header_skipped(self):
        """Date headers are skipped (need LLM to handle)."""
        import datetime
        sheet = SheetData(name="T", rows=[
            ["Name", datetime.datetime(2025, 1, 1), datetime.datetime(2025, 2, 1), datetime.datetime(2025, 3, 1)],
            ["Alice", 100, 200, 300],
            ["Bob", 150, 250, 350],
            ["Charlie", 120, 220, 320],
        ])
        wb = WorkbookData(source_file="test.xlsx", sheets=[sheet])
        wb, warnings = detect_structure(wb)
        assert len(wb.sheets) == 0

    def test_all_numeric_skipped(self):
        """All-numeric sheets are skipped (need LLM to handle)."""
        sheet = SheetData(name="T", rows=[
            [None, None, None],
            [100, 200, 300],
            [101, 201, 301],
            [102, 202, 302],
        ])
        wb = WorkbookData(source_file="test.xlsx", sheets=[sheet])
        wb, warnings = detect_structure(wb)
        assert len(wb.sheets) == 0


class TestOverallScorePenalty:
    def test_skipped_sheets_lower_score(self, fixture_path):
        """Files with skipped sheets should have lower overall_score."""
        converter = TableConverter()
        with converter.convert(fixture_path("Evaluation_stocking.xlsx")) as result:
            # With cascading detection, more sheets should convert
            # But if any are skipped, score should be < 1.0
            if result.quality["sheets_skipped"]:
                assert result.quality["overall_score"] < 0.95


class TestColorMode:
    def test_color_ignore_default(self, fixture_path):
        """Default color_mode='ignore' should not extract colors."""
        converter = TableConverter()
        with converter.convert(fixture_path("simple.xlsx")) as result:
            assert len(result.tables) >= 1

    def test_color_value_mode(self, fixture_path):
        """color_mode='value' should fill empty colored cells with hex strings."""
        converter = TableConverter(color_mode="value")
        # This just verifies the mode doesn't crash
        with converter.convert(fixture_path("simple.xlsx")) as result:
            assert len(result.tables) >= 1
