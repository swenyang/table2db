"""Tests for quality metrics in ConversionResult."""

import pytest
from table2db import TableConverter


class TestQualityMetrics:
    def test_overall_score_exists(self, fixture_path):
        converter = TableConverter()
        with converter.convert(fixture_path("simple.xlsx")) as result:
            assert "overall_score" in result.quality
            assert 0.0 <= result.quality["overall_score"] <= 1.0

    def test_table_score_exists(self, fixture_path):
        converter = TableConverter()
        with converter.convert(fixture_path("simple.xlsx")) as result:
            for table in result.tables:
                assert table.name in result.quality["tables"]
                tq = result.quality["tables"][table.name]
                assert "table_score" in tq
                assert 0.0 <= tq["table_score"] <= 1.0

    def test_type_reliability_per_column(self, fixture_path):
        converter = TableConverter()
        with converter.convert(fixture_path("simple.xlsx")) as result:
            tq = result.quality["tables"][result.tables[0].name]
            assert "type_reliability" in tq
            for col in result.tables[0].columns:
                assert col["name"] in tq["type_reliability"]
                assert 0.0 <= tq["type_reliability"][col["name"]] <= 1.0

    def test_null_rates_per_column(self, fixture_path):
        converter = TableConverter()
        with converter.convert(fixture_path("mixed_types.xlsx")) as result:
            tq = result.quality["tables"][result.tables[0].name]
            assert "null_rates" in tq
            # mixed_types has outliers converted to NULL in value column
            assert tq["null_rates"]["value"] > 0

    def test_cleaning_stats(self, fixture_path):
        converter = TableConverter()
        with converter.convert(fixture_path("subtotals.xlsx")) as result:
            tq = result.quality["tables"][result.tables[0].name]
            assert tq["rows_filtered"] > 0
            assert tq["rows_before_cleaning"] > tq["rows_after_cleaning"]

    def test_sheets_skipped(self, fixture_path):
        converter = TableConverter()
        with converter.convert(fixture_path("simple.xlsx")) as result:
            assert "sheets_found" in result.quality
            assert "sheets_skipped" in result.quality
            assert result.quality["sheets_converted"] == len(result.tables)

    def test_relationship_quality(self, fixture_path):
        converter = TableConverter()
        with converter.convert(fixture_path("multi_sheet_fk.xlsx")) as result:
            assert len(result.quality["relationships"]) >= 2
            for rq in result.quality["relationships"]:
                assert "from" in rq
                assert "to" in rq
                assert "confidence" in rq

    def test_high_quality_simple_file(self, fixture_path):
        """A clean simple file should score very high."""
        converter = TableConverter()
        with converter.convert(fixture_path("simple.xlsx")) as result:
            assert result.quality["overall_score"] >= 0.8

    def test_multi_table_sheet_quality(self, fixture_path):
        converter = TableConverter()
        with converter.convert(fixture_path("multi_table_sheet.xlsx")) as result:
            for tname, tq in result.quality["tables"].items():
                assert "detection_confidence" in tq
                assert tq["detection_confidence"] > 0
