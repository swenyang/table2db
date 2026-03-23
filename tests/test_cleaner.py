import pytest
from table2db.models import SheetData, WorkbookData
from table2db.pipeline.cleaner import clean_data


def _make_wb(sheets):
    return WorkbookData(source_file="test.xlsx", sheets=sheets)


def _make_sheet(name, headers, rows, row_styles=None):
    s = SheetData(name=name, headers=headers, rows=rows, header_row_end=0)
    if row_styles:
        s.row_styles = row_styles
    return s


class TestEmptyRowRemoval:
    def test_remove_empty_rows(self):
        sheet = _make_sheet("S", ["a", "b"], [
            [1, 2], [None, None], [3, 4],
        ])
        wb, warnings = clean_data(_make_wb([sheet]))
        assert len(wb.sheets[0].rows) == 2
        assert wb.sheets[0].rows[0] == [1, 2]
        assert wb.sheets[0].rows[1] == [3, 4]


class TestDuplicateRemoval:
    def test_remove_duplicate_rows(self):
        sheet = _make_sheet("S", ["a", "b"], [
            [1, "x"], [2, "y"], [1, "x"], [3, "z"],
        ])
        wb, _ = clean_data(_make_wb([sheet]))
        assert len(wb.sheets[0].rows) == 3


class TestSubtotalDetection:
    def test_keyword_chinese(self):
        sheet = _make_sheet("S", ["region", "amount"], [
            ["North", 100], ["South", 200], ["小计", 300],
        ])
        wb, _ = clean_data(_make_wb([sheet]))
        assert len(wb.sheets[0].rows) == 2

    def test_keyword_english(self):
        sheet = _make_sheet("S", ["region", "amount"], [
            ["North", 100], ["South", 200], ["Total", 300],
        ])
        wb, _ = clean_data(_make_wb([sheet]))
        assert len(wb.sheets[0].rows) == 2

    def test_keyword_case_insensitive(self):
        sheet = _make_sheet("S", ["region", "amount"], [
            ["A", 10], ["B", 20], ["C", 30], ["SUBTOTAL", 60],
        ])
        wb, _ = clean_data(_make_wb([sheet]))
        assert len(wb.sheets[0].rows) == 3

    def test_keyword_with_spaces(self):
        sheet = _make_sheet("S", ["x", "val"], [
            ["a", 1], ["合 计", 1],
        ])
        wb, _ = clean_data(_make_wb([sheet]))
        assert len(wb.sheets[0].rows) == 1

    def test_sum_signal(self):
        """Row values = sum of above → detected even without keywords."""
        sheet = _make_sheet("S", ["label", "qty", "price"], [
            ["A", 10, 100],
            ["B", 20, 200],
            ["", 30, 300],  # sum row, no keyword but bold
        ], row_styles={3: {"bold": True, "fill_color": None}})
        # header_row_end=0, so data row 2 → orig row 3
        sheet.header_row_end = 0
        wb, _ = clean_data(_make_wb([sheet]))
        # sum(0.3) + style(0.2) = 0.5 → filtered
        assert len(wb.sheets[0].rows) == 2

    def test_custom_keywords(self):
        sheet = _make_sheet("S", ["a", "b"], [
            ["x", 1], ["Gesamt", 1],
        ])
        wb, _ = clean_data(_make_wb([sheet]), subtotal_keywords=["Gesamt"])
        assert len(wb.sheets[0].rows) == 1

    def test_data_rows_preserved(self):
        sheet = _make_sheet("S", ["a", "b"], [
            [1, "x"], [2, "y"], [3, "z"],
        ])
        wb, _ = clean_data(_make_wb([sheet]))
        assert len(wb.sheets[0].rows) == 3

    def test_excluded_rows_recorded(self):
        sheet = _make_sheet("S", ["r", "v"], [
            ["A", 10], ["Total", 10],
        ])
        wb, _ = clean_data(_make_wb([sheet]))
        assert len(wb.sheets[0].excluded_rows) > 0


class TestEmptyAfterClean:
    def test_sheet_removed_if_empty(self):
        sheet = _make_sheet("S", ["a", "b"], [
            ["Total", 500], ["合计", 500],
        ])
        wb, warnings = clean_data(_make_wb([sheet]))
        assert len(wb.sheets) == 0
        assert any("no data rows after cleaning" in w for w in warnings)
