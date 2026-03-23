import datetime
import pytest
from table2db.models import SheetData, WorkbookData
from table2db.pipeline.typer import infer_types


def _make_wb(sheets):
    return WorkbookData(source_file="test.xlsx", sheets=sheets)


def _make_sheet(name, headers, rows):
    return SheetData(name=name, headers=headers, rows=rows)


class TestTypeInference:
    def test_all_integers(self):
        sheet = _make_sheet("S", ["val"], [[1], [2], [3], [4], [5]])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "INTEGER"

    def test_all_floats(self):
        sheet = _make_sheet("S", ["val"], [[1.5], [2.7], [3.14]])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "REAL"

    def test_all_strings(self):
        sheet = _make_sheet("S", ["val"], [["a"], ["b"], ["c"]])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "TEXT"

    def test_all_dates(self):
        sheet = _make_sheet("S", ["val"], [
            [datetime.datetime(2024, 1, 1)],
            [datetime.datetime(2024, 2, 1)],
            [datetime.datetime(2024, 3, 1)],
        ])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "DATE"

    def test_majority_int_with_outlier(self):
        rows = [[i] for i in range(1, 10)] + [["outlier"]]
        sheet = _make_sheet("S", ["val"], rows)
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "INTEGER"
        # The outlier should be converted to None
        assert wb.sheets[0].rows[-1][0] is None

    def test_below_threshold_fallback_text(self):
        # 6 ints + 4 strings = 60% int < 80% threshold
        rows = [[i] for i in range(1, 7)] + [["a"], ["b"], ["c"], ["d"]]
        sheet = _make_sheet("S", ["val"], rows)
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "TEXT"

    def test_number_as_text(self):
        sheet = _make_sheet("S", ["val"], [["100"], ["200"], ["300"]])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "INTEGER"
        assert wb.sheets[0].rows[0][0] == 100

    def test_float_as_text(self):
        sheet = _make_sheet("S", ["val"], [["1.5"], ["2.7"], ["3.14"]])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "REAL"

    def test_boolean_to_integer(self):
        sheet = _make_sheet("S", ["val"], [[True], [False], [True]])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "INTEGER"
        assert wb.sheets[0].rows[0][0] == 1
        assert wb.sheets[0].rows[1][0] == 0

    def test_date_string_detection(self):
        sheet = _make_sheet("S", ["val"], [
            ["2024-01-15"], ["2024-02-20"], ["2024-03-10"],
        ])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "DATE"

    def test_mixed_date_formats(self):
        sheet = _make_sheet("S", ["val"], [
            [datetime.datetime(2024, 1, 15)],
            ["2024-02-20"],
            [datetime.datetime(2024, 3, 10)],
        ])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "DATE"

    def test_empty_column(self):
        sheet = _make_sheet("S", ["val"], [[None], [None], [None]])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["val"] == "TEXT"

    def test_custom_threshold(self):
        # 6 ints + 4 strings = 60%. With threshold=0.5, should be INTEGER
        rows = [[i] for i in range(1, 7)] + [["a"], ["b"], ["c"], ["d"]]
        sheet = _make_sheet("S", ["val"], rows)
        wb = infer_types(_make_wb([sheet]), type_threshold=0.5)
        assert wb.sheets[0].column_types["val"] == "INTEGER"

    def test_values_converted_in_place(self):
        sheet = _make_sheet("S", ["a", "b"], [
            [1, "hello"],
            [2, "world"],
        ])
        wb = infer_types(_make_wb([sheet]))
        assert wb.sheets[0].column_types["a"] == "INTEGER"
        assert wb.sheets[0].column_types["b"] == "TEXT"
        # Values should still be correct after conversion
        assert wb.sheets[0].rows[0][0] == 1
        assert wb.sheets[0].rows[0][1] == "hello"
