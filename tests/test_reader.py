"""Tests for Stage 1 — Reader."""
import pytest

from table2db.pipeline.reader import read_workbook
from table2db.errors import FileReadError, UnsupportedFormatError


def test_read_simple(fixture_path):
    wb = read_workbook(fixture_path("simple.xlsx"))
    assert len(wb.sheets) == 1
    sheet = wb.sheets[0]
    assert sheet.name == "Orders"
    # Raw rows: 1 header + 20 data = 21
    assert len(sheet.rows) == 21
    assert len(sheet.rows[0]) >= 3


def test_read_merged_cells(fixture_path):
    wb = read_workbook(fixture_path("merged_cells.xlsx"))
    sheet = wb.sheets[0]
    # Column B: "Group A" label merged across rows 3-5 (0-indexed: rows 2-4)
    assert sheet.rows[2][1] == "Group A"
    assert sheet.rows[3][1] == "Group A"
    assert sheet.rows[4][1] == "Group A"
    # Column D: numeric subtotal merged across rows 3-5
    # Group A total = 100 + 200 + 300 = 600, filled into all 3 rows
    assert sheet.rows[2][3] == 600
    assert sheet.rows[3][3] == 600
    assert sheet.rows[4][3] == 600


def test_read_error_values_to_none(fixture_path):
    wb = read_workbook(fixture_path("error_values.xlsx"))
    sheet = wb.sheets[0]
    # Row 1 (0-indexed) has "#REF!" in column 1 → None
    assert sheet.rows[1][1] is None
    # Row 2: column 2 has "#N/A" → None
    assert sheet.rows[2][2] is None
    # Row 3: column 1 has "#DIV/0!" → None
    assert sheet.rows[3][1] is None


def test_read_hidden_rows_cols_metadata(fixture_path):
    wb = read_workbook(fixture_path("hidden_rows_cols.xlsx"))
    sheet = wb.sheets[0]
    # Rows 5, 10, 15 (1-indexed) hidden → 0-indexed = 4, 9, 14
    assert 4 in sheet.metadata["hidden_rows"]
    assert 9 in sheet.metadata["hidden_rows"]
    assert 14 in sheet.metadata["hidden_rows"]
    # Column D (1-indexed col 4) hidden → 0-indexed = 3
    assert 3 in sheet.metadata["hidden_cols"]
    # Hidden rows/cols data is still present (hidden != deleted)
    assert len(sheet.rows) == 21  # 1 header + 20 data


def test_read_multi_sheet(fixture_path):
    wb = read_workbook(fixture_path("multi_sheet_fk.xlsx"))
    assert len(wb.sheets) == 3
    names = {s.name for s in wb.sheets}
    assert names == {"Customers", "Products", "Orders"}


def test_read_nonexistent_file():
    with pytest.raises(FileReadError):
        read_workbook("nonexistent_file.xlsx")


def test_read_unsupported_format(tmp_path):
    txt_file = tmp_path / "data.txt"
    txt_file.write_text("hello")
    with pytest.raises(UnsupportedFormatError):
        read_workbook(str(txt_file))


def test_read_row_styles_preserved(fixture_path):
    wb = read_workbook(fixture_path("subtotals.xlsx"))
    sheet = wb.sheets[0]
    # Subtotals layout: header(row1), 4xNorth(rows2-5), subtotal(row6), 4xSouth(rows7-10),
    # subtotal(row11), 4xEast(rows12-15), Subtotal(row16), grand_total(row17).
    # 0-indexed: subtotal=row5, subtotal=row10, Subtotal=row15, grand_total=row16
    assert sheet.row_styles[5]["bold"] is True   # first subtotal row
    assert sheet.row_styles[15]["bold"] is True  # East "Subtotal"
    # First data row (North/Widget A) → 0-indexed row 1
    assert sheet.row_styles[1]["bold"] is False


def test_read_csv(fixture_path):
    wb = read_workbook(fixture_path("students_csv.csv"))
    assert len(wb.sheets) == 1
    assert len(wb.sheets[0].rows) >= 20  # header + data


def test_read_tsv(fixture_path):
    wb = read_workbook(fixture_path("products_tsv.tsv"))
    assert len(wb.sheets) == 1
    assert len(wb.sheets[0].rows) >= 15


def test_read_bytesio_xlsx(fixture_path):
    """Read .xlsx from a BytesIO stream."""
    import io
    path = fixture_path("simple.xlsx")
    with open(path, "rb") as f:
        stream = io.BytesIO(f.read())
    wb = read_workbook(stream, file_name="simple.xlsx")
    assert len(wb.sheets) == 1
    assert wb.source_file == "simple.xlsx"
    assert len(wb.sheets[0].rows) == 21  # 1 header + 20 data


def test_read_bytesio_csv(fixture_path):
    """Read .csv from a BytesIO stream."""
    import io
    path = fixture_path("students_csv.csv")
    with open(path, "rb") as f:
        stream = io.BytesIO(f.read())
    wb = read_workbook(stream, file_name="students_csv.csv")
    assert len(wb.sheets) == 1
    assert len(wb.sheets[0].rows) >= 20


def test_read_stream_requires_filename():
    """Stream without file_name raises FileReadError."""
    import io
    stream = io.BytesIO(b"data")
    with pytest.raises(FileReadError):
        read_workbook(stream)
