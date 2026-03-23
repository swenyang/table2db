import os
import tempfile
from table2db.models import SheetData, WorkbookData, ForeignKey, TableInfo, ConversionResult


class TestSheetData:
    def test_create_minimal(self):
        sheet = SheetData(name="Sheet1")
        assert sheet.name == "Sheet1"
        assert sheet.headers == []
        assert sheet.rows == []
        assert sheet.column_types == {}
        assert sheet.primary_key is None

    def test_create_with_data(self):
        sheet = SheetData(
            name="Orders",
            header_row_start=0,
            header_row_end=0,
            headers=["id", "name", "amount"],
            rows=[[1, "Alice", 100.0], [2, "Bob", 200.0]],
        )
        assert len(sheet.rows) == 2
        assert sheet.headers == ["id", "name", "amount"]


class TestWorkbookData:
    def test_create(self):
        wb = WorkbookData(source_file="test.xlsx")
        assert wb.sheets == []
        assert wb.relationships == []


class TestForeignKey:
    def test_create(self):
        fk = ForeignKey(
            from_table="orders", from_column="customer_id",
            to_table="customers", to_column="id",
            confidence=0.95,
        )
        assert fk.confidence == 0.95


class TestConversionResult:
    def test_cleanup_deletes_file(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        result = ConversionResult(db_path=tmp.name, tables=[], relationships=[], warnings=[], metadata={})
        assert os.path.exists(result.db_path)
        result.cleanup()
        assert not os.path.exists(result.db_path)

    def test_context_manager(self):
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        path = tmp.name
        with ConversionResult(db_path=path, tables=[], relationships=[], warnings=[], metadata={}) as result:
            assert os.path.exists(result.db_path)
        assert not os.path.exists(path)
