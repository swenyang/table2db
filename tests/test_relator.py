import pytest
from table2db.models import SheetData, WorkbookData
from table2db.pipeline.relator import infer_relationships


def _make_sheet(name, headers, rows, column_types=None):
    s = SheetData(name=name, headers=headers, rows=rows)
    if column_types:
        s.column_types = column_types
    return s


def _make_wb(sheets):
    return WorkbookData(source_file="test.xlsx", sheets=sheets)


class TestPrimaryKeyInference:
    def test_pk_by_name_and_uniqueness(self):
        sheet = _make_sheet("T", ["id", "name"], [
            [1, "a"], [2, "b"], [3, "c"]
        ], {"id": "INTEGER", "name": "TEXT"})
        wb = infer_relationships(_make_wb([sheet]))
        assert wb.sheets[0].primary_key == "id"

    def test_pk_id_suffix(self):
        sheet = _make_sheet("T", ["customer_id", "name"], [
            [1, "a"], [2, "b"], [3, "c"]
        ], {"customer_id": "INTEGER", "name": "TEXT"})
        wb = infer_relationships(_make_wb([sheet]))
        assert wb.sheets[0].primary_key == "customer_id"

    def test_no_pk_if_not_unique(self):
        sheet = _make_sheet("T", ["id", "name"], [
            [1, "a"], [1, "b"], [2, "c"]
        ], {"id": "INTEGER", "name": "TEXT"})
        wb = infer_relationships(_make_wb([sheet]))
        assert wb.sheets[0].primary_key is None

    def test_no_pk_for_float(self):
        sheet = _make_sheet("T", ["id", "name"], [
            [1.1, "a"], [2.2, "b"], [3.3, "c"]
        ], {"id": "REAL", "name": "TEXT"})
        wb = infer_relationships(_make_wb([sheet]))
        assert wb.sheets[0].primary_key is None

    def test_no_pk_if_has_nulls(self):
        sheet = _make_sheet("T", ["id", "name"], [
            [1, "a"], [None, "b"], [3, "c"]
        ], {"id": "INTEGER", "name": "TEXT"})
        wb = infer_relationships(_make_wb([sheet]))
        assert wb.sheets[0].primary_key is None


class TestForeignKeyInference:
    def _customers_products_orders(self):
        customers = _make_sheet("Customers", ["customer_id", "name"],
            [[i, f"C{i}"] for i in range(1, 21)],
            {"customer_id": "INTEGER", "name": "TEXT"})
        products = _make_sheet("Products", ["product_id", "pname"],
            [[i, f"P{i}"] for i in range(1, 16)],
            {"product_id": "INTEGER", "pname": "TEXT"})
        orders = _make_sheet("Orders",
            ["order_id", "customer_id", "product_id", "qty"],
            [[i, (i % 20) + 1, (i % 15) + 1, i * 10] for i in range(1, 31)],
            {"order_id": "INTEGER", "customer_id": "INTEGER",
             "product_id": "INTEGER", "qty": "INTEGER"})
        return customers, products, orders

    def test_fk_exact_name_match(self):
        customers, products, orders = self._customers_products_orders()
        wb = infer_relationships(_make_wb([customers, products, orders]))
        fk_cols = [(fk.from_column, fk.to_column) for fk in wb.relationships]
        assert ("customer_id", "customer_id") in fk_cols

    def test_fk_value_containment(self):
        customers, products, orders = self._customers_products_orders()
        wb = infer_relationships(_make_wb([customers, products, orders]))
        assert len(wb.relationships) >= 2  # customer_id and product_id

    def test_fk_cardinality_guard(self):
        """PK with only 5 distinct values → no FK."""
        small = _make_sheet("Small", ["id", "name"],
            [[i, f"S{i}"] for i in range(1, 6)],
            {"id": "INTEGER", "name": "TEXT"})
        ref = _make_sheet("Ref", ["ref_id", "id", "val"],
            [[i, (i % 5) + 1, i * 10] for i in range(1, 11)],
            {"ref_id": "INTEGER", "id": "INTEGER", "val": "INTEGER"})
        wb = infer_relationships(_make_wb([small, ref]))
        assert len(wb.relationships) == 0

    def test_fk_both_pk_excluded(self):
        """Same column is PK in both tables → not a FK."""
        t1 = _make_sheet("T1", ["id", "a"],
            [[i, f"a{i}"] for i in range(1, 21)],
            {"id": "INTEGER", "a": "TEXT"})
        t2 = _make_sheet("T2", ["id", "b"],
            [[i, f"b{i}"] for i in range(1, 21)],
            {"id": "INTEGER", "b": "TEXT"})
        wb = infer_relationships(_make_wb([t1, t2]))
        assert len(wb.relationships) == 0

    def test_fk_confidence_threshold(self):
        customers, products, orders = self._customers_products_orders()
        wb = infer_relationships(_make_wb([customers, products, orders]),
                                 fk_confidence_threshold=0.99)
        for fk in wb.relationships:
            assert fk.confidence >= 0.99
