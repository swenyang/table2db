from table2db.pipeline.island_detector import detect_table_islands, TableRegion


class TestIslandDetector:
    def test_single_table(self):
        rows = [
            ["a", "b", "c"],
            [1, 2, 3],
            [4, 5, 6],
        ]
        islands = detect_table_islands(rows)
        assert len(islands) == 1
        assert islands[0].confidence > 0.5

    def test_two_tables_vertical(self):
        rows = [
            ["a", "b"],
            [1, 2],
            [3, 4],
            [None, None],
            [None, None],
            ["x", "y"],
            [10, 20],
            [30, 40],
        ]
        islands = detect_table_islands(rows)
        assert len(islands) == 2

    def test_empty_grid(self):
        islands = detect_table_islands([])
        assert islands == []

    def test_all_none(self):
        rows = [[None, None], [None, None]]
        islands = detect_table_islands(rows)
        assert islands == []

    def test_single_row_filtered(self):
        """A single row doesn't meet min_rows=2."""
        rows = [
            [1, 2, 3],
            [None, None, None],
            [None, None, None],
            [4, 5, 6],
            [7, 8, 9],
        ]
        islands = detect_table_islands(rows)
        # First "table" is only 1 row, should be filtered
        assert len(islands) == 1

    def test_confidence_based_on_density(self):
        # Dense table -> high confidence
        rows = [[1, 2], [3, 4], [5, 6]]
        islands = detect_table_islands(rows)
        assert islands[0].confidence > 0.8
