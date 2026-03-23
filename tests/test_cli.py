import os
import sqlite3
import pytest
from table2db.cli import main


class TestConvertCommand:
    def test_basic_convert(self, fixture_path, tmp_path):
        out = str(tmp_path / "output.db")
        ret = main(["convert", fixture_path("simple.xlsx"), "-o", out])
        assert ret == 0
        assert os.path.exists(out)
        conn = sqlite3.connect(out)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name != '_meta'"
        )]
        assert len(tables) == 1
        conn.close()

    def test_convert_with_summary(self, fixture_path, tmp_path):
        out = str(tmp_path / "output.db")
        ret = main(["convert", fixture_path("simple.xlsx"), "-o", out, "--summary"])
        assert ret == 0
        summary_path = str(tmp_path / "output_summary.md")
        assert os.path.exists(summary_path)
        content = open(summary_path, encoding="utf-8").read()
        assert "# Database Summary" in content

    def test_convert_default_output_name(self, fixture_path, monkeypatch, tmp_path):
        monkeypatch.chdir(tmp_path)
        ret = main(["convert", fixture_path("simple.xlsx")])
        assert ret == 0
        assert os.path.exists(tmp_path / "simple.db")

    def test_convert_bad_file(self, tmp_path):
        bad = str(tmp_path / "nope.xlsx")
        ret = main(["convert", bad])
        assert ret == 1


class TestDescribeCommand:
    def test_describe_stdout(self, fixture_path, tmp_path, capsys):
        # First create a .db
        out = str(tmp_path / "test.db")
        main(["convert", fixture_path("multi_sheet_fk.xlsx"), "-o", out])
        ret = main(["describe", out])
        assert ret == 0
        captured = capsys.readouterr()
        assert "# Database Summary" in captured.out
        assert "customers" in captured.out

    def test_describe_to_file(self, fixture_path, tmp_path):
        out_db = str(tmp_path / "test.db")
        main(["convert", fixture_path("simple.xlsx"), "-o", out_db])
        out_md = str(tmp_path / "summary.md")
        ret = main(["describe", out_db, "-o", out_md])
        assert ret == 0
        assert os.path.exists(out_md)

    def test_describe_nonexistent(self, tmp_path):
        ret = main(["describe", str(tmp_path / "nope.db")])
        assert ret == 1


class TestNoCommand:
    def test_no_args_returns_1(self):
        ret = main([])
        assert ret == 1
