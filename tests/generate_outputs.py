"""Generate output .db and _summary.md files from all test fixtures.

Usage:
    python tests/generate_outputs.py
"""
import os
import shutil
from table2db import TableConverter
from table2db.describe import generate_db_summary

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output_dbs")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Clear old outputs
    for f in os.listdir(OUTPUT_DIR):
        os.unlink(os.path.join(OUTPUT_DIR, f))

    converter = TableConverter()
    count = 0

    for fx in sorted(os.listdir(FIXTURES_DIR)):
        if fx.startswith("~$"):
            continue
        src = os.path.join(FIXTURES_DIR, fx)
        base = os.path.splitext(fx)[0]

        try:
            result = converter.convert(src)
            shutil.copy2(result.db_path, os.path.join(OUTPUT_DIR, f"{base}.db"))

            summary = generate_db_summary(result, sample_rows=3)
            with open(os.path.join(OUTPUT_DIR, f"{base}_summary.md"), "w", encoding="utf-8") as f:
                f.write(summary)

            total_rows = sum(t.row_count for t in result.tables)
            print(f"  {fx:35s} -> {len(result.tables)} tables, {total_rows:3d} rows")
            result.cleanup()
            count += 1
        except Exception as e:
            print(f"  {fx:35s} -> {type(e).__name__}: {e}")

    print(f"\nGenerated {count} output sets (.db + _summary.md) in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
