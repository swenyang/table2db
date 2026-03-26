"""Generate output .db, _summary.md, and _quality.json files from all test fixtures.

Usage:
    python tests/generate_outputs.py
"""
import json
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
        try:
            os.unlink(os.path.join(OUTPUT_DIR, f))
        except PermissionError:
            pass  # skip locked files, will be overwritten

    converter = TableConverter()
    count = 0

    for fx in sorted(os.listdir(FIXTURES_DIR)):
        if fx.startswith("~$"):
            continue
        src = os.path.join(FIXTURES_DIR, fx)
        base = os.path.splitext(fx)[0]
        # Sanitize spaces in filename
        safe_base = base.replace(" ", "_")

        try:
            result = converter.convert(src)
            db_dst = os.path.join(OUTPUT_DIR, f"{safe_base}.db")
            shutil.copy2(result.db_path, db_dst)

            mapping_dst = os.path.join(OUTPUT_DIR, f"{safe_base}.mapping.json")
            result.write_mapping(mapping_dst)

            summary = generate_db_summary(result, sample_rows=3)
            with open(os.path.join(OUTPUT_DIR, f"{safe_base}_summary.md"), "w", encoding="utf-8") as f:
                f.write(summary)

            with open(os.path.join(OUTPUT_DIR, f"{safe_base}_quality.json"), "w", encoding="utf-8") as f:
                json.dump(result.quality, f, indent=2, ensure_ascii=False)

            total_rows = sum(t.row_count for t in result.tables)
            score = result.quality["overall_score"]
            print(f"  {fx:50s} -> {len(result.tables):2d} tables, {total_rows:3d} rows, score={score}")
            result.cleanup()
            count += 1
        except Exception as e:
            print(f"  {fx:50s} -> {type(e).__name__}: {e}")

    print(f"\nGenerated {count} output sets (.db + _summary.md + _quality.json) in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
