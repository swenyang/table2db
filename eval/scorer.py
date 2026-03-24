"""Evaluation scoring system: compare table2db script outputs against golden standard DBs."""

import os
import re
import sqlite3
import json
import sys
from difflib import SequenceMatcher
from collections import defaultdict

EVAL_DIR = os.path.dirname(__file__)
GOLDEN_DIRS = [
    os.path.join(EVAL_DIR, "golden_dbs", "references"),
    os.path.join(EVAL_DIR, "golden_dbs", "deliverables"),
]
SCRIPT_OUTPUT_DIR = os.path.join(
    os.path.dirname(EVAL_DIR), "tests", "gdpval_outputs"
)

WEIGHTS = {
    "table_coverage": 0.3,
    "column_coverage": 0.2,
    "row_coverage": 0.2,
    "data_accuracy": 0.2,
    "type_correctness": 0.1,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_connect(path: str) -> sqlite3.Connection | None:
    """Open a SQLite DB, returning None on any error."""
    try:
        conn = sqlite3.connect(path)
        conn.execute("SELECT 1")  # verify it's a valid DB
        return conn
    except Exception:
        return None


def _get_tables(conn: sqlite3.Connection) -> list[str]:
    """Return non-meta user table names."""
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [r[0] for r in cur.fetchall() if not r[0].startswith("_")]


def _get_meta(conn: sqlite3.Connection, meta_table: str) -> dict[str, str]:
    """Read key-value meta table into a dict."""
    try:
        cur = conn.execute(f"SELECT key, value FROM [{meta_table}]")
        return dict(cur.fetchall())
    except Exception:
        return {}


def _table_source_sheet(meta: dict[str, str], table_name: str) -> str | None:
    """Extract source_sheet for a given table from meta dict."""
    return meta.get(f"table:{table_name}:source_sheet")


def _get_columns(conn: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    """Return [(col_name, col_type), ...] for a table."""
    try:
        cur = conn.execute(f"PRAGMA table_info([{table}])")
        return [(r[1], r[2]) for r in cur.fetchall()]
    except Exception:
        return []


def _get_row_count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return conn.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
    except Exception:
        return 0


def _normalize_name(name: str) -> str:
    """Normalize a column/table name for comparison."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _is_generic_column(name: str) -> bool:
    """Check if a column name is a generic placeholder like column_1, column_N."""
    return bool(re.match(r"^column[_\s]?\d+$", name.strip(), re.IGNORECASE))


def _normalize_source_sheet(sheet: str) -> str:
    """Strip _table_N suffix that script output adds to source_sheet values."""
    return re.sub(r"_table_\d+$", "", sheet).strip()


# ---------------------------------------------------------------------------
# Table matching
# ---------------------------------------------------------------------------

def match_tables(
    golden_conn: sqlite3.Connection,
    script_conn: sqlite3.Connection,
    golden_meta: dict[str, str],
    script_meta: dict[str, str],
) -> list[tuple[str, str | None]]:
    """Match golden tables to script tables.

    Returns list of (golden_table, script_table_or_None).
    """
    golden_tables = _get_tables(golden_conn)
    script_tables = _get_tables(script_conn)

    if not golden_tables:
        return []

    # Build source-sheet lookup for script tables
    # script source_sheet may be "Sheet1_table_1" — normalize to "Sheet1"
    script_sheet_map: dict[str, list[str]] = defaultdict(list)
    for st in script_tables:
        sheet = _table_source_sheet(script_meta, st)
        if sheet:
            norm = _normalize_source_sheet(sheet)
            script_sheet_map[norm.lower()].append(st)

    matched: list[tuple[str, str | None]] = []
    used_script: set[str] = set()

    # Pass 1: match by source_sheet
    for gt in golden_tables:
        g_sheet = _table_source_sheet(golden_meta, gt)
        if g_sheet:
            candidates = script_sheet_map.get(g_sheet.lower(), [])
            best, best_score = None, 0.0
            for c in candidates:
                if c in used_script:
                    continue
                # Prefer the candidate whose column set overlaps best
                g_cols = {_normalize_name(c_[0]) for c_ in _get_columns(golden_conn, gt)}
                s_cols = {_normalize_name(c_[0]) for c_ in _get_columns(script_conn, c)}
                if not g_cols:
                    score = 0.5
                else:
                    score = len(g_cols & s_cols) / len(g_cols)
                if score > best_score:
                    best_score = score
                    best = c
            if best is not None:
                matched.append((gt, best))
                used_script.add(best)
            else:
                matched.append((gt, None))
        else:
            matched.append((gt, None))

    # Pass 2: for unmatched golden tables, try fuzzy name matching
    for i, (gt, st) in enumerate(matched):
        if st is not None:
            continue
        best, best_ratio = None, 0.0
        gt_norm = _normalize_name(gt)
        for cand in script_tables:
            if cand in used_script:
                continue
            ratio = SequenceMatcher(None, gt_norm, _normalize_name(cand)).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best = cand
        if best_ratio > 0.6 and best is not None:
            matched[i] = (gt, best)
            used_script.add(best)

    # Pass 3: for still-unmatched golden tables with source_sheet, try matching
    # against script tables whose sheet is a superset (e.g. "Sheet1" matches
    # a script table with sheet "Sheet1_table_1" that wasn't picked up earlier)
    for i, (gt, st) in enumerate(matched):
        if st is not None:
            continue
        g_sheet = _table_source_sheet(golden_meta, gt)
        if not g_sheet:
            continue
        # Find any unused script table whose normalized sheet starts with g_sheet
        best, best_col_score = None, -1.0
        for cand in script_tables:
            if cand in used_script:
                continue
            s_sheet = _table_source_sheet(script_meta, cand)
            if s_sheet and _normalize_source_sheet(s_sheet).lower() == g_sheet.lower():
                g_cols = {_normalize_name(c_[0]) for c_ in _get_columns(golden_conn, gt)}
                s_cols = {_normalize_name(c_[0]) for c_ in _get_columns(script_conn, cand)}
                col_score = len(g_cols & s_cols) / max(len(g_cols), 1)
                if col_score > best_col_score:
                    best_col_score = col_score
                    best = cand
        if best is not None:
            matched[i] = (gt, best)
            used_script.add(best)

    return matched


# ---------------------------------------------------------------------------
# Scoring dimensions
# ---------------------------------------------------------------------------

def score_table_coverage(
    golden_conn: sqlite3.Connection,
    script_conn: sqlite3.Connection | None,
    matches: list[tuple[str, str | None]],
) -> float:
    """Score = matched_tables / golden_tables."""
    golden_tables = _get_tables(golden_conn)
    if not golden_tables:
        return 1.0  # no tables to match → perfect
    if script_conn is None:
        return 0.0
    matched = sum(1 for _, st in matches if st is not None)
    return matched / len(golden_tables)


def score_columns(golden_conn: sqlite3.Connection, script_conn: sqlite3.Connection,
                  matches: list[tuple[str, str | None]]) -> float:
    """Average column coverage across matched table pairs."""
    scores = []
    for gt, st in matches:
        if st is None:
            scores.append(0.0)
            continue
        g_cols = _get_columns(golden_conn, gt)
        s_cols = _get_columns(script_conn, st)
        if not g_cols:
            scores.append(1.0)
            continue
        s_name_set = {_normalize_name(c[0]) for c in s_cols}
        s_raw_names = {c[0] for c in s_cols}
        matched = 0.0
        for gc_name, _ in g_cols:
            gc_norm = _normalize_name(gc_name)
            if gc_norm in s_name_set:
                matched += 1.0
            elif any(_is_generic_column(sn) for sn in s_raw_names):
                # Partial credit: script used a generic column_N name
                matched += 0.5
            # else: 0
        scores.append(matched / len(g_cols))
    return sum(scores) / len(scores) if scores else 0.0


def score_rows(golden_conn: sqlite3.Connection, script_conn: sqlite3.Connection,
               matches: list[tuple[str, str | None]]) -> float:
    """Row count similarity averaged across matched tables."""
    scores = []
    for gt, st in matches:
        if st is None:
            scores.append(0.0)
            continue
        g_count = _get_row_count(golden_conn, gt)
        s_count = _get_row_count(script_conn, st)
        if g_count == 0 and s_count == 0:
            scores.append(1.0)
        elif max(g_count, s_count) == 0:
            scores.append(0.0)
        else:
            scores.append(min(g_count, s_count) / max(g_count, s_count))
    return sum(scores) / len(scores) if scores else 0.0


def _values_match(golden_val, script_val) -> bool:
    """Compare two cell values with tolerance."""
    # NULL handling
    if golden_val is None and script_val is None:
        return True
    if golden_val is None or script_val is None:
        # Treat empty string as NULL equivalent
        other = script_val if golden_val is None else golden_val
        if isinstance(other, str) and other.strip() == "":
            return True
        return False

    # Both numeric
    try:
        g_num = float(golden_val)
        s_num = float(script_val)
        if g_num == 0 and s_num == 0:
            return True
        if g_num == 0:
            return abs(s_num) < 0.01
        return abs(g_num - s_num) / max(abs(g_num), 1e-10) <= 0.01
    except (TypeError, ValueError):
        pass

    # String comparison (case-insensitive, stripped)
    g_str = str(golden_val).strip().lower()
    s_str = str(script_val).strip().lower()
    return g_str == s_str


def score_data(golden_conn: sqlite3.Connection, script_conn: sqlite3.Connection,
               matches: list[tuple[str, str | None]], sample: int = 20) -> float:
    """Data accuracy by sampling rows from matched tables."""
    scores = []
    for gt, st in matches:
        if st is None:
            scores.append(0.0)
            continue

        g_cols = _get_columns(golden_conn, gt)
        s_cols = _get_columns(script_conn, st)
        if not g_cols:
            scores.append(1.0)
            continue

        # Build column mapping: golden col index → script col index
        s_norm_map: dict[str, int] = {}
        for idx, (name, _) in enumerate(s_cols):
            s_norm_map[_normalize_name(name)] = idx

        col_mapping: list[tuple[int, int]] = []  # (g_idx, s_idx)
        for g_idx, (g_name, _) in enumerate(g_cols):
            s_idx = s_norm_map.get(_normalize_name(g_name))
            if s_idx is not None:
                col_mapping.append((g_idx, s_idx))

        if not col_mapping:
            scores.append(0.0)
            continue

        try:
            g_rows = golden_conn.execute(
                f"SELECT * FROM [{gt}] LIMIT ?", (sample,)
            ).fetchall()
            s_rows = script_conn.execute(
                f"SELECT * FROM [{st}] LIMIT ?", (sample,)
            ).fetchall()
        except Exception:
            scores.append(0.0)
            continue

        if not g_rows:
            scores.append(1.0)
            continue

        # For each golden row, find best matching script row
        total_cells = 0
        matching_cells = 0
        s_used: set[int] = set()

        for g_row in g_rows:
            best_match_count = -1
            best_s_idx = -1
            for s_idx, s_row in enumerate(s_rows):
                if s_idx in s_used:
                    continue
                mc = sum(
                    1 for gi, si in col_mapping
                    if _values_match(
                        g_row[gi] if gi < len(g_row) else None,
                        s_row[si] if si < len(s_row) else None,
                    )
                )
                if mc > best_match_count:
                    best_match_count = mc
                    best_s_idx = s_idx

            total_cells += len(col_mapping)
            if best_s_idx >= 0:
                s_used.add(best_s_idx)
                matching_cells += best_match_count

        scores.append(matching_cells / total_cells if total_cells else 1.0)

    return sum(scores) / len(scores) if scores else 0.0


def _type_category(type_str: str) -> str:
    """Map SQLite type to a category for comparison."""
    t = type_str.upper().strip() if type_str else ""
    if "INT" in t:
        return "NUMERIC"
    if "REAL" in t or "FLOAT" in t or "DOUBLE" in t or "NUMERIC" in t:
        return "NUMERIC"
    if "DATE" in t or "TIME" in t:
        return "DATETIME"
    if "BLOB" in t:
        return "BLOB"
    # TEXT or empty
    return "TEXT"


def score_types(golden_conn: sqlite3.Connection, script_conn: sqlite3.Connection,
                matches: list[tuple[str, str | None]]) -> float:
    """Type correctness for matched column pairs."""
    scores = []
    for gt, st in matches:
        if st is None:
            scores.append(0.0)
            continue

        g_cols = _get_columns(golden_conn, gt)
        s_cols = _get_columns(script_conn, st)
        if not g_cols:
            scores.append(1.0)
            continue

        s_type_map: dict[str, str] = {_normalize_name(n): t for n, t in s_cols}
        matched = 0
        total = 0
        for g_name, g_type in g_cols:
            s_type = s_type_map.get(_normalize_name(g_name))
            if s_type is not None:
                total += 1
                if _type_category(g_type) == _type_category(s_type):
                    matched += 1
                # TEXT storing numbers is a partial pass — but spec says full match only

        scores.append(matched / total if total else 0.0)
    return sum(scores) / len(scores) if scores else 0.0


# ---------------------------------------------------------------------------
# Per-file scoring
# ---------------------------------------------------------------------------

def score_file(golden_db_path: str, script_db_path: str | None) -> dict:
    """Score a single file comparison. Returns a dict of dimension scores."""
    basename = os.path.basename(golden_db_path)
    result: dict = {
        "file": basename,
        "converted": script_db_path is not None,
        "scores": {},
        "weighted_total": 0.0,
        "details": {},
    }

    golden_conn = _safe_connect(golden_db_path)
    if golden_conn is None:
        result["error"] = "Could not open golden DB"
        return result

    if script_db_path is None or not os.path.exists(script_db_path):
        # Failed to convert — all zeros
        for dim in WEIGHTS:
            result["scores"][dim] = 0.0
        result["details"]["golden_tables"] = len(_get_tables(golden_conn))
        result["details"]["script_tables"] = 0
        result["details"]["matched_tables"] = 0
        golden_conn.close()
        return result

    script_conn = _safe_connect(script_db_path)
    if script_conn is None:
        for dim in WEIGHTS:
            result["scores"][dim] = 0.0
        result["error"] = "Could not open script output DB"
        golden_conn.close()
        return result

    try:
        golden_meta = _get_meta(golden_conn, "_golden_meta")
        script_meta = _get_meta(script_conn, "_meta")

        matches = match_tables(golden_conn, script_conn, golden_meta, script_meta)

        golden_table_count = len(_get_tables(golden_conn))
        script_table_count = len(_get_tables(script_conn))
        matched_count = sum(1 for _, st in matches if st is not None)

        result["details"]["golden_tables"] = golden_table_count
        result["details"]["script_tables"] = script_table_count
        result["details"]["matched_tables"] = matched_count
        result["details"]["matches"] = [
            {"golden": gt, "script": st} for gt, st in matches
        ]

        result["scores"]["table_coverage"] = score_table_coverage(
            golden_conn, script_conn, matches
        )
        result["scores"]["column_coverage"] = score_columns(
            golden_conn, script_conn, matches
        )
        result["scores"]["row_coverage"] = score_rows(
            golden_conn, script_conn, matches
        )
        result["scores"]["data_accuracy"] = score_data(
            golden_conn, script_conn, matches
        )
        result["scores"]["type_correctness"] = score_types(
            golden_conn, script_conn, matches
        )

        result["weighted_total"] = sum(
            result["scores"][dim] * WEIGHTS[dim] for dim in WEIGHTS
        )
    except Exception as e:
        result["error"] = str(e)
        for dim in WEIGHTS:
            result["scores"].setdefault(dim, 0.0)
    finally:
        golden_conn.close()
        script_conn.close()

    return result


# ---------------------------------------------------------------------------
# Full evaluation
# ---------------------------------------------------------------------------

def _find_script_output(golden_basename: str, script_dirs: list[str]) -> str | None:
    """Find the matching script output DB for a golden DB filename."""
    stem = os.path.splitext(golden_basename)[0]
    candidates = [
        stem.replace(" ", "_") + ".db",
        golden_basename,
    ]
    for sdir in script_dirs:
        for name in candidates:
            path = os.path.join(sdir, name)
            if os.path.exists(path):
                return path
    return None


def run_evaluation(script_output_dir: str | None = None) -> dict:
    """Run full evaluation across all golden DBs and return results.
    
    Args:
        script_output_dir: Directory containing script output .db files.
            Can be a flat directory, or contain references/ and deliverables/ subdirs.
            Defaults to tests/gdpval_outputs.
    """
    if script_output_dir is None:
        script_output_dir = SCRIPT_OUTPUT_DIR

    # Build list of dirs to search for script outputs
    script_dirs = []
    for subdir in ["references", "deliverables"]:
        sub = os.path.join(script_output_dir, subdir)
        if os.path.isdir(sub):
            script_dirs.append(sub)
    if not script_dirs:
        # Flat directory
        script_dirs = [script_output_dir]

    golden_files = []  # list of (filename, full_path, category)
    for gdir in GOLDEN_DIRS:
        if not os.path.isdir(gdir):
            continue
        category = "references" if "references" in gdir else "deliverables"
        for f in os.listdir(gdir):
            if f.endswith(".db"):
                golden_files.append((f, os.path.join(gdir, f), category))
    golden_files.sort(key=lambda x: x[0])

    results: dict = {
        "total_files": len(golden_files),
        "converted": 0,
        "failed": 0,
        "file_results": [],
        "overall_scores": {},
    }

    dim_sums: dict[str, float] = {d: 0.0 for d in WEIGHTS}

    for gf, golden_path, category in golden_files:
        script_path = _find_script_output(gf, script_dirs)

        file_result = score_file(golden_path, script_path)
        file_result["category"] = category
        results["file_results"].append(file_result)

        if file_result["converted"]:
            results["converted"] += 1
        else:
            results["failed"] += 1

        for dim in WEIGHTS:
            dim_sums[dim] += file_result["scores"].get(dim, 0.0)

    n = max(results["total_files"], 1)
    for dim in WEIGHTS:
        results["overall_scores"][dim] = round(dim_sums[dim] / n, 4)

    results["overall_scores"]["weighted_total"] = round(
        sum(results["overall_scores"][d] * WEIGHTS[d] for d in WEIGHTS), 4
    )

    return results


# ---------------------------------------------------------------------------
# Report printing
# ---------------------------------------------------------------------------

def print_report(results: dict) -> None:
    total = results["total_files"]
    converted = results["converted"]
    failed = results["failed"]
    ov = results["overall_scores"]

    print("=== EVALUATION REPORT ===")
    print(f"Files: {total} total, {converted} converted, {failed} failed")
    print()
    print("Overall Scores:")
    print(f"  Table Coverage:   {ov.get('table_coverage', 0):.4f}")
    print(f"  Column Coverage:  {ov.get('column_coverage', 0):.4f}")
    print(f"  Row Coverage:     {ov.get('row_coverage', 0):.4f}")
    print(f"  Data Accuracy:    {ov.get('data_accuracy', 0):.4f}")
    print(f"  Type Correctness: {ov.get('type_correctness', 0):.4f}")
    print(f"  WEIGHTED TOTAL:   {ov.get('weighted_total', 0):.4f}")
    print()

    # Score distribution
    file_results = results["file_results"]
    grades = {"A (>=0.8)": 0, "B (0.6-0.8)": 0, "C (0.4-0.6)": 0,
              "D (0.2-0.4)": 0, "F (<0.2)": 0}
    for fr in file_results:
        w = fr.get("weighted_total", 0.0)
        if w >= 0.8:
            grades["A (>=0.8)"] += 1
        elif w >= 0.6:
            grades["B (0.6-0.8)"] += 1
        elif w >= 0.4:
            grades["C (0.4-0.6)"] += 1
        elif w >= 0.2:
            grades["D (0.2-0.4)"] += 1
        else:
            grades["F (<0.2)"] += 1

    print("Score Distribution:")
    for grade, count in grades.items():
        suffix = f" (includes {failed} failures)" if "F" in grade else ""
        print(f"  {grade}: {count} files{suffix}")
    print()

    # Sort by weighted total
    sorted_results = sorted(file_results, key=lambda x: x.get("weighted_total", 0.0))

    # Worst 10
    print("Worst 10 files:")
    for fr in sorted_results[:10]:
        s = fr["scores"]
        w = fr.get("weighted_total", 0.0)
        name = fr["file"]
        d = fr.get("details", {})
        gt = d.get("golden_tables", "?")
        mt = d.get("matched_tables", "?")
        tag = "(not converted)" if not fr["converted"] else (
            f"(table: {mt}/{gt}, col: {s.get('column_coverage',0):.2f}, "
            f"row: {s.get('row_coverage',0):.2f}, data: {s.get('data_accuracy',0):.2f})"
        )
        print(f"  {w:.4f}  {name}  {tag}")
    print()

    # Best 10
    print("Best 10 files:")
    for fr in sorted_results[-10:][::-1]:
        s = fr["scores"]
        w = fr.get("weighted_total", 0.0)
        name = fr["file"]
        d = fr.get("details", {})
        gt = d.get("golden_tables", "?")
        mt = d.get("matched_tables", "?")
        if w >= 0.99:
            tag = "(perfect match)"
        else:
            tag = (
                f"(table: {mt}/{gt}, col: {s.get('column_coverage',0):.2f}, "
                f"row: {s.get('row_coverage',0):.2f}, data: {s.get('data_accuracy',0):.2f})"
            )
        print(f"  {w:.4f}  {name}  {tag}")


def save_csv(results: dict, csv_path: str) -> None:
    """Save per-file results as CSV."""
    import csv
    file_results = results.get("file_results", [])

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "Category", "File", "Converted", "Weighted_Total",
            "Table_Coverage", "Column_Coverage", "Row_Coverage",
            "Data_Accuracy", "Type_Correctness",
        ])
        for fr in sorted(file_results, key=lambda x: (
            x.get("category", ""), -x.get("weighted_total", 0.0)
        )):
            s = fr["scores"]
            wt = fr.get("weighted_total", sum(s.get(d, 0) * WEIGHTS[d] for d in WEIGHTS))
            w.writerow([
                fr.get("category", ""),
                fr["file"],
                "Y" if fr["converted"] else "N",
                f"{wt:.2f}",
                f"{s.get('table_coverage', 0):.2f}",
                f"{s.get('column_coverage', 0):.2f}",
                f"{s.get('row_coverage', 0):.2f}",
                f"{s.get('data_accuracy', 0):.2f}",
                f"{s.get('type_correctness', 0):.2f}",
            ])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Evaluate table2db outputs against golden standard DBs.")
    parser.add_argument(
        "output_dir", nargs="?", default=None,
        help="Directory with script output .db files (flat or with references/deliverables subdirs). "
             "Default: tests/gdpval_outputs",
    )
    args = parser.parse_args()

    results = run_evaluation(script_output_dir=args.output_dir)
    print_report(results)

    eval_dir = os.path.dirname(__file__)
    json_path = os.path.join(eval_dir, "eval_results.json")
    csv_path = os.path.join(eval_dir, "eval_scores.csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    save_csv(results, csv_path)

    print(f"\nResults saved to:")
    print(f"  {json_path}")
    print(f"  {csv_path}")
