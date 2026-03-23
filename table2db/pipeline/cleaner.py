"""Stage 3: Data cleaning — remove empty rows, duplicates, and subtotal rows."""

from __future__ import annotations

import re
from typing import Any

from table2db.models import SheetData, WorkbookData

DEFAULT_SUBTOTAL_KEYWORDS = [
    "合计", "小计", "总计", "总价",
    "total", "subtotal", "sum", "grand total",
]


def _strip_ws(s: str) -> str:
    """Remove ALL whitespace from a string."""
    return re.sub(r"\s+", "", s)


def _keyword_score(row: list[Any], keywords: list[str]) -> float:
    normalized_kw = [_strip_ws(k).lower() for k in keywords]
    for cell in row:
        if isinstance(cell, str):
            cell_norm = _strip_ws(cell).lower()
            if any(kw in cell_norm for kw in normalized_kw):
                return 1.0
    return 0.0


def _find_numeric_columns(rows: list[list[Any]], ncols: int) -> list[int]:
    """Return column indices where the majority of values are numeric."""
    numeric_cols = []
    for col in range(ncols):
        total = 0
        numeric = 0
        for row in rows:
            if col < len(row) and row[col] is not None:
                total += 1
                if isinstance(row[col], (int, float)) and not isinstance(row[col], bool):
                    numeric += 1
        if total > 0 and numeric / total > 0.5:
            numeric_cols.append(col)
    return numeric_cols


def _sum_score(
    row_idx: int,
    rows: list[list[Any]],
    numeric_cols: list[int],
    subtotal_flags: list[bool],
) -> float:
    if not numeric_cols:
        return 0.0

    # Find preceding consecutive non-subtotal rows
    preceding: list[list[Any]] = []
    for j in range(row_idx - 1, -1, -1):
        if subtotal_flags[j]:
            break
        preceding.append(rows[j])

    if not preceding:
        return 0.0

    matches = 0
    checked = 0
    for col in numeric_cols:
        row_val = rows[row_idx][col] if col < len(rows[row_idx]) else None
        if not isinstance(row_val, (int, float)) or isinstance(row_val, bool):
            continue
        expected = 0.0
        for p_row in preceding:
            v = p_row[col] if col < len(p_row) else None
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                expected += v
        checked += 1
        tolerance = 0.01 * max(1, abs(expected))
        if abs(row_val - expected) < tolerance:
            matches += 1

    if checked == 0:
        return 0.0
    return 1.0 if matches / checked >= 0.5 else 0.0


def _style_score(sheet: SheetData, data_row_idx: int) -> float:
    orig_row = sheet.header_row_end + 1 + data_row_idx
    style = sheet.row_styles.get(orig_row, {})
    if style.get("bold"):
        return 1.0
    if style.get("fill_color") is not None:
        return 1.0
    return 0.0


def clean_data(
    wb: WorkbookData,
    subtotal_keywords: list[str] | None = None,
) -> tuple[WorkbookData, list[str]]:
    keywords = subtotal_keywords if subtotal_keywords is not None else DEFAULT_SUBTOTAL_KEYWORDS
    warnings: list[str] = []
    kept_sheets: list[SheetData] = []

    for sheet in wb.sheets:
        # --- Remove empty rows ---
        rows = [r for r in sheet.rows if not all(v is None for v in r)]

        # --- Remove duplicate rows ---
        seen: set[tuple] = set()
        deduped: list[list[Any]] = []
        for r in rows:
            key = tuple(r)
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        rows = deduped

        # --- Subtotal detection ---
        ncols = len(sheet.headers)
        numeric_cols = _find_numeric_columns(rows, ncols)
        subtotal_flags = [False] * len(rows)

        kw_weight, sum_weight, style_weight = 0.5, 0.3, 0.2

        # We need the original indices for style lookup.
        # After empty/dup removal, row positions shift; we re-index using
        # header_row_end since the original mapping was for sheet.rows indices.
        # However, after filtering empty/dup rows, we lose the original
        # data-row index.  We need to track it.

        # Rebuild with original indices
        # Re-do from scratch: track original row index from sheet.rows
        rows_with_orig: list[tuple[int, list[Any]]] = []
        seen2: set[tuple] = set()
        for orig_i, r in enumerate(sheet.rows):
            if all(v is None for v in r):
                continue
            key = tuple(r)
            if key not in seen2:
                seen2.add(key)
                rows_with_orig.append((orig_i, r))

        rows = [r for _, r in rows_with_orig]
        orig_indices = [i for i, _ in rows_with_orig]

        # Track rows before cleaning and duplicates removed
        empty_removed = sum(1 for r in sheet.rows if all(v is None for v in r))
        duplicate_rows_removed = len(sheet.rows) - empty_removed - len(rows)
        sheet.metadata["rows_before_cleaning"] = len(rows)
        sheet.metadata["duplicate_rows_removed"] = max(duplicate_rows_removed, 0)

        numeric_cols = _find_numeric_columns(rows, ncols)
        subtotal_flags = [False] * len(rows)

        for i in range(len(rows)):
            kw = _keyword_score(rows[i], keywords)
            ss = _sum_score(i, rows, numeric_cols, subtotal_flags)
            # Use the original sheet.rows index for style lookup
            orig_row = sheet.header_row_end + 1 + orig_indices[i]
            style = sheet.row_styles.get(orig_row, {})
            st = 0.0
            if style.get("bold"):
                st = 1.0
            elif style.get("fill_color") is not None:
                st = 1.0

            score = kw_weight * kw + sum_weight * ss + style_weight * st
            if score >= 0.5:
                subtotal_flags[i] = True

        # Collect excluded original row indices and filter
        excluded: list[int] = []
        clean_rows: list[list[Any]] = []
        for i, row in enumerate(rows):
            if subtotal_flags[i]:
                excluded.append(sheet.header_row_end + 1 + orig_indices[i])
            else:
                clean_rows.append(row)

        sheet.rows = clean_rows
        sheet.excluded_rows = excluded
        sheet.metadata["rows_after_cleaning"] = len(sheet.rows)
        sheet.metadata["rows_filtered"] = len(excluded)

        if len(sheet.rows) == 0:
            warnings.append(f"Sheet '{sheet.name}' has no data rows after cleaning, skipped")
        else:
            kept_sheets.append(sheet)

    wb.sheets = kept_sheets
    return wb, warnings
