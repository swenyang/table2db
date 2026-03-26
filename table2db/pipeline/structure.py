"""Stage 2: Detect structure — find headers, normalise names, extract data rows."""
from __future__ import annotations

from table2db.models import SheetData, WorkbookData
from table2db.pipeline.island_detector import detect_table_islands


def detect_structure(
    wb: WorkbookData,
    header_min_fill_ratio: float = 0.5,
    header_min_string_ratio: float = 0.7,
) -> tuple[WorkbookData, list[str]]:
    """Detect headers and data regions in each sheet.

    Args:
        header_min_fill_ratio: Min ratio of non-empty cells in a header row (default 0.5).
        header_min_string_ratio: Min ratio of string cells in a header row (default 0.7).

    Returns the modified WorkbookData and a list of warning strings.
    """
    warnings: list[str] = []
    kept_sheets: list[SheetData] = []

    for sheet in wb.sheets:
        islands = detect_table_islands(sheet.rows)

        if len(islands) <= 1:
            sheet_warnings = _process_sheet(sheet, header_min_fill_ratio,
                                            header_min_string_ratio)
            warnings.extend(sheet_warnings)
            if sheet.headers and sheet.rows:
                sheet.metadata.setdefault("island_confidence", 1.0)
                kept_sheets.append(sheet)
            else:
                warnings.append(
                    f"Sheet '{sheet.name}' removed: no headers or no data rows"
                )
        else:
            warnings.append(
                f"Sheet '{sheet.name}': detected {len(islands)} tables, splitting"
            )
            for i, island in enumerate(islands):
                sub_name = f"{sheet.name}_table_{i+1}"
                sub_sheet = SheetData(
                    name=sub_name,
                    rows=[row[island.col_start:island.col_end]
                          for row in sheet.rows[island.row_start:island.row_end]],
                    merge_map=sheet.merge_map,
                    metadata={**sheet.metadata, "island_confidence": island.confidence},
                    row_styles=sheet.row_styles,
                    original_col_indices=(
                        sheet.original_col_indices[island.col_start:island.col_end]
                        if sheet.original_col_indices else []
                    ),
                )
                sub_warnings = _process_sheet(sub_sheet, header_min_fill_ratio,
                                              header_min_string_ratio)
                warnings.extend(sub_warnings)
                if sub_sheet.headers and sub_sheet.rows:
                    kept_sheets.append(sub_sheet)

    wb.sheets = kept_sheets
    return wb, warnings


def _prune_empty_columns(rows: list[list], threshold: float = 0.9) -> tuple[list[list], list[int]]:
    """Remove columns where >= threshold of cells are None.

    Returns (pruned_rows, kept_column_indices).
    """
    if not rows:
        return rows, []
    max_cols = max(len(r) for r in rows)
    if max_cols == 0:
        return rows, []

    keep_cols = []
    for col in range(max_cols):
        none_count = sum(1 for r in rows if col >= len(r) or r[col] is None)
        if none_count / len(rows) < threshold:
            keep_cols.append(col)

    if len(keep_cols) == max_cols:
        return rows, list(range(max_cols))  # nothing pruned

    pruned = [[r[c] if c < len(r) else None for c in keep_cols] for r in rows]
    return pruned, keep_cols


def _process_sheet(
    sheet: SheetData,
    header_min_fill_ratio: float = 0.5,
    header_min_string_ratio: float = 0.7,
) -> list[str]:
    """Process a single sheet using heuristics: find header, extract data, normalise."""
    warnings: list[str] = []
    rows = sheet.rows

    if not rows:
        return warnings

    # Prune empty columns first
    original_max_cols = max((len(r) for r in rows), default=0)
    rows, kept_cols = _prune_empty_columns(rows)
    pruned_count = original_max_cols - len(kept_cols) if original_max_cols > 0 else 0
    if pruned_count > 0:
        sheet.metadata["columns_pruned"] = pruned_count
        # Update original_col_indices to reflect pruning
        if sheet.original_col_indices:
            sheet.original_col_indices = [sheet.original_col_indices[c] for c in kept_cols]
        # Remap merge_map column indices
        old_to_new = {old: new for new, old in enumerate(kept_cols)}
        new_merge_map = {}
        for (r, c), val in sheet.merge_map.items():
            if c in old_to_new:
                new_merge_map[(r, old_to_new[c])] = val
        sheet.merge_map = new_merge_map
    sheet.rows = rows

    max_cols = max((len(r) for r in rows), default=0)
    if max_cols == 0:
        return warnings

    # Pad short rows
    for i, row in enumerate(rows):
        if len(row) < max_cols:
            rows[i] = row + [None] * (max_cols - len(row))

    # Try header detection — Strategy 1: string-dominant
    header_start = None
    header_confidence_base = 0.0

    header_start = _strategy_string_dominant(rows, max_cols, sheet.merge_map,
                                             header_min_fill_ratio, header_min_string_ratio)
    if header_start is not None:
        header_confidence_base = 0.9

    if header_start is None:
        sheet.headers = []
        sheet.rows = []
        return warnings

    sheet.header_row_start = header_start

    # Compute header confidence
    non_none = [v for v in rows[header_start] if v is not None]
    fill_ratio = len(non_none) / max_cols if max_cols > 0 else 0
    sheet.metadata["header_confidence"] = round(header_confidence_base * min(fill_ratio * 1.2, 1.0), 2)

    # Multi-level header detection
    header_end = header_start
    next_row = header_start + 1
    if next_row < len(rows):
        if _is_string_row(rows[next_row], threshold=0.7) and not _is_data_row(
            rows, next_row, max_cols
        ):
            check_range = list(range(next_row + 1, min(next_row + 4, len(rows))))
            all_string_below = check_range and all(
                _is_string_row(rows[r], threshold=0.5)
                and not _is_data_row(rows, r, max_cols)
                for r in check_range
            )
            if not all_string_below:
                header_end = next_row

    sheet.header_row_end = header_end

    # Extract headers
    if header_end > header_start:
        headers = _merge_multi_level_headers(
            rows[header_start], rows[header_end], max_cols, sheet.merge_map,
            header_start,
        )
    else:
        headers = [
            v if v is not None else None for v in rows[header_start]
        ]

    # Normalise column names
    headers = _normalize_headers(headers, max_cols)
    sheet.headers = headers

    # Extract data rows (after header_end)
    data_start = header_end + 1
    data_rows = rows[data_start:]

    # Trim trailing all-None rows
    while data_rows and all(v is None for v in data_rows[-1]):
        data_rows.pop()

    # Post-header empty column pruning: remove columns that are 100% NULL in data rows
    if data_rows and headers:
        cols_to_keep = []
        for col_idx in range(len(headers)):
            null_count = sum(1 for r in data_rows if col_idx >= len(r) or r[col_idx] is None)
            if null_count < len(data_rows):  # at least one non-null data value
                cols_to_keep.append(col_idx)
        if len(cols_to_keep) < len(headers):
            pruned = len(headers) - len(cols_to_keep)
            if sheet.original_col_indices:
                sheet.original_col_indices = [sheet.original_col_indices[i] for i in cols_to_keep]
            headers = [headers[i] for i in cols_to_keep]
            data_rows = [[r[i] if i < len(r) else None for i in cols_to_keep] for r in data_rows]
            sheet.metadata["data_columns_pruned"] = pruned

    sheet.headers = headers
    sheet.rows = data_rows
    return warnings


def _strategy_string_dominant(
    rows: list[list], max_cols: int, merge_map: dict[tuple, object],
    min_fill_ratio: float = 0.5, min_string_ratio: float = 0.7,
) -> int | None:
    """Strategy 1: Find the first row that looks like a header (string-dominant)."""
    for idx, row in enumerate(rows):
        non_none = [v for v in row if v is not None]
        non_none_count = len(non_none)

        # Enough cells filled?
        if max_cols > 0 and non_none_count / max_cols < min_fill_ratio:
            continue

        # Mostly strings?
        string_count = sum(1 for v in non_none if isinstance(v, str))
        if non_none_count > 0 and string_count / non_none_count < min_string_ratio:
            continue

        # Skip title rows: ≤2 cells filled in a wide table
        if non_none_count <= 2 and max_cols > 2:
            continue

        # Skip title rows: single merged cell spanning ≥80%
        if _is_title_merge(idx, max_cols, merge_map):
            continue

        # Check rows below have data (≥3 rows with at least 1 non-None)
        data_rows_below = 0
        for check_idx in range(idx + 1, min(idx + 6, len(rows))):
            if any(v is not None for v in rows[check_idx]):
                data_rows_below += 1
        if data_rows_below < 3:
            # Relax: if there's at least 1 data row below, still accept
            if data_rows_below < 1:
                continue

        return idx

    return None


def _strategy_type_transition(rows: list[list], max_cols: int) -> int | None:
    """Strategy 2: Find header by detecting where cell types change between adjacent rows.

    Good for numeric/date headers like [Week, 1, 2, 3...] or [Date, 2025-01-01, ...]
    """
    for i in range(len(rows) - 1):
        row = rows[i]
        next_row = rows[i + 1]

        # Skip sparse rows
        non_none = [v for v in row if v is not None]
        next_non_none = [v for v in next_row if v is not None]
        if len(non_none) < max_cols * 0.3 or len(next_non_none) < max_cols * 0.3:
            continue

        # Classify types
        def type_sig(vals):
            return [type(v).__name__ for v in vals if v is not None]

        row_types = set(type_sig(row))
        next_types = set(type_sig(next_row))

        # Type transition: the types in this row differ significantly from next row
        # AND this row has at least one string (likely a label column)
        has_string = any(isinstance(v, str) for v in non_none)
        types_differ = row_types != next_types

        if has_string and types_differ and len(non_none) >= 3:
            # Verify there's data below (at least 2 more rows)
            data_below = sum(1 for j in range(i + 1, min(i + 5, len(rows)))
                            if any(v is not None for v in rows[j]))
            if data_below >= 2:
                return i

    return None


def _strategy_first_substantive(rows: list[list], max_cols: int) -> int | None:
    """Strategy 3: Fallback — use the first row with enough non-None cells as header."""
    for i, row in enumerate(rows):
        non_none = sum(1 for v in row if v is not None)
        if non_none >= max(2, max_cols * 0.3):
            data_below = sum(1 for j in range(i + 1, min(i + 4, len(rows)))
                            if any(v is not None for v in rows[j]))
            if data_below >= 1:
                return i
    return None


def _is_title_merge(
    row_idx: int, max_cols: int, merge_map: dict[tuple, object]
) -> bool:
    """Check if a single merged group spans ≥80% of columns in this row."""
    # Group columns by their merge value to find individual merge ranges
    value_to_cols: dict[str, set[int]] = {}
    for (r, c), val in merge_map.items():
        if r == row_idx and val is not None:
            key = str(val)
            value_to_cols.setdefault(key, set()).add(c)

    # A title row has ONE merge group spanning ≥80% of columns
    for cols in value_to_cols.values():
        if max_cols > 0 and len(cols) >= 0.8 * max_cols:
            return True
    return False


def _is_string_row(row: list, threshold: float = 0.7) -> bool:
    """Check if ≥threshold of non-None cells are strings."""
    non_none = [v for v in row if v is not None]
    if not non_none:
        return False
    string_count = sum(1 for v in non_none if isinstance(v, str))
    return string_count / len(non_none) >= threshold


def _is_data_row(rows: list[list], idx: int, max_cols: int) -> bool:
    """Heuristic: a row looks like data if it has numeric values."""
    if idx >= len(rows):
        return False
    row = rows[idx]
    non_none = [v for v in row if v is not None]
    if not non_none:
        return False
    numeric_count = sum(1 for v in non_none if isinstance(v, (int, float)))
    return numeric_count / len(non_none) > 0.3


def _merge_multi_level_headers(
    top_row: list, bottom_row: list, max_cols: int,
    merge_map: dict[tuple, object], header_start: int,
) -> list[str]:
    """Merge two header rows into combined names like 'Parent_Child'."""
    # Build a mapping of which top-level header covers each column
    # by checking horizontal merges in merge_map at header_start row
    parent_for_col: dict[int, str] = {}

    # First pass: direct values from top row
    for c in range(max_cols):
        val = top_row[c] if c < len(top_row) else None
        if val is not None:
            parent_for_col[c] = str(val).strip()

    # For merged cells in the top row, the merge_map will have the same value
    # for all cells in the merge range. We use that to propagate parent names.
    # Group consecutive columns with the same merge_map value in the top row
    for c in range(max_cols):
        key = (header_start, c)
        if key in merge_map and merge_map[key] is not None:
            parent_for_col[c] = str(merge_map[key]).strip()

    headers = []
    for c in range(max_cols):
        parent = parent_for_col.get(c)
        child = bottom_row[c] if c < len(bottom_row) else None

        if child is not None:
            child_str = str(child).strip()
        else:
            child_str = None

        if parent and child_str:
            # If parent == child (e.g. "ID" spanning both rows), just use parent
            if parent == child_str:
                headers.append(parent)
            else:
                headers.append(f"{parent}_{child_str}")
        elif parent:
            headers.append(parent)
        elif child_str:
            headers.append(child_str)
        else:
            headers.append(None)

    return headers


def _normalize_headers(headers: list, max_cols: int) -> list[str]:
    """Normalize: strip whitespace, fill blanks, deduplicate."""
    result: list[str] = []
    for i, h in enumerate(headers):
        if h is None or (isinstance(h, str) and h.strip() == ""):
            result.append(f"column_{i + 1}")
        else:
            result.append(str(h).strip().replace("\n", " "))

    # Deduplicate
    seen: dict[str, int] = {}
    final: list[str] = []
    for name in result:
        if name in seen:
            count = seen[name]
            new_name = f"{name}_{count}"
            seen[name] = count + 1
            # Handle edge case where new_name also exists
            while new_name in seen:
                count += 1
                new_name = f"{name}_{count}"
                seen[name] = count + 1
            seen[new_name] = 1
            final.append(new_name)
        else:
            seen[name] = 1
            final.append(name)

    return final


