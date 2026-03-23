"""Island detector — find multiple tables in a single sheet via connected-component analysis."""
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class TableRegion:
    """A detected table region within a sheet."""
    row_start: int  # 0-indexed, inclusive
    row_end: int    # 0-indexed, exclusive
    col_start: int  # 0-indexed, inclusive
    col_end: int    # 0-indexed, exclusive
    confidence: float  # 0.0 ~ 1.0


def detect_table_islands(
    rows: list[list], min_rows: int = 2, min_cols: int = 2
) -> list[TableRegion]:
    """Detect separate table regions in a 2D grid of cells.

    Algorithm:
    1. Build a boolean grid: True if cell is non-None
    2. Project non-empty cells onto rows and columns
    3. Find contiguous row ranges where at least one cell is non-empty
    4. Split on gaps of >= 2 consecutive fully-empty rows
    5. Within each row band, find column extents
    6. Filter out regions smaller than min_rows x min_cols
    7. Assign confidence based on density (non-empty cells / total cells in bbox)
    """
    if not rows:
        return []

    max_cols = max(len(r) for r in rows) if rows else 0
    if max_cols == 0:
        return []

    # Pad rows to uniform width
    grid = []
    for r in rows:
        padded = r + [None] * (max_cols - len(r))
        grid.append([v is not None for v in padded])

    num_rows = len(grid)

    # Step 1: Find row bands separated by >= 2 consecutive empty rows
    row_has_data = [any(grid[r]) for r in range(num_rows)]

    bands: list[tuple[int, int]] = []  # (start_row, end_row) — end is exclusive
    band_start = None
    empty_count = 0

    for r in range(num_rows):
        if row_has_data[r]:
            if band_start is None:
                band_start = r
            empty_count = 0
        else:
            empty_count += 1
            if empty_count >= 2 and band_start is not None:
                # End the current band at the row before empty streak started
                band_end = r - empty_count + 1
                if band_end > band_start:
                    bands.append((band_start, band_end))
                band_start = None

    # Close last band
    if band_start is not None:
        band_end = num_rows
        # Trim trailing empty rows
        while band_end > band_start and not row_has_data[band_end - 1]:
            band_end -= 1
        if band_end > band_start:
            bands.append((band_start, band_end))

    # Step 2: For each band, find column extent and build regions
    regions = []
    for band_start, band_end in bands:
        col_min = max_cols
        col_max = 0
        for r in range(band_start, band_end):
            for c in range(max_cols):
                if grid[r][c]:
                    col_min = min(col_min, c)
                    col_max = max(col_max, c)

        if col_max < col_min:
            continue

        row_count = band_end - band_start
        col_count = col_max - col_min + 1

        if row_count < min_rows or col_count < min_cols:
            continue

        # Compute density for confidence
        filled = sum(
            1 for r in range(band_start, band_end)
            for c in range(col_min, col_max + 1)
            if grid[r][c]
        )
        total = row_count * col_count
        density = filled / total if total > 0 else 0
        confidence = min(1.0, density * 1.2)  # slight boost, cap at 1.0

        regions.append(TableRegion(
            row_start=band_start,
            row_end=band_end,
            col_start=col_min,
            col_end=col_max + 1,  # exclusive
            confidence=round(confidence, 2),
        ))

    return regions
