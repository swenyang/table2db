# Skill: Excel to SQLite — Agent-Powered Conversion

## Purpose

Convert any Excel file (.xlsx/.xls) into a clean, queryable SQLite database. The agent reads the Excel, analyzes its structure through iterative exploration, then constructs the ideal database.

## When to Use

When a user uploads an Excel file and needs it converted to a structured database for querying, analysis, or integration with backend services.

## Prerequisites

- Python with `openpyxl`, `sqlite3` (built-in)
- Agent must be able to execute Python code and inspect results across multiple turns

## Agent Workflow

### Phase 1: Initial Reconnaissance

Read the workbook and get an overview of what we're working with.

```python
import openpyxl

wb = openpyxl.load_workbook("input.xlsx", data_only=True)
for ws in wb.worksheets:
    print(f"Sheet: {ws.title}, {ws.max_row} rows × {ws.max_column} cols, visible={ws.sheet_state == 'visible'}")
    print(f"  Merged cells: {len(ws.merged_cells.ranges)}")
```

**Decision point:** For each visible sheet, proceed to Phase 2.

### Phase 2: Explore Sheet Structure

For each sheet, read the first ~8 rows to understand the top structure:

```python
ws = wb["SheetName"]
for i, row in enumerate(ws.iter_rows(max_row=8, values_only=True)):
    non_none = [(j, v) for j, v in enumerate(row) if v is not None]
    fill_pct = len(non_none) / ws.max_column if ws.max_column else 0
    types = set(type(v).__name__ for _, v in non_none)
    print(f"  row {i}: fill={fill_pct:.0%} types={types} | {non_none[:6]}")
```

Then check the last few rows (to find totals/summaries):

```python
for i, row in enumerate(ws.iter_rows(min_row=max(1, ws.max_row - 3), values_only=True)):
    row_idx = max(0, ws.max_row - 4) + i
    non_none = [(j, v) for j, v in enumerate(row) if v is not None]
    print(f"  row {row_idx}: {non_none[:6]}")
```

**Decision points after initial exploration:**

1. **Where is the header?** Look for the first row that has:
   - High fill rate (>50% non-empty)
   - Mix of string values (column names)
   - Data rows following it
   
2. **Are there title/subtitle rows above the header?** Rows with very few cells filled, or a single merged cell spanning the width = title. Skip them.

3. **Are there multiple tables?** Look for:
   - Empty row gaps (≥2 consecutive empty rows) separating data blocks
   - Dramatically different column structures in different row ranges
   - Section headers (bold rows with just 1-2 cells) between data regions

4. **Do I need to see more rows?** If the first 8 rows are all title/header and I haven't seen data yet, read more:
   ```python
   for i, row in enumerate(ws.iter_rows(min_row=9, max_row=20, values_only=True)):
       ...
   ```

### Phase 3: Deep Exploration (if needed)

For complex sheets, explore specific regions:

**Check for bold/subtotal rows:**
```python
from openpyxl.styles import Font
for r in range(1, ws.max_row + 1):
    cell = ws.cell(r, 1)
    if cell.font and cell.font.bold:
        vals = [ws.cell(r, c).value for c in range(1, min(ws.max_column + 1, 6))]
        print(f"  BOLD row {r-1}: {vals}")
```

**Check for color-coded cells (Gantt charts, status matrices):**
```python
color_rows = set()
for r in range(1, min(ws.max_row + 1, 30)):
    for c in range(1, ws.max_column + 1):
        cell = ws.cell(r, c)
        if cell.value is None and cell.fill and cell.fill.start_color:
            rgb = cell.fill.start_color.rgb
            if isinstance(rgb, str) and rgb != "00000000":
                color_rows.add(r - 1)
                break
if color_rows:
    print(f"  Color-only rows (possible Gantt chart): {sorted(color_rows)[:10]}")
```

**Check for merged cell structure (multi-level headers):**
```python
for merge in ws.merged_cells.ranges:
    if merge.min_row <= 5:  # merges in header area
        val = ws.cell(merge.min_row, merge.min_col).value
        print(f"  Merge: {merge} = {val}")
```

**Look at a mid-sheet row to verify structure is consistent:**
```python
mid = ws.max_row // 2
row = [ws.cell(mid, c).value for c in range(1, ws.max_column + 1)]
non_none = [(j, v) for j, v in enumerate(row) if v is not None]
print(f"  Mid-row {mid}: {non_none[:6]}")
```

### Phase 4: Make Structural Decisions

Based on exploration, decide for each table region:

1. **Table name**: descriptive snake_case based on sheet name or section header
2. **Header row(s)**: which row(s) contain column names
   - Single row: use directly
   - Multi-row (merged parent + child): combine as "Parent_Child"
   - Numeric headers (Week 1, 2, 3...): name semantically ("Week 1", "Week 2", ...)
3. **Column names**: read from header row, clean up:
   - Strip whitespace and newlines
   - If header is a number/date, create semantic name
   - Skip empty separator columns
4. **Data range**: start row (after header) to end row (before totals/empty)
5. **Skip rows**: titles, subtitles, subtotals, empty rows, section headers
6. **Types**: determine from data values
   - All integers → INTEGER
   - Has decimals → REAL
   - Dates → TEXT (ISO format)
   - Everything else → TEXT

### Phase 5: Extract and Build Database

```python
import sqlite3
import datetime

conn = sqlite3.connect("output.db")

# For each table identified in Phase 4:
# Example: table from rows 3-50, headers in row 3, data rows 4-49, skip row 25 (subtotal)

headers = [ws.cell(header_row + 1, c + 1).value for c in col_indices]
# Clean headers: strip, handle None, deduplicate
headers = [str(h).strip() if h else f"column_{i+1}" for i, h in enumerate(headers)]

# Deduplicate
seen = {}
for i, h in enumerate(headers):
    if h in seen:
        seen[h] += 1
        headers[i] = f"{h}_{seen[h]}"
    else:
        seen[h] = 0

# Create table
col_defs = ", ".join(f'"{h}" {types[i]}' for i, h in enumerate(headers))
conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

# Insert data rows
for r in range(data_start, data_end + 1):
    if r in skip_rows:
        continue
    
    row_data = []
    for ci in col_indices:
        val = ws.cell(r + 1, ci + 1).value  # openpyxl is 1-indexed
        
        # Handle merged cells
        if val is None:
            for merge in ws.merged_cells.ranges:
                if merge.min_row <= r + 1 <= merge.max_row and merge.min_col <= ci + 1 <= merge.max_col:
                    val = ws.cell(merge.min_row, merge.min_col).value
                    break
        
        # Convert errors to NULL
        if isinstance(val, str) and val in ("#REF!", "#N/A", "#DIV/0!", "#VALUE!", "#NAME?", "#NULL!", "#NUM!"):
            val = None
        
        # Convert dates to ISO
        if isinstance(val, (datetime.datetime, datetime.date)):
            val = val.isoformat()
        
        row_data.append(val)
    
    # Skip if completely empty
    if all(v is None for v in row_data):
        continue
    
    placeholders = ", ".join(["?"] * len(row_data))
    conn.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})', tuple(row_data))

conn.commit()
```

### Phase 6: Verify

After building the DB, verify the result:

```python
# Check each table
for (tbl,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != '_meta'"):
    count = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
    cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{tbl}")')]
    sample = conn.execute(f'SELECT * FROM "{tbl}" LIMIT 3').fetchall()
    print(f"Table: {tbl}, {count} rows, cols={cols}")
    for row in sample:
        print(f"  {row}")
```

Cross-check against original Excel:
- Row count: does `DB rows + skipped rows (titles/subtotals) ≈ Excel rows`?
- Sample values: do first/last rows match?
- No all-NULL columns remaining?

## Rules (apply at every decision point)

1. **Every visible sheet → at least one table** (unless truly empty or chart-only)
2. **Column names must be semantic** — actual header text from Excel, cleaned up. NEVER use "column_1" if there is any text in the header area.
3. **Title/subtitle rows → skip** (e.g., "Monthly Report Q1 2025" in row 0)
4. **Annotation rows → skip** (rows with 1-2 cells that describe the data below but are NOT column headers)
5. **Subtotal/total rows → exclude from data** (look for: keywords "Total"/"Subtotal"/"合计"/"小计", bold formatting, values that sum the rows above)
6. **ALL actual data rows → include** — zero data loss is the goal
7. **Types must be correct**: numbers as INTEGER/REAL, text as TEXT, dates as ISO strings in TEXT columns
8. **Merged cells → expand** (fill the top-left value into all positions in the merged range)
9. **Multiple tables in one sheet → create separate DB tables** (separated by empty row gaps or different column structures)
10. **Empty separator columns → remove** (columns that are entirely NULL between data columns)
11. **Color-only cells** (Gantt charts, status matrices): note as color data, can optionally extract color hex values
12. **Template/form sheets with no data** → 0-row table preserving the column structure

## Common Patterns to Recognize

| Pattern | How to detect | How to handle |
|---------|--------------|---------------|
| Title + subtitle + header | First 1-2 rows sparse, row 3+ has dense strings | Skip rows 0-1, header at row 2+ |
| Multi-level headers | Merged cells in header area spanning columns | Combine parent + child: "Category_Subcategory" |
| Subtotal rows | Bold, contains "Total"/"Subtotal"/"合计", or values = sum of above | Exclude from data |
| Side-by-side tables | Two dense regions separated by empty columns | Split into two tables |
| Stacked tables | Two dense regions separated by empty rows | Split into two tables |
| Gantt chart / timeline | Rows where most cells are empty but have background colors | Extract as color data or skip |
| Data not starting at A1 | Top rows and/or left columns empty | Find the actual data region |
| Numbers stored as text | Column looks numeric but values are strings ("1234") | Set type as INTEGER/REAL, values will convert |

## Error Handling

- **openpyxl fails to open**: File may be corrupted, password-protected, or .xls format. Try xlrd for .xls.
- **Sheet has no detectable structure**: Create a table with all rows as-is, using row 0 as headers.
- **Merged cells create ambiguity**: Always use the top-left cell's value for all cells in the merge range.
- **Very large sheets (10000+ rows)**: Read in chunks, but still check first/last rows and a mid-point sample.
- **Formula cells with None values**: File may not have been saved in Excel. Note as a warning.
