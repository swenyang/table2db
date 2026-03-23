# table2db Design Document

## Overview

A Python library that automatically parses arbitrary Excel files (`.xlsx` / `.xlsm` / `.xls`) and CSV/TSV files into well-structured, reliable SQLite databases. Handles various complex scenarios commonly found in Excel (merged cells, multi-level headers, subtotal rows, mixed types, error values, etc.), providing clean data for backend services to query.

**Scenario**: User uploads any Excel/CSV → System automatically analyzes structure and loads into database → Returns a temporary SQLite DB → Cleanup after session ends
**Integration**: As a standalone Python library, backend (FastAPI) developers integrate and call it on their own

---

## Feature Overview

### ✅ v1 Supported

| Category | Feature |
|----------|---------|
| **File Formats** | `.xlsx` / `.xlsm` (openpyxl), `.xls` (compatibility, xlrd), `.csv` / `.tsv` (built-in csv with automatic delimiter sniffing and encoding detection) |
| **Stream Input** | Accepts both file paths (`str`) and file-like objects (`BinaryIO` / `BytesIO` / `UploadFile.file`); `file_name` parameter required for streams to detect format |
| **Multi-Sheet** | **Each Sheet → one independent SQLite table**; automatic table name normalization, handling name conflicts |
| **Merged Cells** | Automatically detect and expand; top-left cell value fills all merged cells |
| **Header Detection** | Automatically locate header row (supports offset / non-A1 start); multi-level headers merged into `level1_level2` |
| **Subtotal/Total Filtering** | Three-signal weighted detection (keywords + sum verification + style); built-in Chinese & English keywords, extensible |
| **Type Inference** | Majority vote (≥80%); supports INTEGER / REAL / TEXT / DATE; numbers stored as text auto-corrected; boolean → integer |
| **Error Value Handling** | `#REF!`, `#N/A`, `#DIV/0!`, etc. all → NULL |
| **Formula Handling** | Take computed result value; uncomputed formulas detected with WARNING |
| **Primary Key Inference** | Column name pattern + uniqueness + non-null detection |
| **Cross-Sheet Foreign Key Inference** | Column name matching + value domain containment verification (≥90%), cardinality protection (≥10 distinct) |
| **Hidden Rows/Columns/Sheets** | Hidden Sheets skipped by default (configurable); hidden rows/columns retain data but flagged in metadata |
| **Multi-Table in One Sheet** | Auto-detects multiple tables via island detection (`island_detector.detect_table_islands()`, connected-component analysis); splits into separate DB tables |
| **Data Cleaning** | Automatically remove empty rows, deduplicate fully duplicate rows |
| **DB Summary** | Independent module, generates LLM-friendly Markdown (table structure + sample data + column-level statistics) |
| **Async Support** | `convert_async()` / `process_async()` for non-blocking FastAPI integration via `asyncio.to_thread()` |
| **Pluggable Loaders** | `BaseLoader` ABC; default `SqliteLoader` with optional output path; bring your own DB backend |
| **Lifecycle Management** | Temporary SQLite file + context manager for automatic cleanup |
| **Error Handling** | Complete exception hierarchy (FileReadError / NoDataError / UnsupportedFormatError / SchemaError) |
| **Observability** | Per-Stage logging (Python logging); processing warnings collected |

### ❌ v1 Explicitly Not Supported

| Exclusion | Description |
|-----------|-------------|
| `.xlsb` format | Binary Excel, requires additional dependencies |
| Password-protected files | Raises `FileReadError`, cannot decrypt and read |

---

## Architecture: Pipeline

The parsing process is split into 6 ordered Stages, each doing one thing, consuming and producing a unified intermediate data structure `WorkbookData`.

```
Excel File / CSV / TSV / Stream (BytesIO)
  → Stage 1: Raw Reading (reader)          — Parse cells, merged regions, hidden state; CSV/TSV delimiter sniffing & encoding detection
  → Stage 2: Structure Detection (structure) — Island detection (detect_table_islands), find headers, multi-table split
  → Stage 3: Data Cleaning (cleaner)
  → Stage 4: Type Inference (typer)
  → Stage 5: Relationship Inference (relator)
  → Stage 6: Database Loading (loader)
  → ConversionResult (SQLite DB + metadata)
```

**Advantages**: Each stage can be independently tested/debugged/replaced; clear responsibilities; easy to extend with new Stages.

**Logging**: Uses the Python `logging` module, each Stage has its own logger (e.g., `table2db.pipeline.reader`). Key points log INFO (entering/exiting Stage, row count changes), exceptional situations log WARNING. Callers can control the level via the standard `logging.getLogger("table2db")`.

---

## Error Handling

Defines an exception hierarchy, all exceptions inherit from `ExcelToDbError`:

```python
class ExcelToDbError(Exception): ...

class FileReadError(ExcelToDbError):
    """File cannot be read: corrupted, password-protected, non-Excel format, file not found"""

class NoDataError(ExcelToDbError):
    """File is readable but contains no valid data: all Sheets are empty or have no data after cleaning"""

class UnsupportedFormatError(ExcelToDbError):
    """Unsupported file format (e.g., .csv mistakenly uploaded, .xlsb, etc.)"""

class SchemaError(ExcelToDbError):
    """Cannot infer a valid table structure (e.g., unable to locate headers)"""
```

**Principle**: Recoverable issues are logged as warnings (e.g., single error value → NULL); unrecoverable issues raise exceptions.

---

## Core Data Model

```python
@dataclass
class CellCoord:
    row: int
    col: int

@dataclass
class ForeignKey:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    confidence: float  # 0.0 ~ 1.0

@dataclass
class SheetData:
    name: str                              # Sheet name
    header_row_start: int                  # Header start row number
    header_row_end: int                    # Header end row number (equals start for single-row headers)
    headers: list[str]                     # Column name list (deduplicated, normalized; multi-level headers merged as "level1_level2")
    rows: list[list[Any]]                  # 2D data (excluding headers)
    column_types: dict[str, str]           # Column name → inferred type (TEXT/INTEGER/REAL/DATE)
    primary_key: str | None                # Inferred primary key column name (if any)
    excluded_rows: list[int]               # Filtered row numbers (subtotal/total rows, etc.)
    merge_map: dict[tuple[CellCoord, CellCoord], Any]  # Merged cell mapping (populated in Stage 1, consumed in Stage 2 for header parsing, retained only as metadata afterwards)
    metadata: dict                         # Additional info (original row count, error statistics, hidden row/column info, etc.)

@dataclass
class WorkbookData:
    source_file: str
    sheets: list[SheetData]
    relationships: list[ForeignKey]        # Inferred cross-table relationships

@dataclass
class TableInfo:
    name: str
    columns: list[dict]  # [{"name": str, "type": str}, ...]
    row_count: int
    source_sheet: str
    primary_key: str | None
    confidence: float  # Island detection confidence (1.0 for single-table sheets)

@dataclass
class ConversionResult:
    db_path: str                           # SQLite file path
    tables: list[TableInfo]                # Table structure info
    relationships: list[ForeignKey]        # Foreign key relationships
    warnings: list[str]                    # Processing warnings
    metadata: dict                         # Statistics

    def cleanup(self): ...                 # Delete the temporary .db file
    def __enter__(self): ...               # Context manager support
    def __exit__(self, *args): ...
```

---

## Detailed Stage Design

### Stage 1 — Raw Reading (`pipeline/reader.py`)

**Input**: File path (`str`) or file-like object (`BinaryIO` / `BytesIO`), with optional `file_name` parameter for stream inputs  
**Output**: Initialized `WorkbookData` (raw data + merged cell info)  
**Exceptions**: `FileReadError` (file corrupted/password-protected/not found), `UnsupportedFormatError` (unsupported format)

- **Input types**: Accepts `Union[str, BinaryIO]`. When a stream (e.g., `BytesIO`, `UploadFile.file`) is provided, the `file_name` parameter is required to determine the file format from the extension.
- Uses **openpyxl** (`data_only=True`) to read `.xlsx` / `.xlsm`, taking the computed result of formulas rather than formula text
- **openpyxl limitation**: `data_only=True` returns `None` for files not previously opened/saved in Excel (e.g., script-generated xlsx). Detection strategy: if > 50% of values in a column are `None` but corresponding formulas exist (checked via a secondary read with `data_only=False`), log a WARNING advising the user "This file contains uncomputed formulas; it is recommended to open and save it in Excel before re-uploading"
- `.xls` format falls back to **xlrd**
- **CSV/TSV support**: `.csv` and `.tsv` files are read using Python's built-in `csv` module. Includes automatic **delimiter sniffing** (via `csv.Sniffer`) to detect comma, tab, semicolon, or pipe delimiters. **Encoding detection** tries UTF-8 first, then falls back to locale-aware detection for other encodings (e.g., GBK, Latin-1).
- Read merged cell info → `merge_map`: fill the top-left cell value into all cells within the merged region. Applies to two common scenarios:
  - **Label merging**: e.g., "North China" spanning 3 rows → each row filled with "North China"
  - **Number merging**: e.g., subtotal 600 spanning 3 rows → each row filled with 600 (redundant but no data loss; Stage 3 can filter later)
- Skip hidden Sheets (default behavior, can be disabled via `skip_hidden_sheets=False`)
- **Hidden rows/columns**: Read but flag `hidden_rows` and `hidden_cols` lists in `metadata`; data retained by default (hidden ≠ invalid)
- Excel error values (`#REF!`, `#N/A`, `#DIV/0!`, `#VALUE!`, `#NAME?`, `#NULL!`) are uniformly converted to `None`

### Stage 2 — Structure Detection (`pipeline/structure.py`)

**Input**: `WorkbookData` containing raw data  
**Output**: `WorkbookData` with headers located and column names normalized  
**Exceptions**: `SchemaError` (unable to locate any headers)

**Header location** heuristic rules (scanning from row 1):
1. Non-empty cells in the row account for > 50% (relative to the Sheet's maximum used column count) — configurable via `header_min_fill_ratio` (default 0.5)
2. ≥ 70% of value types are strings — configurable via `header_min_string_ratio` (default 0.7)
3. ≥ 3 consecutive rows with data below (at least 1 non-empty cell per row)
4. Skip title rows (only 1-2 cells have values, or merged cells span ≥ 80% of columns across the entire row)

**Data region boundaries**: From below the header row to the last non-empty row (ignoring trailing consecutive empty rows)

**Column name normalization**:
- Trim leading/trailing whitespace, remove newline characters
- Duplicate column names get suffix `_1`, `_2`
- Empty column names use `column_N` as placeholder

**Multi-level header handling**:
- Detect whether headers span multiple rows (if the row immediately following the header has ≥ 70% string cells and doesn't look like a data row)
- Merge into `"level1_level2"` format
- `header_row_start` records the first row, `header_row_end` records the last row

**Consuming `merge_map`**: Uses merged cell information to assist in identifying cross-column parent headers in multi-level headers. After Stage 2, `merge_map` is retained only as metadata.

**Multiple tables in one Sheet**: Supported via the `island_detector` module (`table2db.pipeline.island_detector`). The `detect_table_islands()` function performs connected-component analysis on the sheet's non-empty cell grid to identify separate data regions (islands). Each detected island is extracted as a separate table. `TableInfo.confidence` carries the island detection confidence score (1.0 for single-table sheets, lower values for ambiguous splits). If only one island is found, processing proceeds as normal.

**Empty Sheet handling**: If a Sheet cannot locate valid headers or data rows, skip that Sheet and log a WARNING.

### Stage 3 — Data Cleaning (`pipeline/cleaner.py`)

**Input**: `WorkbookData` with structure detected  
**Output**: Cleaned `WorkbookData` (summary rows, empty rows, duplicate rows filtered)

**Subtotal/total row detection** (multi-signal weighted scoring, default weights and thresholds):
1. **Keyword matching** (weight 0.5): Text cells in the row contain `合计|小计|总计|总价|sum|total|subtotal|grand total`
   - Built-in Chinese & English keywords, extensible by developers via the `subtotal_keywords` parameter
   - Case-insensitive, supports spaces within keywords (e.g., `"合 计"`)
2. **Structural signal** (weight 0.3): Numeric column values in the row equal the sum of the continuous data region above
3. **Style signal** (weight 0.2): Row formatting differs from data rows (bold, background color, etc., read via openpyxl styles)

Weighted score ≥ 0.5 determines the row as a summary row and filters it. Filtered row numbers are recorded in `excluded_rows`.

**Multi-language support**: Built-in Chinese + English keywords cover mainstream scenarios by default; structural signals serve as a language-agnostic fallback.

**Other cleaning**:
- Entire row is empty → delete
- Fully duplicate rows → deduplicate (keep first occurrence)

**Empty table after cleaning**: If a Sheet has no data rows after cleaning, skip that Sheet and log a WARNING `"Sheet '{name}' has no data rows after cleaning, skipped"`.

### Stage 4 — Type Inference (`pipeline/typer.py`)

**Input**: Cleaned `WorkbookData`  
**Output**: `WorkbookData` with type information

For each column, sample non-null values and classify into SQLite-compatible types: `INTEGER`, `REAL`, `TEXT`, `DATE`

> Note: Boolean values in Excel (TRUE/FALSE) are mapped to `INTEGER` (1/0), not treated as a separate type.

**Majority vote rules**:
- **Merged numeric count**: INTEGER and REAL are merged into a "numeric" category for threshold evaluation. If the numeric category accounts for ≥ 80% (configurable via `type_threshold`), the column is determined to be numeric; if any REAL values exist, choose REAL, otherwise choose INTEGER.
- Non-numeric types (TEXT, DATE) are calculated independently; ≥ 80% adopts that type.
- If none meet the threshold, fallback to `TEXT`.

> Design rationale: For a string like `"51.0"`, after `float("51.0")`, since `51.0 == int(51.0)`, it would be classified as INTEGER, while `"12.75"` would be classified as REAL. If counts are not merged, REAL and INTEGER in the same column would dilute each other, causing what should be a numeric column to fallback to TEXT.

**Special handling**:
- **Numbers stored as text**: Value is a string but `float(val)` doesn't raise an error → treated as a number
- **Date detection**: openpyxl's `datetime` type + common date string pattern matching (`YYYY-MM-DD`, `YYYY/MM/DD`, `DD/MM/YYYY`, etc.)
- **Boolean values**: Python `bool` or strings `"TRUE"/"FALSE"` → converted to `INTEGER` 1/0
- **Type-mismatched values**: Attempt conversion; if conversion fails, set to `None`

### Stage 5 — Relationship Inference (`pipeline/relator.py`)

**Input**: `WorkbookData` with type information  
**Output**: `WorkbookData` with `relationships` and `primary_key`

#### 5a. Primary Key Inference

Evaluate each column of each table as a primary key candidate:
- Column name matches pattern: `id`, `*_id`, `*_no`, `*_code` (weight +0.3)
- Values are 100% unique and 100% non-null (required condition)
- Type is `INTEGER` or `TEXT` (excludes REAL/DATE)
- If multiple candidate columns exist, take the first one named `id`; otherwise take the leftmost candidate column

#### 5b. Foreign Key Inference

**Column name matching**:
- Identical column names appearing in two tables (e.g., `customer_id`)
- Naming pattern matching (table A has an `id` column, table B has an `a_id` column, where `a` is table A's name or abbreviation)

**Value domain validation**:
- The value set of that column in table B ⊆ the value set of the candidate primary key column in table A
- Allows ≥ 90% containment rate (tolerates a small amount of dirty data)
- **Cardinality protection**: The candidate primary key column must have ≥ 10 distinct values (avoids false matches on small integer columns, e.g., rating columns with values 1-5)
- Excludes cases where both tables have the same column name but both are primary keys (that indicates same-name, non-related columns)

**Output**: `ForeignKey(from_table, from_col, to_table, to_col, confidence)` list  
**Default threshold**: Only relationships with `confidence ≥ 0.8` establish actual SQL foreign key constraints. Configurable via `fk_confidence_threshold`.

### Stage 6 — Database Loading (`pipeline/loader.py`)

**Input**: Complete `WorkbookData`  
**Output**: `ConversionResult` (containing SQLite DB path)

- Creates SQLite file under `tempfile.mkdtemp()`
- **Table name normalization**: Sheet name → remove special characters → convert to snake_case → deduplicate (duplicate names get suffix `_1`, `_2`)
- **Identifier safety**: All table names and column names are wrapped in SQLite double quotes (`"table_name"`) in DDL, preventing syntax errors from special characters or reserved words. Additionally filters control characters and NUL bytes.
- Generates `CREATE TABLE` DDL based on `column_types`
- If `primary_key` exists, adds `PRIMARY KEY` constraint
- **Foreign key constraints written to DDL**: `FOREIGN KEY ("col") REFERENCES "other_table" ("col")`. Referenced tables are created first to ensure correct reference order. Also enables `PRAGMA foreign_keys = ON`.
- Uses `executemany` for batch data insertion
- Creates metadata table `_meta`: original filename, source Sheet for each table, row count, column count, type inference statistics, **foreign key relationships and confidence scores**
- Table names in `ConversionResult.relationships` are unified to the normalized names (consistent with DDL)

---

## DB Summary Module (`describe.py`)

An optional module independent of the pipeline, used to generate LLM-friendly database descriptions.

**Input**: `ConversionResult`  
**Output**: Markdown-formatted string

**Included content**:
- Table descriptions (table name, column names + types, row count, primary/foreign keys)
- First N rows of sample data per table (default 3 rows, Markdown table format)
- Column-level statistics:
  - Numeric columns: min / max / avg / null rate / distinct count
  - Text columns: null rate / distinct count / top 3 values and frequencies
- Cross-table relationship descriptions (from.col → to.col)

```python
from table2db.describe import generate_db_summary

summary: str = generate_db_summary(result, sample_rows=3)
# Returns Markdown-formatted text, can be used directly as LLM context
```

**Testing approach**: `tests/test_describe.py` constructs in-memory SheetData → load_to_sqlite → generate_db_summary, verifying the output Markdown contains:
- Table names, column names, column types
- Sample data values
- Numeric column statistics (min/max)
- Null percentages
- FK relationship symbol (`→`)
- sample_rows parameter takes effect

**FastAPI integration example**:
```python
@app.post("/upload")
async def upload(file: UploadFile):
    converter = TableConverter()
    result = converter.convert(saved_path)
    summary = generate_db_summary(result, sample_rows=5)
    # summary can be sent directly to LLM as database schema context
    return {"tables": [t.name for t in result.tables], "summary": summary}
```

---

## Public API

```python
from table2db import TableConverter
from table2db.errors import FileReadError, NoDataError

# Basic usage
converter = TableConverter()
result = converter.convert("data.xlsx")

result.db_path          # str: SQLite file path
result.tables           # list[TableInfo]
result.relationships    # list[ForeignKey]
result.warnings         # list[str]
result.metadata         # dict

# From a file-like object (e.g., FastAPI UploadFile)
import io
with open("data.xlsx", "rb") as f:
    stream = io.BytesIO(f.read())
result = converter.convert(stream, file_name="data.xlsx")

# Two-phase usage: stages 1-5 only, then load separately
workbook_data, warnings = converter.process("data.xlsx")
# Inspect intermediate results before loading
for sheet in workbook_data.sheets:
    print(f"{sheet.name}: {len(sheet.headers)} columns, {len(sheet.rows)} rows")

# Async support for FastAPI / asyncio
result = await converter.convert_async("data.xlsx")
wb, warnings = await converter.process_async("data.xlsx")

# Querying
import sqlite3
conn = sqlite3.connect(result.db_path)
rows = conn.execute("SELECT * FROM orders LIMIT 10").fetchall()

# Cleanup
result.cleanup()

# Context manager
with converter.convert("data.xlsx") as result:
    conn = sqlite3.connect(result.db_path)
    # ...
# Automatic cleanup on exit

# Error handling
try:
    result = converter.convert("bad_file.xlsx")
except FileReadError as e:
    print(f"File cannot be read: {e}")
except NoDataError as e:
    print(f"File has no valid data: {e}")

# Configuration options
converter = TableConverter(
    subtotal_keywords=["合计", "Total", ...],  # Extend subtotal keywords
    type_threshold=0.8,                         # Type majority vote threshold (default 0.8)
    skip_hidden_sheets=True,                    # Whether to skip hidden Sheets (default True)
    fk_confidence_threshold=0.8,                # Foreign key confidence threshold (default 0.8)
    header_min_fill_ratio=0.5,                  # Min non-empty cell ratio for header detection (default 0.5)
    header_min_string_ratio=0.7,                # Min string cell ratio for header detection (default 0.7)
)
```

---

## Project Structure

```
table2db/                          # project root
├── pyproject.toml
├── README.md
├── table2db/                      # package
│   ├── __init__.py                # Exports TableConverter, ConversionResult
│   ├── converter.py               # TableConverter main class, orchestrates pipeline
│   ├── models.py                  # Data models
│   ├── errors.py                  # Exception hierarchy
│   ├── cli.py                     # CLI entry point
│   ├── describe.py                # DB summary generation
│   ├── loaders/
│   │   ├── base.py                # BaseLoader ABC
│   │   └── sqlite_loader.py       # SqliteLoader
│   └── pipeline/
│       ├── reader.py              # Stage 1: Raw Reading
│       ├── structure.py           # Stage 2: Structure Detection
│       ├── island_detector.py     # Multi-table detection
│       ├── cleaner.py             # Stage 3: Data Cleaning
│       ├── typer.py               # Stage 4: Type Inference
│       ├── relator.py             # Stage 5: Relationship Inference
│       └── loader.py              # Stage 6: Database Loading (thin wrapper)
└── tests/
    ├── conftest.py
    ├── generate_fixtures.py       # Auto-generates test fixtures
    ├── generate_outputs.py        # Generates .db + _summary.md for all fixtures
    └── test_*.py                  # Per-stage + integration tests
```

## Dependencies

- `openpyxl` — reads `.xlsx` (merged cells, styles, formula computed values)
- `xlrd` — reads `.xls` (legacy format fallback)
- `sqlite3` — database loading (Python built-in)
- `tempfile` — temporary file management (Python built-in)
- `logging` — logging (Python built-in)
- `pytest` — testing framework (dev dependency)

---

## Testing Strategy

### Programmatically Constructed Fixtures (Primary)

Uses `tests/generate_fixtures.py` to programmatically generate test Excel files via openpyxl, precisely controlling each edge case:

| Fixture File | Verification Scenario |
|---|---|
| `simple.xlsx` | Baseline: clean table, verifies normal flow |
| `merged_cells.xlsx` | Merged cells (header merging, data region merging) |
| `multi_header.xlsx` | Multi-level headers (2-3 row nesting) |
| `subtotals.xlsx` | Subtotal/total/grand total row filtering (Chinese & English) |
| `mixed_types.xlsx` | Mixed types in the same column (numbers + text + nulls) |
| `error_values.xlsx` | Excel error values (#REF!, #N/A, etc.) |
| `offset_table.xlsx` | Data does not start from A1 |
| `empty_gaps.xlsx` | Empty rows/columns interspersed in data |
| `multi_sheet_fk.xlsx` | Multi-Sheet + cross-table foreign key relationships |
| `number_as_text.xlsx` | Numbers stored as text format |
| `dates_mixed.xlsx` | Multiple date formats mixed |
| `real_world_dirty.xlsx` | Comprehensive: multiple issues combined |
| `hidden_rows_cols.xlsx` | Hidden rows and hidden columns |
| `empty_after_clean.xlsx` | No data after cleaning (all summary rows) |
| `duplicate_sheet_names.xlsx` | Multiple Sheet names conflict after normalization |

### Test Layering

- **Unit tests**: Each Stage tested independently (input → output assertion)
- **Integration tests**: Full pipeline end-to-end (Excel → SQLite → query verification)
- **Exception tests**: Corrupted files, empty files, non-Excel files → correct exceptions raised
- **Regression tests**: Real-world files do not crash

---

## Excel Complex Scenario Handling Principles

**Structural level** (comprehensive handling): Merged cells, multi-level headers, data offset (not starting from A1), multi-Sheet to multi-table, cross-Sheet foreign keys, hidden rows/columns/Sheets

**Data level** (focused handling): Formula cells (take computed values), uncomputed formulas (warning), error values, interspersed empty rows/columns, manual subtotal/total rows, mixed types, numbers stored as text, inconsistent date formats, boolean values, implicit type conversion, duplicate rows

**Format level** (ignored, does not affect data): Data validation (dropdowns), conditional formatting, named ranges, comments/annotations, embedded objects
