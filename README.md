# table2db

Convert messy Excel files into clean, queryable SQLite databases — merged cells, multi-level headers, subtotals, mixed types, and all.

## Installation

```bash
pip install table2db
```

Requires Python 3.10+. Dependencies (`openpyxl`, `xlrd`) are installed automatically.

## Quick Start

### As a Library

```python
from table2db import TableConverter

converter = TableConverter()

# Use as a context manager — temp DB is auto-cleaned on exit
with converter.convert("sales_report.xlsx") as result:
    print(result.db_path)          # path to the SQLite file
    print(result.tables)           # list of TableInfo objects
    print(result.relationships)    # detected foreign keys
    print(result.warnings)         # any processing warnings

    # Query with your favorite SQLite tool
    import sqlite3
    conn = sqlite3.connect(result.db_path)
    rows = conn.execute("SELECT * FROM sheet1 LIMIT 10").fetchall()

# From a file-like object (e.g., FastAPI UploadFile)
import io
with open("data.xlsx", "rb") as f:
    stream = io.BytesIO(f.read())
with converter.convert(stream, file_name="data.xlsx") as result:
    print(result.db_path)
```

To persist the database to a specific path:

```python
from table2db import TableConverter, SqliteLoader

converter = TableConverter()
loader = SqliteLoader(output_path="output.db")
result = converter.convert("sales_report.xlsx", loader=loader)

# result.db_path == "output.db", file persists after the program exits
print(f"Created {len(result.tables)} tables")
```

### As a CLI

```bash
# Convert an Excel file to SQLite
table2db convert report.xlsx -o report.db

# Convert with a summary printed to stdout
table2db convert report.xlsx -o report.db --summary

# Describe an existing database (generates LLM-friendly Markdown)
table2db describe report.db

# Save the description to a file
table2db describe report.db -o report_summary.md
```

**CLI options for `convert`:**

| Flag | Description |
|------|-------------|
| `-o`, `--output` | Output SQLite path (default: `<input_name>.db` in current directory) |
| `--summary` | Print a Markdown summary after conversion |
| `--sample-rows N` | Number of sample rows in the summary (default: 3) |
| `--type-threshold F` | Type inference majority threshold (default: 0.8) |
| `--fk-threshold F` | FK detection confidence threshold (default: 0.8) |

**CLI options for `describe`:**

| Flag | Description |
|------|-------------|
| `-o`, `--output` | Output Markdown file path (default: stdout) |
| `--sample-rows N` | Number of sample rows to include (default: 3) |

## Features

| Category | Feature |
|----------|---------|
| **File Formats** | `.xlsx` / `.xlsm` (openpyxl), `.xls` (xlrd), `.csv` / `.tsv` (built-in csv) |
| **Multi-Sheet** | Each sheet → separate SQLite table; auto name normalization and conflict handling |
| **Multi-Table in One Sheet** | Auto-detects multiple tables via island detection (connected-component analysis); splits into separate DB tables |
| **Merged Cells** | Auto-detect and expand; top-left value fills all merged cells (labels and numeric subtotals) |
| **Header Detection** | Auto-locate header row (supports offset / non-A1 start); multi-level headers merged as `level1_level2` |
| **Subtotal Filtering** | Three-signal weighted detection (keyword + sum verification + style); built-in Chinese & English keywords, extensible |
| **Type Inference** | Majority vote (≥80%); INTEGER/REAL merged counting; supports `INTEGER`, `REAL`, `TEXT`, `DATE`; numbers-as-text auto-correction; bool → integer |
| **Error Values** | `#REF!`, `#N/A`, `#DIV/0!`, etc. → `NULL` |
| **Formulas** | Takes computed values; warns on uncalculated formulas |
| **Primary Key Inference** | Column name pattern + uniqueness + non-null detection |
| **Cross-Sheet FK** | Column name matching + value containment (≥90%), cardinality protection (≥10 distinct values); FK written to SQLite DDL + `_meta` table |
| **Hidden Rows/Cols/Sheets** | Hidden sheets skipped by default (configurable); hidden rows/cols data preserved but flagged in metadata |
| **Data Cleaning** | Auto-remove empty rows; deduplicate identical rows |
| **DB Summary** | Generates LLM-friendly Markdown with table structure, sample data, and column stats — no LLM needed |
| **Async Support** | `convert_async()` / `process_async()` for non-blocking FastAPI integration |
| **Pluggable Loaders** | `BaseLoader` ABC; default `SqliteLoader` with optional output path; bring your own DB backend |
| **Lifecycle Management** | Temp SQLite file + context manager for auto-cleanup |
| **Error Handling** | Full exception hierarchy: `FileReadError`, `NoDataError`, `UnsupportedFormatError`, `SchemaError` |
| **Observability** | Per-stage logging via Python `logging`; warnings collection during processing |

### Explicitly Not Supported

| Exclusion | Note |
|-----------|------|
| `.xlsb` format | Binary Excel; would require extra dependencies |
| Password-protected files | Raises `FileReadError` |

## Architecture

table2db processes files through a **6-stage pipeline**:

```
Input File (.xlsx / .xlsm / .xls / .csv / .tsv)
  │
  ├─ Stage 1: Raw Reading         — Parse cells, merged regions, hidden state
  ├─ Stage 2: Structure Detection  — Island detection, find headers, multi-table split
  ├─ Stage 3: Data Cleaning        — Strip subtotals, empty rows, deduplicate
  ├─ Stage 4: Type Inference       — Majority-vote column types, coerce values
  ├─ Stage 5: Relationship Inference — Detect PKs and cross-sheet FKs
  └─ Stage 6: Database Loading     — Create tables, insert data, add constraints
  │
  ▼
ConversionResult (SQLite DB + metadata + warnings)
```

Stages 1–5 are handled by `TableConverter.process()`, which returns an intermediate `WorkbookData` object. Stage 6 is handled by a pluggable `BaseLoader` implementation. Calling `TableConverter.convert()` runs all six stages end-to-end.

## Pluggable Loaders

The default loader writes to SQLite, but you can implement your own by subclassing `BaseLoader`:

### Using SqliteLoader

```python
from table2db import TableConverter, SqliteLoader

converter = TableConverter()
loader = SqliteLoader(output_path="my_data.db")
result = converter.convert("input.xlsx", loader=loader)
```

### Writing a Custom Loader

```python
from table2db import BaseLoader, ConversionResult, WorkbookData

class PostgresLoader(BaseLoader):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def load(self, wb: WorkbookData) -> ConversionResult:
        # 1. Connect to your database
        # 2. Create tables from wb.tables
        # 3. Insert data
        # 4. Return a ConversionResult
        ...

# Use with TableConverter
converter = TableConverter()
loader = PostgresLoader("postgresql://localhost/mydb")
result = converter.convert("input.xlsx", loader=loader)
```

### Two-Phase Usage (Process + Load Separately)

```python
converter = TableConverter()

# Stages 1-5 only: parse, detect structure, clean, infer types & relationships
workbook_data, warnings = converter.process("input.xlsx")

# Inspect intermediate results before loading
for sheet in workbook_data.sheets:
    print(f"{sheet.name}: {len(sheet.headers)} columns, {len(sheet.rows)} rows")

# Stage 6: load into SQLite (or any loader)
loader = SqliteLoader(output_path="output.db")
result = loader.load(workbook_data)
```

## Async Support

For non-blocking usage in FastAPI or other async frameworks:

```python
# Async support for FastAPI / asyncio
result = await converter.convert_async("data.xlsx")

wb, warnings = await converter.process_async("data.xlsx")
```

Both methods use `asyncio.to_thread()` internally to avoid blocking the event loop.

## DB Summary (LLM Context)

Generate a Markdown summary of the converted database — useful for providing schema context to LLMs:

```python
from table2db import TableConverter
from table2db.describe import generate_db_summary

with TableConverter().convert("data.xlsx") as result:
    summary = generate_db_summary(result, sample_rows=3)
    print(summary)
```

Or via the CLI:

```bash
table2db describe my_database.db --sample-rows 5
```

The summary includes table schemas, column types, sample rows, and basic column statistics.

## Configuration

All configuration is passed to the `TableConverter` constructor:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `subtotal_keywords` | `list[str] \| None` | `None` | Additional keywords for subtotal/total row detection. Built-in keywords (Chinese & English) are always active. |
| `type_threshold` | `float` | `0.8` | Minimum fraction of non-null values that must share a type for the column to be inferred as that type. Range: 0.0–1.0. |
| `skip_hidden_sheets` | `bool` | `True` | Whether to skip hidden sheets during processing. |
| `fk_confidence_threshold` | `float` | `0.8` | Minimum confidence for cross-sheet foreign key detection. Higher values reduce false positives. |
| `header_min_fill_ratio` | `float` | `0.5` | Min ratio of non-empty cells for header row detection. Lower values = more lenient. |
| `header_min_string_ratio` | `float` | `0.7` | Min ratio of string cells for header row detection. Lower values = more lenient. |

```python
converter = TableConverter(
    subtotal_keywords=["subtotal", "grand total"],
    type_threshold=0.7,
    skip_hidden_sheets=False,
    fk_confidence_threshold=0.9,
    header_min_fill_ratio=0.4,
    header_min_string_ratio=0.6,
)
```

## What It Handles

### Structural Complexity (fully handled)

Merged cells (labels & numeric) · multi-level headers · offset data regions (not starting at A1) · **multiple tables in one sheet** (island detection) · multi-sheet → multi-table · cross-sheet foreign keys · hidden rows, columns, and sheets.

### Data-Level Complexity (key focus)

Formula cells (computed values) · uncalculated formulas (with warnings) · Excel error values (`#REF!`, `#N/A`, etc.) · empty rows/columns interspersed in data · manual subtotal/total rows · mixed-type columns · numbers stored as text · inconsistent date formats · boolean values · implicit type coercion · duplicate rows.

### Formatting (ignored — does not affect data)

Data validation (dropdowns) · conditional formatting · named ranges · comments and notes · embedded objects (charts, images).

## API Reference

### `TableConverter`

The main entry point for converting Excel files.

```python
class TableConverter:
    def __init__(
        self,
        subtotal_keywords: list[str] | None = None,
        type_threshold: float = 0.8,
        skip_hidden_sheets: bool = True,
        fk_confidence_threshold: float = 0.8,
        header_min_fill_ratio: float = 0.5,
        header_min_string_ratio: float = 0.7,
    ): ...

    def process(
        self, file_path: str | BinaryIO, *, file_name: str | None = None
    ) -> tuple[WorkbookData, list[str]]:
        """Run stages 1-5. Returns (WorkbookData, warnings).
        Accepts a file path or file-like object (BytesIO).
        file_name is required for streams to detect format."""

    def convert(
        self, file_path: str | BinaryIO, *, file_name: str | None = None,
        loader: BaseLoader | None = None,
    ) -> ConversionResult:
        """Run the full 6-stage pipeline. Uses SqliteLoader by default."""

    async def convert_async(
        self, file_path: str | BinaryIO, *, file_name: str | None = None,
        loader: BaseLoader | None = None,
    ) -> ConversionResult:
        """Async version of convert(). Uses asyncio.to_thread()."""

    async def process_async(
        self, file_path: str | BinaryIO, *, file_name: str | None = None
    ) -> tuple[WorkbookData, list[str]]:
        """Async version of process(). Uses asyncio.to_thread()."""
```

### `ConversionResult`

Returned by `convert()` and loader `load()` methods. Supports context manager protocol for automatic cleanup.

```python
@dataclass
class ConversionResult:
    db_path: str                    # Path to the SQLite database
    tables: list[TableInfo]         # Table metadata
    relationships: list[ForeignKey] # Detected foreign keys
    warnings: list[str]             # Processing warnings
    metadata: dict                  # Additional metadata

    def cleanup(self) -> None:
        """Delete the database file (for temp DBs)."""

    def __enter__(self) -> ConversionResult: ...
    def __exit__(self, *args) -> None: ...
```

### `SqliteLoader`

The built-in loader that writes to a SQLite database.

```python
class SqliteLoader(BaseLoader):
    def __init__(self, output_path: str | None = None):
        """If output_path is None, creates a temporary file."""

    def load(self, wb: WorkbookData) -> ConversionResult: ...
```

### `BaseLoader`

Abstract base class for implementing custom loaders.

```python
class BaseLoader(ABC):
    @abstractmethod
    def load(self, wb: WorkbookData) -> ConversionResult: ...
```

### Key Data Classes

- **`WorkbookData`** — Intermediate representation of the parsed workbook (stages 1–5 output).
- **`TableInfo`** — Metadata for a single converted table (name, columns, row count, `confidence` float from island detection — 1.0 for single-table sheets).
- **`ForeignKey`** — A detected cross-sheet foreign key relationship.

### Exceptions

All exceptions inherit from `ExcelToDbError`:

| Exception | When Raised |
|-----------|-------------|
| `ExcelToDbError` | Base exception for all library errors |
| `FileReadError` | File not found, permission denied, password-protected, or corrupt |
| `NoDataError` | Sheet or workbook contains no usable data |
| `UnsupportedFormatError` | File format is not `.xlsx`, `.xlsm`, `.xls`, `.csv`, or `.tsv` |
| `SchemaError` | Cannot construct a valid schema (e.g., no columns detected) |

## Development

```bash
git clone https://github.com/swenyang/table2db.git
cd table2db
pip install -e ".[dev]"
pytest tests/ -v
```

### Test Fixtures & Outputs

Test fixtures (Excel/CSV/TSV) are auto-generated when you run `pytest` for the first time. To manually regenerate:

```bash
# Regenerate test fixture files (tests/fixtures/)
python tests/generate_fixtures.py

# Regenerate output DBs and Markdown summaries (tests/output_dbs/)
python tests/generate_outputs.py
```

Both directories are in `.gitignore` since they are derived artifacts.

### Project Structure

```
table2db/                          # project root
├── pyproject.toml
├── README.md
├── table2db/                      # package
│   ├── __init__.py                # Public API exports
│   ├── converter.py               # TableConverter
│   ├── models.py                  # Data classes (WorkbookData, TableInfo, etc.)
│   ├── errors.py                  # Exception hierarchy
│   ├── loaders/
│   │   ├── base.py                # BaseLoader ABC
│   │   └── sqlite_loader.py       # SqliteLoader
│   ├── pipeline/
│   │   ├── reader.py              # Stage 1: Raw Reading
│   │   ├── structure.py           # Stage 2: Structure Detection
│   │   ├── cleaner.py             # Stage 3: Data Cleaning
│   │   ├── typer.py               # Stage 4: Type Inference
│   │   ├── relator.py             # Stage 5: Relationship Inference
│   │   └── island_detector.py     # Multi-table detection
│   ├── describe.py                # DB summary generation
│   └── cli.py                     # CLI entry point
└── tests/
    ├── conftest.py
    ├── generate_fixtures.py       # Auto-generates test fixtures
    └── test_*.py
```

## License

MIT
