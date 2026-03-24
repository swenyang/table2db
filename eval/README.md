# table2db Evaluation Framework

## Overview

This evaluation framework measures the quality of table2db's Excel-to-SQLite conversion by comparing script outputs against **golden standard databases** — manually crafted ideal conversions for 151 real-world Excel files from the [OpenAI gdpval dataset](https://huggingface.co/datasets/openai/gdpval).

## Directory Structure

```
eval/
├── README.md                  # This file
├── scorer.py                  # Evaluation scoring system
├── golden_builder.py          # Helper for creating golden DBs
├── source_files/
│   ├── references/            # 86 reference Excel files (input data)
│   └── deliverables/          # 65 deliverable Excel files (task outputs)
└── golden_dbs/
    ├── references/            # 86 golden standard SQLite DBs
    └── deliverables/          # 65 golden standard SQLite DBs
```

## Quick Start

```bash
# 1. Generate script outputs from the source Excel files
python -c "
import os, shutil
from table2db import TableConverter
converter = TableConverter()
for subdir in ['references', 'deliverables']:
    src_dir = f'eval/source_files/{subdir}'
    out_dir = f'my_outputs/{subdir}'
    os.makedirs(out_dir, exist_ok=True)
    for f in os.listdir(src_dir):
        if not f.endswith(('.xlsx', '.xls')):
            continue
        try:
            result = converter.convert(os.path.join(src_dir, f))
            base = os.path.splitext(f)[0].replace(' ', '_')
            shutil.copy2(result.db_path, os.path.join(out_dir, f'{base}.db'))
            result.cleanup()
        except Exception as e:
            print(f'FAIL: {f}: {e}')
"

# 2. Run evaluation
python eval/scorer.py my_outputs/

# 3. Check results
cat eval/eval_scores.csv
```

## Scorer Usage

```bash
# Evaluate against default output directory (tests/gdpval_outputs)
python eval/scorer.py

# Evaluate against a custom output directory (flat)
python eval/scorer.py path/to/outputs/

# Evaluate against a structured output directory
python eval/scorer.py path/to/outputs/
#   outputs/
#   ├── references/*.db
#   └── deliverables/*.db
```

The scorer auto-detects whether the output directory is flat or has `references/`/`deliverables/` subdirectories.

## Output Files

After running, the scorer produces:

| File | Format | Contents |
|------|--------|----------|
| `eval/eval_results.json` | JSON | Full per-file details: scores, table matching, column mapping |
| `eval/eval_scores.csv` | CSV | Summary table: Category, File, Converted, Weighted_Total, 5 dimension scores |

## Scoring Dimensions

Each file is scored on 5 dimensions (0.0 to 1.0):

### 1. Table Coverage (weight 0.3)

How many golden tables were captured in the script output?

```
score = matched_tables / golden_tables
```

Tables are matched by:
1. Source sheet metadata (from `_meta` / `_golden_meta` tables)
2. Fuzzy name matching (SequenceMatcher > 0.6)
3. Column overlap when multiple candidates exist

### 2. Column Coverage (weight 0.2)

For each matched table pair, how many golden columns appear in the script output?

```
score = avg(matched_cols / golden_cols) across all table pairs
```

- Exact column name match (normalized): 1.0 credit
- Script used `column_N` where golden has a real name: 0.5 credit
- No match: 0.0

### 3. Row Coverage (weight 0.2)

Row count similarity for each matched table pair:

```
score = min(script_rows, golden_rows) / max(script_rows, golden_rows)
```

Penalizes both missing rows (data loss) and extra rows (failed subtotal filtering).

### 4. Data Accuracy (weight 0.2)

Cell-by-cell comparison of sampled rows (up to 20 per table):

- Numbers: 1% tolerance
- Strings: case-insensitive, whitespace-trimmed
- NULLs: match NULLs; empty string matches NULL

Uses greedy row matching to handle different row ordering.

### 5. Type Correctness (weight 0.1)

SQLite column type comparison for matched columns:

- INTEGER and REAL both map to "NUMERIC" (mutual match)
- TEXT matches TEXT
- Numbers stored as TEXT: no match

### Weighted Total

```
total = 0.3×table + 0.2×column + 0.2×row + 0.2×data + 0.1×type
```

### Grade Scale

| Grade | Score | Meaning |
|-------|-------|---------|
| A | ≥ 0.8 | Excellent — data is reliable |
| B | 0.6–0.8 | Good — minor issues |
| C | 0.4–0.6 | Usable — notable data loss or structural issues |
| D | 0.2–0.4 | Poor — significant problems |
| F | < 0.2 | Failed — includes unconverted files (score 0) |

## Golden Standard Design Principles

Each golden DB was created by analyzing the original Excel file and applying these rules:

1. **Every visible sheet produces at least one table** (unless truly empty or chart-only)
2. **Column names are semantic** — actual header text from Excel, cleaned up
3. **Title/subtitle rows are skipped** (e.g., "Monthly Report Q1 2025")
4. **Subtotal/total rows are excluded** from data
5. **All data rows are included** — no data loss
6. **Types are correct**: numbers as REAL/INTEGER, text as TEXT, dates as ISO strings
7. **Merged cells are expanded** (value filled into all merged positions)
8. **Multiple tables in one sheet** create separate DB tables
9. **Empty separator columns** between data are removed
10. **Template/form sheets** with no data get 0-row tables with structure preserved

## Dataset Source

Excel files are from [OpenAI gdpval](https://huggingface.co/datasets/openai/gdpval) (`train` split):

- **Reference files** (86): Input data provided to task performers
- **Deliverable files** (65): Output data created by task performers

These cover a wide range of real-world Excel complexity: financial reports, inventory lists, scheduling templates, data analysis sheets, forms, and more.

## Adding New Golden DBs

To add a golden DB for a new Excel file:

```python
import sys
sys.path.insert(0, "eval")
from golden_builder import create_golden_db

create_golden_db("eval/golden_dbs/references/My File.db", {
    "my_table": {
        "columns": [
            {"name": "ID", "type": "INTEGER"},
            {"name": "Name", "type": "TEXT"},
            {"name": "Amount", "type": "REAL"},
        ],
        "rows": [
            [1, "Alice", 100.50],
            [2, "Bob", 200.75],
        ],
        "source_sheet": "Sheet1",
        "description": "Customer transactions",
    }
})
```
