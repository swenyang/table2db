"""Stage 4: Type inference — classify column types and convert values."""

from __future__ import annotations

import datetime
import math
import re
from collections import Counter
from typing import Any

from table2db.models import WorkbookData

# Date patterns: (regex, strptime format)
_DATE_PATTERNS: list[tuple[str, str]] = [
    (r"^\d{4}-\d{2}-\d{2}$", "%Y-%m-%d"),
    (r"^\d{4}/\d{2}/\d{2}$", "%Y/%m/%d"),
    (r"^\d{2}/\d{2}/\d{4}$", None),  # ambiguous DD/MM or MM/DD
    (r"^\d{2}-\d{2}-\d{4}$", None),  # ambiguous DD-MM or MM-DD
]


def _try_parse_date(s: str) -> datetime.date | None:
    s = s.strip()
    # YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        try:
            return datetime.date(int(m[1]), int(m[2]), int(m[3]))
        except ValueError:
            return None

    # YYYY/MM/DD
    m = re.match(r"^(\d{4})/(\d{2})/(\d{2})$", s)
    if m:
        try:
            return datetime.date(int(m[1]), int(m[2]), int(m[3]))
        except ValueError:
            return None

    # DD/MM/YYYY or MM/DD/YYYY — try DD/MM first, then MM/DD
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if m:
        a, b, y = int(m[1]), int(m[2]), int(m[3])
        # Try DD/MM/YYYY
        try:
            return datetime.date(y, b, a)
        except ValueError:
            pass
        # Try MM/DD/YYYY
        try:
            return datetime.date(y, a, b)
        except ValueError:
            return None

    # DD-MM-YYYY or MM-DD-YYYY
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", s)
    if m:
        a, b, y = int(m[1]), int(m[2]), int(m[3])
        try:
            return datetime.date(y, b, a)
        except ValueError:
            pass
        try:
            return datetime.date(y, a, b)
        except ValueError:
            return None

    return None


def _classify_value(val: Any) -> tuple[str, Any]:
    """Classify a value and return (type_name, converted_value)."""
    if val is None:
        return ("NONE", None)

    if isinstance(val, bool):
        return ("INTEGER", 1 if val else 0)

    if isinstance(val, int):
        return ("INTEGER", val)

    if isinstance(val, float):
        if not math.isinf(val) and val == int(val):
            return ("INTEGER", int(val))
        return ("REAL", val)

    if isinstance(val, datetime.datetime):
        return ("DATE", val.isoformat())

    if isinstance(val, datetime.date):
        return ("DATE", val.isoformat())

    if isinstance(val, str):
        # Try date parsing
        parsed = _try_parse_date(val)
        if parsed is not None:
            return ("DATE", parsed.isoformat())

        # Try numeric
        try:
            f = float(val)
            if not math.isinf(f) and f == int(f):
                return ("INTEGER", int(f))
            return ("REAL", f)
        except (ValueError, OverflowError):
            pass

        return ("TEXT", val)

    return ("TEXT", str(val))


def _convert_value(val: Any, target_type: str) -> Any:
    """Convert a value to the target column type. Returns None on failure."""
    if val is None:
        return None

    try:
        if target_type == "INTEGER":
            classified_type, converted = _classify_value(val)
            if classified_type == "INTEGER":
                return converted
            if classified_type == "REAL":
                return int(converted) if converted == int(converted) else None
            return None

        if target_type == "REAL":
            classified_type, converted = _classify_value(val)
            if classified_type in ("INTEGER", "REAL"):
                return float(converted)
            return None

        if target_type == "DATE":
            classified_type, converted = _classify_value(val)
            if classified_type == "DATE":
                return converted
            return None

        # TEXT
        classified_type, converted = _classify_value(val)
        if classified_type == "NONE":
            return None
        return str(converted) if classified_type != "TEXT" else converted

    except Exception:
        return None


def infer_types(wb: WorkbookData, type_threshold: float = 0.8) -> WorkbookData:
    for sheet in wb.sheets:
        ncols = len(sheet.headers)

        for col_idx in range(ncols):
            # Collect classifications for non-None values
            type_counts: Counter[str] = Counter()
            total_non_none = 0

            for row in sheet.rows:
                val = row[col_idx] if col_idx < len(row) else None
                type_name, _ = _classify_value(val)
                if type_name != "NONE":
                    type_counts[type_name] += 1
                    total_non_none += 1

            # Determine column type via majority vote
            if total_non_none == 0:
                col_type = "TEXT"
            else:
                # Merge INTEGER and REAL as "numeric" for threshold check
                numeric_count = type_counts.get("INTEGER", 0) + type_counts.get("REAL", 0)
                top_type, top_count = type_counts.most_common(1)[0]

                if numeric_count / total_non_none >= type_threshold:
                    # Column is numeric; pick REAL if any REAL values exist, else INTEGER
                    col_type = "REAL" if type_counts.get("REAL", 0) > 0 else "INTEGER"
                elif top_count / total_non_none >= type_threshold:
                    col_type = top_type
                else:
                    col_type = "TEXT"

            sheet.column_types[sheet.headers[col_idx]] = col_type

            # Convert values in place
            for row in sheet.rows:
                if col_idx < len(row):
                    row[col_idx] = _convert_value(row[col_idx], col_type)

    return wb
