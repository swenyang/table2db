"""Stage 5: Relationship inference — primary keys and foreign keys."""

from __future__ import annotations

import re
from table2db.models import WorkbookData, ForeignKey

_PK_PATTERN = re.compile(r'^(id|.*_id|.*_no|.*_code)$', re.IGNORECASE)


def _normalize_table_name(name: str) -> str:
    """'Customers' → 'customer', 'Order Details' → 'order_details'"""
    name = re.sub(r'[^a-zA-Z0-9]+', '_', name.lower()).strip('_')
    if name.endswith('s'):
        name = name[:-1]
    return name


def _get_column_values(sheet, col_name: str) -> list:
    """Extract all values for a column from sheet rows."""
    idx = sheet.headers.index(col_name)
    return [row[idx] for row in sheet.rows]


def _infer_primary_key(sheet) -> str | None:
    """Infer the primary key for a single sheet."""
    candidates = []
    for col in sheet.headers:
        col_type = sheet.column_types.get(col, "")
        if col_type not in ("INTEGER", "TEXT"):
            continue
        if not _PK_PATTERN.match(col):
            continue
        values = _get_column_values(sheet, col)
        if any(v is None for v in values):
            continue
        if len(values) != len(set(values)):
            continue
        # Name score: prefer "id" exactly
        name_score = 0.3
        candidates.append((col, name_score))

    if not candidates:
        return None

    # Prefer column named exactly "id", else leftmost
    for col, _ in candidates:
        if col.lower() == "id":
            return col
    return candidates[0][0]


def infer_relationships(
    wb: WorkbookData,
    fk_confidence_threshold: float = 0.8,
) -> WorkbookData:
    """Infer primary keys and foreign keys across all sheets."""
    # Phase A: Primary Key inference
    for sheet in wb.sheets:
        sheet.primary_key = _infer_primary_key(sheet)

    # Phase B: Foreign Key inference
    fks: list[ForeignKey] = []

    for a in wb.sheets:
        if a.primary_key is None:
            continue

        a_pk_values = _get_column_values(a, a.primary_key)
        a_pk_set = set(v for v in a_pk_values if v is not None)

        if len(a_pk_set) < 10:
            continue

        a_norm = _normalize_table_name(a.name)

        for b in wb.sheets:
            if b is a:
                continue

            for b_col in b.headers:
                # Skip if b_col is B's own primary key
                if b_col == b.primary_key:
                    continue

                # Determine match type
                base_confidence = 0.0
                if b_col == a.primary_key:
                    base_confidence = 0.9  # exact match
                elif a.primary_key.lower() == "id" and b_col.lower() in (
                    f"{a_norm}_id",
                    f"{_normalize_table_name(a.name + 's')}_id",  # with trailing s
                ):
                    base_confidence = 0.8  # pattern match
                else:
                    continue

                # Value containment check
                b_values = _get_column_values(b, b_col)
                b_non_none_set = set(v for v in b_values if v is not None)
                if not b_non_none_set:
                    continue

                overlap = len(b_non_none_set & a_pk_set) / len(b_non_none_set)
                if overlap < 0.9:
                    continue

                confidence = base_confidence * overlap
                if confidence >= fk_confidence_threshold:
                    fks.append(ForeignKey(
                        from_table=b.name,
                        from_column=b_col,
                        to_table=a.name,
                        to_column=a.primary_key,
                        confidence=confidence,
                    ))

    wb.relationships = fks
    return wb
