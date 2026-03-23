"""TableConverter — orchestrates the 6-stage pipeline."""
from __future__ import annotations

import asyncio
import logging
from typing import BinaryIO, Union
from .models import WorkbookData, ConversionResult, ForeignKey
from .pipeline.reader import read_workbook
from .pipeline.structure import detect_structure
from .pipeline.cleaner import clean_data
from .pipeline.typer import infer_types
from .pipeline.relator import infer_relationships
from .loaders.sqlite_loader import SqliteLoader
from .loaders.base import BaseLoader
from .errors import NoDataError

logger = logging.getLogger(__name__)


def _build_quality(
    wb: WorkbookData,
    result: ConversionResult,
    original_sheet_count: int,
    original_sheet_names: list[str],
    skipped_sheets: list[str],
) -> dict:
    """Build quality metrics dict from pipeline metadata."""
    table_qualities: dict[str, dict] = {}

    for sheet, table_info in zip(wb.sheets, result.tables):
        detection_confidence = sheet.metadata.get("island_confidence", 1.0)
        header_confidence = sheet.metadata.get("header_confidence", 1.0)
        type_reliability = sheet.metadata.get("type_reliability", {})
        null_rates = sheet.metadata.get("null_rates", {})

        avg_type_reliability = (
            sum(type_reliability.values()) / len(type_reliability)
            if type_reliability else 1.0
        )
        avg_null_rate = (
            sum(null_rates.values()) / len(null_rates)
            if null_rates else 0.0
        )

        table_score = (
            detection_confidence * 0.2
            + header_confidence * 0.2
            + avg_type_reliability * 0.3
            + (1 - avg_null_rate) * 0.3
        )

        table_qualities[table_info.name] = {
            "table_score": round(table_score, 2),
            "detection_confidence": round(detection_confidence, 2),
            "header_confidence": round(header_confidence, 2),
            "type_reliability": type_reliability,
            "avg_type_reliability": round(avg_type_reliability, 2),
            "null_rates": null_rates,
            "avg_null_rate": round(avg_null_rate, 2),
            "rows_before_cleaning": sheet.metadata.get("rows_before_cleaning", len(sheet.rows)),
            "rows_after_cleaning": sheet.metadata.get("rows_after_cleaning", len(sheet.rows)),
            "rows_filtered": sheet.metadata.get("rows_filtered", 0),
            "duplicate_rows_removed": sheet.metadata.get("duplicate_rows_removed", 0),
        }

    # Overall score: weighted average by row count
    total_rows = sum(t.row_count for t in result.tables) or 1
    overall_score = sum(
        table_qualities[t.name]["table_score"] * t.row_count / total_rows
        for t in result.tables
    )

    # Relationship quality
    rel_quality = [
        {
            "from": f"{fk.from_table}.{fk.from_column}",
            "to": f"{fk.to_table}.{fk.to_column}",
            "confidence": fk.confidence,
        }
        for fk in result.relationships
    ]

    return {
        "overall_score": round(overall_score, 2),
        "sheets_found": original_sheet_count,
        "sheets_converted": len(result.tables),
        "sheets_skipped": skipped_sheets,
        "tables": table_qualities,
        "relationships": rel_quality,
    }


class TableConverter:
    def __init__(
        self,
        subtotal_keywords: list[str] | None = None,
        type_threshold: float = 0.8,
        skip_hidden_sheets: bool = True,
        fk_confidence_threshold: float = 0.8,
        header_min_fill_ratio: float = 0.5,
        header_min_string_ratio: float = 0.7,
    ):
        self.subtotal_keywords = subtotal_keywords
        self.type_threshold = type_threshold
        self.skip_hidden_sheets = skip_hidden_sheets
        self.fk_confidence_threshold = fk_confidence_threshold
        self.header_min_fill_ratio = header_min_fill_ratio
        self.header_min_string_ratio = header_min_string_ratio

    def process(
        self, source: Union[str, BinaryIO], file_name: str | None = None
    ) -> tuple[WorkbookData, list[str]]:
        """Run stages 1-5 (read, structure, clean, type, relate).

        Args:
            source: File path (str) or file-like object (BytesIO, UploadFile.file).
            file_name: Original file name (required when source is a stream).

        Returns (WorkbookData, warnings).
        """
        all_warnings: list[str] = []
        source_label = source if isinstance(source, str) else (file_name or "stream")

        logger.info("Stage 1: Reading workbook from %s", source_label)
        wb = read_workbook(source, skip_hidden_sheets=self.skip_hidden_sheets,
                           file_name=file_name)
        for sheet in wb.sheets:
            all_warnings.extend(sheet.metadata.get("warnings", []))

        # Track original sheet info for quality metrics
        original_sheet_count = len(wb.sheets)
        original_sheet_names = [s.name for s in wb.sheets]

        logger.info("Stage 2: Detecting structure (%d sheets)", len(wb.sheets))
        wb, warnings = detect_structure(
            wb,
            header_min_fill_ratio=self.header_min_fill_ratio,
            header_min_string_ratio=self.header_min_string_ratio,
        )
        all_warnings.extend(warnings)

        if not wb.sheets:
            raise NoDataError(f"No valid sheets found in {source_label}")

        logger.info("Stage 3: Cleaning data (%d sheets)", len(wb.sheets))
        wb, warnings = clean_data(wb, subtotal_keywords=self.subtotal_keywords)
        all_warnings.extend(warnings)

        if not wb.sheets:
            raise NoDataError(f"No data remaining after cleaning in {source_label}")

        logger.info("Stage 4: Inferring types")
        wb = infer_types(wb, type_threshold=self.type_threshold)

        logger.info("Stage 5: Inferring relationships")
        wb = infer_relationships(wb, fk_confidence_threshold=self.fk_confidence_threshold)

        # Compute skipped sheets
        remaining_names = {s.name for s in wb.sheets}
        skipped_sheets = [n for n in original_sheet_names if n not in remaining_names]
        # Store quality-related metadata on the workbook for convert() to use
        wb.metadata = {
            "original_sheet_count": original_sheet_count,
            "original_sheet_names": original_sheet_names,
            "skipped_sheets": skipped_sheets,
        }

        return wb, all_warnings

    def convert(
        self,
        source: Union[str, BinaryIO],
        loader: BaseLoader | None = None,
        file_name: str | None = None,
    ) -> ConversionResult:
        """Run the full pipeline (stages 1-6) and return ConversionResult.

        Args:
            source: File path (str) or file-like object (BytesIO, UploadFile.file).
            loader: Optional custom loader. Defaults to SqliteLoader().
            file_name: Original file name (required when source is a stream).
        """
        wb, all_warnings = self.process(source, file_name=file_name)

        if loader is None:
            loader = SqliteLoader()

        logger.info("Stage 6: Loading with %s", type(loader).__name__)
        result = loader.load(wb)
        result.warnings = all_warnings

        # Build quality metrics
        result.quality = _build_quality(
            wb,
            result,
            wb.metadata.get("original_sheet_count", len(wb.sheets)),
            wb.metadata.get("original_sheet_names", [s.name for s in wb.sheets]),
            wb.metadata.get("skipped_sheets", []),
        )

        logger.info("Conversion complete: %d tables, %d warnings",
                     len(result.tables), len(result.warnings))
        return result

    async def convert_async(
        self,
        source: Union[str, BinaryIO],
        loader: BaseLoader | None = None,
        file_name: str | None = None,
    ) -> ConversionResult:
        """Async version of convert(). Runs pipeline in a thread pool."""
        return await asyncio.to_thread(self.convert, source, loader, file_name)

    async def process_async(
        self, source: Union[str, BinaryIO], file_name: str | None = None
    ) -> tuple[WorkbookData, list[str]]:
        """Async version of process(). Runs pipeline in a thread pool."""
        return await asyncio.to_thread(self.process, source, file_name)
