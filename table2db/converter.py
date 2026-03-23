"""TableConverter — orchestrates the 6-stage pipeline."""
from __future__ import annotations

import asyncio
import logging
from typing import BinaryIO, Union
from .models import WorkbookData, ConversionResult
from .pipeline.reader import read_workbook
from .pipeline.structure import detect_structure
from .pipeline.cleaner import clean_data
from .pipeline.typer import infer_types
from .pipeline.relator import infer_relationships
from .loaders.sqlite_loader import SqliteLoader
from .loaders.base import BaseLoader
from .errors import NoDataError

logger = logging.getLogger(__name__)


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
