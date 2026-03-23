"""Base loader protocol for table2db."""
from __future__ import annotations
from abc import ABC, abstractmethod
from table2db.models import WorkbookData, ConversionResult


class BaseLoader(ABC):
    """Abstract base class for database loaders.

    To create a custom loader, subclass BaseLoader and implement load().

    Example:
        class MyPostgresLoader(BaseLoader):
            def load(self, wb: WorkbookData) -> ConversionResult:
                # Create tables and insert data into PostgreSQL
                ...
    """

    @abstractmethod
    def load(self, wb: WorkbookData) -> ConversionResult:
        """Load WorkbookData into a database and return ConversionResult."""
        ...
