class ExcelToDbError(Exception):
    """Base exception for table2db library."""

class FileReadError(ExcelToDbError):
    """File cannot be read: corrupted, password-protected, missing."""

class NoDataError(ExcelToDbError):
    """File readable but contains no usable data."""

class UnsupportedFormatError(ExcelToDbError):
    """Unsupported file format (e.g. .xlsb)."""

class SchemaError(ExcelToDbError):
    """Cannot infer valid table structure (e.g. no header found)."""
