from .null import NullDestination
from .sql import SQLDestination
from .file import CSVDestination, CSVAppendDestination, XLSXDestination

__all__ = ["NullDestination", "SQLDestination", "CSVDestination", "CSVAppendDestination", "XLSXDestination"]
