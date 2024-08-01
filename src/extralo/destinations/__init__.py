from .null import NullDestination
from .sql import SQLDestination, SQLAppendDestination
from .file import CSVDestination, CSVAppendDestination, XLSXDestination

__all__ = [
    "NullDestination",
    "SQLDestination",
    "CSVDestination",
    "CSVAppendDestination",
    "XLSXDestination",
    "SQLAppendDestination",
]
