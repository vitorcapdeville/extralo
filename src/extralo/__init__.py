import logging

from extralo.destinations import SQLDestination
from extralo.etl import ETL
from extralo.sources import CSVSource, SQLSource, XLSXSource

_default_logger = logging.getLogger("etl")
_default_logger.setLevel(logging.INFO)
_default_formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
_default_handler = logging.StreamHandler()
_default_handler.setFormatter(_default_formatter)
_default_logger.addHandler(_default_handler)


__all__ = ["ETL", "CSVSource", "SQLSource", "XLSXSource", "SQLDestination"]
