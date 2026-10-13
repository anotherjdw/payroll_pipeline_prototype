"""Input rows for the dim_date tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

INPUTS_FILLED = True

DATES_ROWS = [
    Row(date=date(2026, 1, 1)),
    Row(date=date(2026, 1, 31)),
    # Valentine's Day
    Row(date=date(2026, 2, 14)),
    # Mother's Day
    Row(date=date(2025, 5, 11)),
    Row(date=date(2026, 5, 10)),
    # Christmas
    Row(date=date(2026, 12, 13)),
    Row(date=date(2026, 12, 26)),
]
