"""Expected output rows of the dim_date transformations.

One list per transformation. `engineering-automation generate expected-outputs` drafts
each empty list from the transformation's description and input rows, without seeing its
code. Check every row, correct it, then set that transformation's REVIEWED flag to True.
Its test fails until you do.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

CREATE_YEAR_COLUMN_REVIEWED = True
CREATE_YEAR_COLUMN_ROWS: list[Row] = [
    Row(date=date(2026, 1, 1), year=2026),
    Row(date=date(2026, 1, 31), year=2026),
    Row(date=date(2026, 2, 14), year=2026),
    Row(date=date(2025, 5, 11), year=2025),
    Row(date=date(2026, 5, 10), year=2026),
    Row(date=date(2026, 12, 13), year=2026),
    Row(date=date(2026, 12, 26), year=2026),
]

CREATE_QUARTER_COLUMN_REVIEWED = True
CREATE_QUARTER_COLUMN_ROWS: list[Row] = [
    Row(date=date(2026, 1, 1), year=2026, quarter=1),
    Row(date=date(2026, 1, 31), year=2026, quarter=1),
    Row(date=date(2026, 2, 14), year=2026, quarter=1),
    Row(date=date(2025, 5, 11), year=2025, quarter=2),
    Row(date=date(2026, 5, 10), year=2026, quarter=2),
    Row(date=date(2026, 12, 13), year=2026, quarter=4),
    Row(date=date(2026, 12, 26), year=2026, quarter=4),
]

CREATE_MONTH_NAME_COLUMN_REVIEWED = True
CREATE_MONTH_NAME_COLUMN_ROWS: list[Row] = [
    Row(date=date(2026, 1, 1), year=2026, quarter=1, month_name="January"),
    Row(date=date(2026, 1, 31), year=2026, quarter=1, month_name="January"),
    Row(date=date(2026, 2, 14), year=2026, quarter=1, month_name="February"),
    Row(date=date(2025, 5, 11), year=2025, quarter=2, month_name="May"),
    Row(date=date(2026, 5, 10), year=2026, quarter=2, month_name="May"),
    Row(date=date(2026, 12, 13), year=2026, quarter=4, month_name="December"),
    Row(date=date(2026, 12, 26), year=2026, quarter=4, month_name="December"),
]

CREATE_PAY_PERIOD_DIMENSION_REVIEWED = True
CREATE_PAY_PERIOD_DIMENSION_ROWS: list[Row] = [
    Row(date=date(2026, 1, 1), year=2026, quarter=1, month_name="January", pay_period="2026-01"),
    Row(date=date(2026, 1, 31), year=2026, quarter=1, month_name="January", pay_period="2026-01"),
    Row(date=date(2026, 2, 14), year=2026, quarter=1, month_name="February", pay_period="2026-02"),
    Row(date=date(2025, 5, 11), year=2025, quarter=2, month_name="May", pay_period="2025-05"),
    Row(date=date(2026, 5, 10), year=2026, quarter=2, month_name="May", pay_period="2026-05"),
    Row(date=date(2026, 12, 13), year=2026, quarter=4, month_name="December", pay_period="2026-12"),
    Row(date=date(2026, 12, 26), year=2026, quarter=4, month_name="December", pay_period="2026-12"),
]

ADD_MONTH_BOUNDARIES_REVIEWED = True
ADD_MONTH_BOUNDARIES_ROWS: list[Row] = [
    Row(
        date=date(2026, 1, 1),
        year=2026,
        quarter=1,
        month_name="January",
        pay_period="2026-01",
        month_begin=date(2026, 1, 1),
        month_end=date(2026, 1, 31),
        month_end_flag=False,
    ),
    Row(
        date=date(2026, 1, 31),
        year=2026,
        quarter=1,
        month_name="January",
        pay_period="2026-01",
        month_begin=date(2026, 1, 1),
        month_end=date(2026, 1, 31),
        month_end_flag=True,
    ),
    Row(
        date=date(2026, 2, 14),
        year=2026,
        quarter=1,
        month_name="February",
        pay_period="2026-02",
        month_begin=date(2026, 2, 1),
        month_end=date(2026, 2, 28),
        month_end_flag=False,
    ),
    Row(
        date=date(2025, 5, 11),
        year=2025,
        quarter=2,
        month_name="May",
        pay_period="2025-05",
        month_begin=date(2025, 5, 1),
        month_end=date(2025, 5, 31),
        month_end_flag=False,
    ),
    Row(
        date=date(2026, 5, 10),
        year=2026,
        quarter=2,
        month_name="May",
        pay_period="2026-05",
        month_begin=date(2026, 5, 1),
        month_end=date(2026, 5, 31),
        month_end_flag=False,
    ),
    Row(
        date=date(2026, 12, 13),
        year=2026,
        quarter=4,
        month_name="December",
        pay_period="2026-12",
        month_begin=date(2026, 12, 1),
        month_end=date(2026, 12, 31),
        month_end_flag=False,
    ),
    Row(
        date=date(2026, 12, 26),
        year=2026,
        quarter=4,
        month_name="December",
        pay_period="2026-12",
        month_begin=date(2026, 12, 1),
        month_end=date(2026, 12, 31),
        month_end_flag=False,
    ),
]

ADD_PEAK_FLAG_REVIEWED = True
ADD_PEAK_FLAG_ROWS: list[Row] = [
    Row(
        date=date(2026, 1, 1),
        year=2026,
        quarter=1,
        month_name="January",
        pay_period="2026-01",
        month_begin=date(2026, 1, 1),
        month_end=date(2026, 1, 31),
        month_end_flag=False,
        peak_flag=False,
    ),
    Row(
        date=date(2026, 1, 31),
        year=2026,
        quarter=1,
        month_name="January",
        pay_period="2026-01",
        month_begin=date(2026, 1, 1),
        month_end=date(2026, 1, 31),
        month_end_flag=True,
        peak_flag=False,
    ),
    Row(
        date=date(2026, 2, 14),
        year=2026,
        quarter=1,
        month_name="February",
        pay_period="2026-02",
        month_begin=date(2026, 2, 1),
        month_end=date(2026, 2, 28),
        month_end_flag=False,
        peak_flag=True,
    ),
    Row(
        date=date(2025, 5, 11),
        year=2025,
        quarter=2,
        month_name="May",
        pay_period="2025-05",
        month_begin=date(2025, 5, 1),
        month_end=date(2025, 5, 31),
        month_end_flag=False,
        peak_flag=True,
    ),
    Row(
        date=date(2026, 5, 10),
        year=2026,
        quarter=2,
        month_name="May",
        pay_period="2026-05",
        month_begin=date(2026, 5, 1),
        month_end=date(2026, 5, 31),
        month_end_flag=False,
        peak_flag=True,
    ),
    Row(
        date=date(2026, 12, 13),
        year=2026,
        quarter=4,
        month_name="December",
        pay_period="2026-12",
        month_begin=date(2026, 12, 1),
        month_end=date(2026, 12, 31),
        month_end_flag=False,
        peak_flag=True,
    ),
    Row(
        date=date(2026, 12, 26),
        year=2026,
        quarter=4,
        month_name="December",
        pay_period="2026-12",
        month_begin=date(2026, 12, 1),
        month_end=date(2026, 12, 31),
        month_end_flag=False,
        peak_flag=False,
    ),
]

ADD_DATE_KEY_REVIEWED = True
ADD_DATE_KEY_ROWS: list[Row] = [
    Row(
        date=date(2026, 1, 1),
        year=2026,
        quarter=1,
        month_name="January",
        pay_period="2026-01",
        month_begin=date(2026, 1, 1),
        month_end=date(2026, 1, 31),
        month_end_flag=False,
        peak_flag=False,
        date_key=20260101,
    ),
    Row(
        date=date(2026, 1, 31),
        year=2026,
        quarter=1,
        month_name="January",
        pay_period="2026-01",
        month_begin=date(2026, 1, 1),
        month_end=date(2026, 1, 31),
        month_end_flag=True,
        peak_flag=False,
        date_key=20260131,
    ),
    Row(
        date=date(2026, 2, 14),
        year=2026,
        quarter=1,
        month_name="February",
        pay_period="2026-02",
        month_begin=date(2026, 2, 1),
        month_end=date(2026, 2, 28),
        month_end_flag=False,
        peak_flag=True,
        date_key=20260214,
    ),
    Row(
        date=date(2025, 5, 11),
        year=2025,
        quarter=2,
        month_name="May",
        pay_period="2025-05",
        month_begin=date(2025, 5, 1),
        month_end=date(2025, 5, 31),
        month_end_flag=False,
        peak_flag=True,
        date_key=20250511,
    ),
    Row(
        date=date(2026, 5, 10),
        year=2026,
        quarter=2,
        month_name="May",
        pay_period="2026-05",
        month_begin=date(2026, 5, 1),
        month_end=date(2026, 5, 31),
        month_end_flag=False,
        peak_flag=True,
        date_key=20260510,
    ),
    Row(
        date=date(2026, 12, 13),
        year=2026,
        quarter=4,
        month_name="December",
        pay_period="2026-12",
        month_begin=date(2026, 12, 1),
        month_end=date(2026, 12, 31),
        month_end_flag=False,
        peak_flag=True,
        date_key=20261213,
    ),
    Row(
        date=date(2026, 12, 26),
        year=2026,
        quarter=4,
        month_name="December",
        pay_period="2026-12",
        month_begin=date(2026, 12, 1),
        month_end=date(2026, 12, 31),
        month_end_flag=False,
        peak_flag=False,
        date_key=20261226,
    ),
]
