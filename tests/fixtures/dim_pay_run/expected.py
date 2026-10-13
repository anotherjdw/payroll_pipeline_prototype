"""Expected output rows of the dim_pay_run transformations.

One list per transformation. `engineering-automation generate expected-outputs` drafts
each empty list from the transformation's description and input rows, without seeing its
code. Check every row, correct it, then set that transformation's REVIEWED flag to True.
Its test fails until you do.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

SELECT_PAY_RUN_COLUMNS_REVIEWED = True
SELECT_PAY_RUN_COLUMNS_ROWS: list[Row] = [
    Row(pay_run_id="2026-01-REG", pay_date=date(2026, 1, 30), run_type="regular"),
    Row(pay_run_id="2026-01-REG", pay_date=date(2026, 1, 30), run_type="regular"),
    Row(pay_run_id="2026-01-REG", pay_date=date(2026, 1, 30), run_type="regular"),
    Row(pay_run_id="2026-02-REG", pay_date=date(2026, 2, 27), run_type="regular"),
    Row(pay_run_id="2026-02-REG", pay_date=date(2026, 2, 27), run_type="regular"),
    Row(pay_run_id="2026-02-REG", pay_date=date(2026, 2, 27), run_type="regular"),
    Row(pay_run_id="2026-02-REG", pay_date=date(2026, 2, 27), run_type="regular"),
    Row(pay_run_id="2026-02-REG", pay_date=date(2026, 2, 27), run_type="regular"),
    Row(pay_run_id="2026-02-REG", pay_date=date(2026, 2, 27), run_type="regular"),
]

DEDUPLICATE_PAY_RUN_DATA_REVIEWED = True
DEDUPLICATE_PAY_RUN_DATA_ROWS: list[Row] = [
    Row(pay_run_id="2026-01-REG", pay_date=date(2026, 1, 30), run_type="regular"),
    Row(pay_run_id="2026-02-REG", pay_date=date(2026, 2, 27), run_type="regular"),
]

CREATE_PAY_RUN_KEY_REVIEWED = True
CREATE_PAY_RUN_KEY_ROWS: list[Row] = [
    Row(
        pay_run_id="2026-01-REG",
        pay_date=date(2026, 1, 30),
        run_type="regular",
        pay_run_key=md5(b"2026-01-REG").hexdigest(),
    ),
    Row(
        pay_run_id="2026-02-REG",
        pay_date=date(2026, 2, 27),
        run_type="regular",
        pay_run_key=md5(b"2026-02-REG").hexdigest(),
    ),
]
