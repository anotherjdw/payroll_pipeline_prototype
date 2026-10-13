"""Input rows for the dim_employee tests, one list per source.

Every source reads another job's output, so its rows are that job's expected output rows,
imported from its fixtures below with their REVIEWED flag. There is nothing to fill in
here; the tests fail until those rows are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

from tests.fixtures.employee_master.expected import ADD_IS_CURRENT_FLAG_REVIEWED
from tests.fixtures.employee_master.expected import ADD_IS_CURRENT_FLAG_ROWS as EMPLOYEE_MASTER_ROWS
