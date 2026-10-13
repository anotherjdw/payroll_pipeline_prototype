"""Input rows for the dim_cost_center tests, one list per source.

Every source reads another job's output, so its rows are that job's expected output rows,
imported from its fixtures below with their REVIEWED flag. There is nothing to fill in
here; the tests fail until those rows are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.cost_center_reference.expected import DEDUPLICATE_COST_CENTER_REFERENCE_REVIEWED
from tests.fixtures.cost_center_reference.expected import (
    DEDUPLICATE_COST_CENTER_REFERENCE_ROWS as COST_CENTER_REFERENCE_ROWS,
)
