"""Input rows for the dim_location tests, one list per source.

Every source reads another job's output, so its rows are that job's expected output rows,
imported from its fixtures below with their REVIEWED flag. There is nothing to fill in
here; the tests fail until those rows are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

from tests.fixtures.location_reference.expected import DEDUPLICATE_LOCATION_REFERENCE_REVIEWED
from tests.fixtures.location_reference.expected import (
    DEDUPLICATE_LOCATION_REFERENCE_ROWS as LOCATION_REFERENCE_ROWS,
)
