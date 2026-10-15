"""Input rows for the fact_pay_component tests, one list per source.

Every source reads another job's output, so its rows are that job's expected output rows,
imported from its fixtures below with their REVIEWED flag. There is nothing to fill in
here; the tests fail until those rows are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.dim_cost_center.expected import CREATE_COST_CENTER_KEY_REVIEWED
from tests.fixtures.dim_cost_center.expected import (
    CREATE_COST_CENTER_KEY_ROWS as DIM_COST_CENTER_ROWS,
)
from tests.fixtures.dim_employee.expected import RENAME_EMPLOYEE_VERSION_ID_REVIEWED
from tests.fixtures.dim_employee.expected import (
    RENAME_EMPLOYEE_VERSION_ID_ROWS as DIM_EMPLOYEE_ROWS,
)
from tests.fixtures.dim_location.expected import CREATE_LOCATION_REFERENCE_KEY_REVIEWED
from tests.fixtures.dim_location.expected import (
    CREATE_LOCATION_REFERENCE_KEY_ROWS as DIM_LOCATION_ROWS,
)
from tests.fixtures.dim_pay_component.expected import ADD_CEILING_CAPPED_FLAG_REVIEWED
from tests.fixtures.dim_pay_component.expected import (
    ADD_CEILING_CAPPED_FLAG_ROWS as DIM_PAY_COMPONENT_ROWS,
)
from tests.fixtures.payroll_register.expected import COMBINE_PAY_COLUMNS_REVIEWED
from tests.fixtures.payroll_register.expected import (
    COMBINE_PAY_COLUMNS_ROWS as PAYROLL_REGISTER_ROWS,
)
