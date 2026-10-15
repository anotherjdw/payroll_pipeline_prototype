"""Expected output rows of the dim_cost_center transformations.

One list per transformation. `engineering-automation generate expected-outputs` drafts
each empty list from the transformation's description and input rows, without seeing its
code. Check every row, correct it, then set that transformation's REVIEWED flag to True.
Its test fails until you do.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

CREATE_COST_CENTER_KEY_REVIEWED = True
CREATE_COST_CENTER_KEY_ROWS: list[Row] = [
    Row(
        snapshot_date=date(2026, 6, 1),
        cost_center_code="CC-EXE",
        cost_center_name="Executive / Leadership",
        department_name="Executive / Leadership",
        cost_center_type="overhead",
        default_location_code="LOC-BER",
        accident_insurance_rate_pct=Decimal("0.30"),
        accident_risk_class="office",
        gl_cost_center_segment="1000",
        planned_headcount_fte=4,
        is_active=True,
        valid_from=date(2024, 7, 1),
        valid_to=date(2026, 6, 30),
        cost_center_key=md5("CC-EXE2024-07-01".encode("utf-8")).hexdigest(),
    ),
    Row(
        snapshot_date=date(2026, 6, 1),
        cost_center_code="CC-WHS",
        cost_center_name="Warehouse / Fulfillment / Logistics",
        department_name="Warehouse / Fulfillment / Logistics",
        cost_center_type="operational",
        default_location_code="LOC-LEI",
        accident_insurance_rate_pct=Decimal("3.50"),
        accident_risk_class="warehouse",
        gl_cost_center_segment="6000",
        planned_headcount_fte=90,
        is_active=True,
        valid_from=date(2024, 7, 1),
        valid_to=date(2026, 6, 30),
        cost_center_key=md5("CC-WHS2024-07-01".encode("utf-8")).hexdigest(),
    ),
    Row(
        snapshot_date=date(2026, 6, 1),
        cost_center_code="CC-MKT",
        cost_center_name="Marketing",
        department_name="Marketing",
        cost_center_type="commercial",
        default_location_code="LOC-BER",
        accident_insurance_rate_pct=Decimal("0.30"),
        accident_risk_class="office",
        gl_cost_center_segment="3000",
        planned_headcount_fte=24,
        is_active=True,
        valid_from=date(2024, 7, 1),
        valid_to=date(2026, 6, 30),
        cost_center_key=md5("CC-MKT2024-07-01".encode("utf-8")).hexdigest(),
    ),
    Row(
        snapshot_date=date(2026, 6, 1),
        cost_center_code="CC-CS",
        cost_center_name="Customer Service / Support",
        department_name="Customer Service / Support",
        cost_center_type="operational",
        default_location_code="LOC-LEI",
        accident_insurance_rate_pct=Decimal("1.50"),
        accident_risk_class="mixed_service",
        gl_cost_center_segment="5000",
        planned_headcount_fte=42,
        is_active=True,
        valid_from=date(2024, 7, 1),
        valid_to=date(2026, 6, 30),
        cost_center_key=md5("CC-CS2024-07-01".encode("utf-8")).hexdigest(),
    ),
    Row(
        snapshot_date=date(2026, 6, 1),
        cost_center_code="CC-FAC",
        cost_center_name="Facilities / Office Admin",
        department_name="Facilities / Office Admin",
        cost_center_type="overhead",
        default_location_code="LOC-BER",
        accident_insurance_rate_pct=Decimal("0.30"),
        accident_risk_class="office",
        gl_cost_center_segment="1300",
        planned_headcount_fte=4,
        is_active=True,
        valid_from=date(2024, 7, 1),
        valid_to=date(2026, 6, 30),
        cost_center_key=md5("CC-FAC2024-07-01".encode("utf-8")).hexdigest(),
    ),
]
