"""Expected output rows of the cost_center_reference transformations.

One list per transformation. `engineering-automation generate expected-outputs` drafts
each empty list from the transformation's description and input rows, without seeing its
code. Check every row, correct it, then set that transformation's REVIEWED flag to True.
Its test fails until you do.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

DEDUPLICATE_COST_CENTER_REFERENCE_REVIEWED = True
DEDUPLICATE_COST_CENTER_REFERENCE_ROWS: list[Row] = [
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
    ),
]
