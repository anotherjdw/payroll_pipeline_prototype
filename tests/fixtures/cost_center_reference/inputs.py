"""Input rows for the cost_center_reference tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

INPUTS_FILLED = True

COST_CENTER_REFERENCE_RAW_ROWS = [
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
    # ---- duplicate of the CC-WHS row above ----
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
]
