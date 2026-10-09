"""Expected output rows of the location_reference transformations.

One list per transformation. `engineering-automation generate expected-outputs` drafts
each empty list from the transformation's description and input rows, without seeing its
code. Check every row, correct it, then set that transformation's REVIEWED flag to True.
Its test fails until you do.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

DEDUPLICATE_LOCATION_REFERENCE_REVIEWED = True
DEDUPLICATE_LOCATION_REFERENCE_ROWS: list[Row] = [
    Row(
        snapshot_date=date(2026, 6, 1),
        location_code="LOC-BER",
        location_name="Berlin HQ",
        site_type="headquarters",
        street_address="Ritterstrasse 12-14",
        postal_code="10969",
        city="Berlin",
        bundesland="Berlin",
        bundesland_code="BE",
        country="Deutschland",
        country_code="DE",
        timezone="Europe/Berlin",
        opened_date=date(2016, 3, 1),
        church_tax_rate_pct=Decimal("9.00"),
        pv_regional_variant="standard",
        rv_av_ceiling_region_pre_2025="West",
        is_active=True,
        valid_from=date(2024, 7, 1),
        valid_to=date(2026, 6, 30),
    ),
    Row(
        snapshot_date=date(2026, 6, 1),
        location_code="LOC-LEI",
        location_name="Leipzig Fulfillment Center",
        site_type="fulfillment_center",
        street_address="Zum Fernsehturm 8",
        postal_code="04347",
        city="Leipzig",
        bundesland="Sachsen",
        bundesland_code="SN",
        country="Deutschland",
        country_code="DE",
        timezone="Europe/Berlin",
        opened_date=date(2019, 9, 1),
        church_tax_rate_pct=Decimal("9.00"),
        pv_regional_variant="saxony",
        rv_av_ceiling_region_pre_2025="East",
        is_active=True,
        valid_from=date(2024, 7, 1),
        valid_to=date(2026, 6, 30),
    ),
]
