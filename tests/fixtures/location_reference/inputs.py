"""Input rows for the location_reference tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

INPUTS_FILLED = True

LOCATION_REFERENCE_RAW_ROWS = [
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
    # ---- duplicate of the LOC-LEI row above ----
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
