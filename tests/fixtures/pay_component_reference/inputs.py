"""Input rows for the pay_component_reference tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.
"""

from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Row

INPUTS_FILLED = True

PAY_COMPONENT_REFERENCE_RAW_ROWS = [
    Row(
        reference_version="2026H1",
        valid_from=date(2026, 1, 1),
        valid_to=date(2026, 6, 30),
        lohnart_code="1000",
        name_de="Grundgehalt",
        name_en="Base salary",
        category="earning",
        bearer="employer",
        gl_account="6010",
        applies_to="salaried",
        is_active=True,
        employee_rate_pct=None,
        employer_rate_pct=None,
        ceiling_basis=None,
        ceiling_monthly_eur=None,
        employer_amount_cap_eur=None,
        rate_on_lohnsteuer_pct=None,
        pv_childless_surcharge_pct=None,
        pv_child_reduction_pct=None,
        employer_rate_asymmetric=False,
        rate_note=None,
    ),
    Row(
        reference_version="2026H1",
        valid_from=date(2026, 1, 1),
        valid_to=date(2026, 6, 30),
        lohnart_code="5040",
        name_de="PV-Beitrag AN",
        name_en="Long-term care (employee)",
        category="employee_deduction",
        bearer="employee",
        gl_account="3730",
        applies_to="gkv",
        is_active=True,
        employee_rate_pct=Decimal("1.80"),
        employer_rate_pct=None,
        ceiling_basis="KV_PV",
        ceiling_monthly_eur=Decimal("5812.50"),
        employer_amount_cap_eur=None,
        rate_on_lohnsteuer_pct=None,
        pv_childless_surcharge_pct=Decimal("0.60"),
        pv_child_reduction_pct=Decimal("0.25"),
        employer_rate_asymmetric=False,
        rate_note="Employee effective rate = base + childless surcharge (23+) - child reductions (2nd-5th). Saxony split out of scope.",  # noqa: E501
    ),
    Row(
        reference_version="2026H1",
        valid_from=date(2026, 1, 1),
        valid_to=date(2026, 6, 30),
        lohnart_code="6130",
        name_de="KV-Beitrag AG",
        name_en="Health (employer)",
        category="employer_contribution",
        bearer="employer",
        gl_account="6130",
        applies_to="gkv",
        is_active=True,
        employee_rate_pct=None,
        employer_rate_pct=Decimal("7.30"),
        ceiling_basis="KV_PV",
        ceiling_monthly_eur=Decimal("5812.50"),
        employer_amount_cap_eur=None,
        rate_on_lohnsteuer_pct=None,
        pv_childless_surcharge_pct=None,
        pv_child_reduction_pct=None,
        employer_rate_asymmetric=False,
        rate_note=None,
    ),
    # ---- duplicate of the lohnart_code="5040" row above ----
    Row(
        reference_version="2026H1",
        valid_from=date(2026, 1, 1),
        valid_to=date(2026, 6, 30),
        lohnart_code="5040",
        name_de="PV-Beitrag AN",
        name_en="Long-term care (employee)",
        category="employee_deduction",
        bearer="employee",
        gl_account="3730",
        applies_to="gkv",
        is_active=True,
        employee_rate_pct=Decimal("1.80"),
        employer_rate_pct=None,
        ceiling_basis="KV_PV",
        ceiling_monthly_eur=Decimal("5812.50"),
        employer_amount_cap_eur=None,
        rate_on_lohnsteuer_pct=None,
        pv_childless_surcharge_pct=Decimal("0.60"),
        pv_child_reduction_pct=Decimal("0.25"),
        employer_rate_asymmetric=False,
        rate_note="Employee effective rate = base + childless surcharge (23+) - child reductions (2nd-5th). Saxony split out of scope.",  # noqa: E501
    ),
]
