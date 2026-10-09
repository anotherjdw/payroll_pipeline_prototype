"""One Spark StructType per data contract used by a rendered job.

Tool-owned and rebuilt whole from the contracts in `contracts/`. Edit the contracts, not
this file.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

cost_center_reference_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("cost_center_code", StringType(), nullable=False),
        StructField("cost_center_name", StringType(), nullable=False),
        StructField("department_name", StringType(), nullable=False),
        StructField("cost_center_type", StringType(), nullable=False),
        StructField("default_location_code", StringType(), nullable=False),
        StructField("accident_insurance_rate_pct", DecimalType(5, 2), nullable=False),
        StructField("accident_risk_class", StringType(), nullable=False),
        StructField("gl_cost_center_segment", StringType(), nullable=False),
        StructField("planned_headcount_fte", IntegerType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("valid_to", DateType(), nullable=False),
    ]
)

cost_center_reference_raw_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("cost_center_code", StringType(), nullable=False),
        StructField("cost_center_name", StringType(), nullable=False),
        StructField("department_name", StringType(), nullable=False),
        StructField("cost_center_type", StringType(), nullable=False),
        StructField("default_location_code", StringType(), nullable=False),
        StructField("accident_insurance_rate_pct", DecimalType(5, 2), nullable=False),
        StructField("accident_risk_class", StringType(), nullable=False),
        StructField("gl_cost_center_segment", StringType(), nullable=False),
        StructField("planned_headcount_fte", IntegerType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("valid_to", DateType(), nullable=False),
    ]
)

location_reference_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("location_code", StringType(), nullable=False),
        StructField("location_name", StringType(), nullable=False),
        StructField("site_type", StringType(), nullable=False),
        StructField("street_address", StringType(), nullable=False),
        StructField("postal_code", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("bundesland", StringType(), nullable=False),
        StructField("bundesland_code", StringType(), nullable=False),
        StructField("country", StringType(), nullable=False),
        StructField("country_code", StringType(), nullable=False),
        StructField("timezone", StringType(), nullable=False),
        StructField("opened_date", DateType(), nullable=False),
        StructField("church_tax_rate_pct", DecimalType(5, 2), nullable=False),
        StructField("pv_regional_variant", StringType(), nullable=False),
        StructField("rv_av_ceiling_region_pre_2025", StringType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("valid_to", DateType(), nullable=False),
    ]
)

location_reference_raw_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("location_code", StringType(), nullable=False),
        StructField("location_name", StringType(), nullable=False),
        StructField("site_type", StringType(), nullable=False),
        StructField("street_address", StringType(), nullable=False),
        StructField("postal_code", StringType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("bundesland", StringType(), nullable=False),
        StructField("bundesland_code", StringType(), nullable=False),
        StructField("country", StringType(), nullable=False),
        StructField("country_code", StringType(), nullable=False),
        StructField("timezone", StringType(), nullable=False),
        StructField("opened_date", DateType(), nullable=False),
        StructField("church_tax_rate_pct", DecimalType(5, 2), nullable=False),
        StructField("pv_regional_variant", StringType(), nullable=False),
        StructField("rv_av_ceiling_region_pre_2025", StringType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("valid_to", DateType(), nullable=False),
    ]
)

pay_component_reference_schema = StructType(
    [
        StructField("reference_version", StringType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("valid_to", DateType(), nullable=False),
        StructField("lohnart_code", StringType(), nullable=False),
        StructField("name_de", StringType(), nullable=False),
        StructField("name_en", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("bearer", StringType(), nullable=False),
        StructField("gl_account", StringType(), nullable=False),
        StructField("applies_to", StringType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("employee_rate_pct", DecimalType(5, 2), nullable=True),
        StructField("employer_rate_pct", DecimalType(5, 2), nullable=True),
        StructField("ceiling_basis", StringType(), nullable=True),
        StructField("ceiling_monthly_eur", DecimalType(10, 2), nullable=True),
        StructField("employer_amount_cap_eur", DecimalType(10, 2), nullable=True),
        StructField("rate_on_lohnsteuer_pct", DecimalType(5, 2), nullable=True),
        StructField("pv_childless_surcharge_pct", DecimalType(5, 2), nullable=True),
        StructField("pv_child_reduction_pct", DecimalType(5, 2), nullable=True),
        StructField("employer_rate_asymmetric", BooleanType(), nullable=False),
        StructField("rate_note", StringType(), nullable=True),
    ]
)

pay_component_reference_raw_schema = StructType(
    [
        StructField("reference_version", StringType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("valid_to", DateType(), nullable=False),
        StructField("lohnart_code", StringType(), nullable=False),
        StructField("name_de", StringType(), nullable=False),
        StructField("name_en", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("bearer", StringType(), nullable=False),
        StructField("gl_account", StringType(), nullable=False),
        StructField("applies_to", StringType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("employee_rate_pct", DecimalType(5, 2), nullable=True),
        StructField("employer_rate_pct", DecimalType(5, 2), nullable=True),
        StructField("ceiling_basis", StringType(), nullable=True),
        StructField("ceiling_monthly_eur", DecimalType(10, 2), nullable=True),
        StructField("employer_amount_cap_eur", DecimalType(10, 2), nullable=True),
        StructField("rate_on_lohnsteuer_pct", DecimalType(5, 2), nullable=True),
        StructField("pv_childless_surcharge_pct", DecimalType(5, 2), nullable=True),
        StructField("pv_child_reduction_pct", DecimalType(5, 2), nullable=True),
        StructField("employer_rate_asymmetric", BooleanType(), nullable=False),
        StructField("rate_note", StringType(), nullable=True),
    ]
)

payroll_register_schema = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=False),
        StructField("pay_period", StringType(), nullable=False),
        StructField("pay_date", DateType(), nullable=False),
        StructField("run_type", StringType(), nullable=False),
        StructField("employee_id", StringType(), nullable=False),
        StructField("cost_center_code", StringType(), nullable=False),
        StructField("location_code", StringType(), nullable=False),
        StructField("lohnart_code", StringType(), nullable=False),
        StructField("component_name", StringType(), nullable=False),
        StructField("bearer", StringType(), nullable=False),
        StructField("gl_account", StringType(), nullable=False),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=False),
    ]
)

payroll_register_raw_schema = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=False),
        StructField("pay_period", StringType(), nullable=False),
        StructField("pay_date", DateType(), nullable=False),
        StructField("run_type", StringType(), nullable=False),
        StructField("employee_id", StringType(), nullable=False),
        StructField("cost_center_code", StringType(), nullable=False),
        StructField("location_code", StringType(), nullable=False),
        StructField("lohnart_code", StringType(), nullable=False),
        StructField("component_name", StringType(), nullable=False),
        StructField("bearer", StringType(), nullable=False),
        StructField("gl_account", StringType(), nullable=False),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=False),
    ]
)
