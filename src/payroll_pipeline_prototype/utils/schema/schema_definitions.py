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
    TimestampType,
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

dates_schema = StructType(
    [
        StructField("date", DateType(), nullable=False),
    ]
)

dim_cost_center_schema = StructType(
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
        StructField("cost_center_key", StringType(), nullable=False),
    ]
)

dim_date_schema = StructType(
    [
        StructField("date", DateType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField("quarter", IntegerType(), nullable=False),
        StructField("month_name", StringType(), nullable=False),
        StructField("pay_period", StringType(), nullable=False),
        StructField("month_begin", DateType(), nullable=False),
        StructField("month_end", DateType(), nullable=False),
        StructField("month_end_flag", BooleanType(), nullable=False),
        StructField("peak_flag", BooleanType(), nullable=False),
        StructField("date_key", IntegerType(), nullable=False),
    ]
)

dim_employee_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("employee_id", StringType(), nullable=False),
        StructField("birth_year", IntegerType(), nullable=False),
        StructField("work_location_code", StringType(), nullable=False),
        StructField("work_location_name", StringType(), nullable=False),
        StructField("department", StringType(), nullable=False),
        StructField("cost_center_code", StringType(), nullable=False),
        StructField("role", StringType(), nullable=False),
        StructField("pay_type", StringType(), nullable=False),
        StructField("employment_type", StringType(), nullable=False),
        StructField("contracted_hours_per_month", DecimalType(6, 2), nullable=False),
        StructField("annual_gross_eur", DecimalType(10, 2), nullable=True),
        StructField("hourly_rate_eur", DecimalType(5, 2), nullable=True),
        StructField("steuerklasse", StringType(), nullable=True),
        StructField("num_children", IntegerType(), nullable=False),
        StructField("childless_surcharge_applies", BooleanType(), nullable=False),
        StructField("church_tax_applies", BooleanType(), nullable=False),
        StructField("health_insurance_type", StringType(), nullable=False),
        StructField("health_insurance_fund", StringType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("hire_date", DateType(), nullable=False),
        StructField("termination_date", DateType(), nullable=True),
        StructField("job_level", StringType(), nullable=True),
        StructField("is_version_start", BooleanType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("valid_to", DateType(), nullable=True),
        StructField("employee_key", StringType(), nullable=False),
        StructField("is_current", BooleanType(), nullable=False),
    ]
)

dim_location_schema = StructType(
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
        StructField("location_key", StringType(), nullable=False),
    ]
)

dim_pay_component_schema = StructType(
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
        StructField("pay_component_key", StringType(), nullable=False),
        StructField("is_ceiling_capped", BooleanType(), nullable=False),
    ]
)

dim_pay_run_schema = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=False),
        StructField("pay_date", DateType(), nullable=False),
        StructField("run_type", StringType(), nullable=False),
        StructField("pay_run_key", StringType(), nullable=False),
    ]
)

employee_master_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("employee_id", StringType(), nullable=False),
        StructField("first_name", StringType(), nullable=False),
        StructField("last_name", StringType(), nullable=False),
        StructField("gender", StringType(), nullable=False),
        StructField("date_of_birth", DateType(), nullable=False),
        StructField("nationality", StringType(), nullable=True),
        StructField("marital_status", StringType(), nullable=True),
        StructField("steuer_id", StringType(), nullable=False),
        StructField("sozialversicherungsnummer", StringType(), nullable=False),
        StructField("iban", StringType(), nullable=False),
        StructField("street_address", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=False),
        StructField("work_location_code", StringType(), nullable=False),
        StructField("work_location_name", StringType(), nullable=False),
        StructField("department", StringType(), nullable=False),
        StructField("cost_center_code", StringType(), nullable=False),
        StructField("role", StringType(), nullable=False),
        StructField("pay_type", StringType(), nullable=False),
        StructField("employment_type", StringType(), nullable=False),
        StructField("contracted_hours_per_month", DecimalType(6, 2), nullable=False),
        StructField("annual_gross_eur", DecimalType(10, 2), nullable=True),
        StructField("hourly_rate_eur", DecimalType(5, 2), nullable=True),
        StructField("steuerklasse", StringType(), nullable=True),
        StructField("num_children", IntegerType(), nullable=False),
        StructField("childless_surcharge_applies", BooleanType(), nullable=False),
        StructField("religious_affiliation", StringType(), nullable=True),
        StructField("church_tax_applies", BooleanType(), nullable=False),
        StructField("health_insurance_type", StringType(), nullable=False),
        StructField("health_insurance_fund", StringType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("hire_date", DateType(), nullable=False),
        StructField("termination_date", DateType(), nullable=True),
        StructField("job_level", StringType(), nullable=True),
        StructField("extract_timestamp", TimestampType(), nullable=False),
        StructField("is_version_start", BooleanType(), nullable=False),
        StructField("valid_from", DateType(), nullable=False),
        StructField("valid_to", DateType(), nullable=True),
        StructField("employee_version_id", StringType(), nullable=False),
        StructField("is_current", BooleanType(), nullable=False),
    ]
)

employee_master_raw_schema = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=False),
        StructField("employee_id", StringType(), nullable=False),
        StructField("first_name", StringType(), nullable=False),
        StructField("last_name", StringType(), nullable=False),
        StructField("gender", StringType(), nullable=False),
        StructField("date_of_birth", DateType(), nullable=False),
        StructField("nationality", StringType(), nullable=True),
        StructField("marital_status", StringType(), nullable=True),
        StructField("steuer_id", StringType(), nullable=False),
        StructField("sozialversicherungsnummer", StringType(), nullable=False),
        StructField("iban", StringType(), nullable=False),
        StructField("street_address", StringType(), nullable=False),
        StructField("postal_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=False),
        StructField("work_location_code", StringType(), nullable=False),
        StructField("work_location_name", StringType(), nullable=False),
        StructField("department", StringType(), nullable=False),
        StructField("cost_center_code", StringType(), nullable=False),
        StructField("role", StringType(), nullable=False),
        StructField("pay_type", StringType(), nullable=False),
        StructField("employment_type", StringType(), nullable=False),
        StructField("contracted_hours_per_month", DecimalType(6, 2), nullable=False),
        StructField("annual_gross_eur", DecimalType(10, 2), nullable=True),
        StructField("hourly_rate_eur", DecimalType(5, 2), nullable=True),
        StructField("steuerklasse", StringType(), nullable=True),
        StructField("num_children", IntegerType(), nullable=False),
        StructField("childless_surcharge_applies", BooleanType(), nullable=False),
        StructField("religious_affiliation", StringType(), nullable=True),
        StructField("church_tax_applies", BooleanType(), nullable=False),
        StructField("health_insurance_type", StringType(), nullable=False),
        StructField("health_insurance_fund", StringType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("hire_date", DateType(), nullable=False),
        StructField("termination_date", DateType(), nullable=True),
        StructField("job_level", StringType(), nullable=True),
        StructField("extract_timestamp", TimestampType(), nullable=False),
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
