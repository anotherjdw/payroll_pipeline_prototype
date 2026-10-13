"""Unit tests of the dim_employee transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
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

from payroll_pipeline_prototype.jobs.dim_employee import (
    dim_employee_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.dim_employee import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
ADD_BIRTH_YEAR_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("nationality", StringType(), nullable=True),
        StructField("marital_status", StringType(), nullable=True),
        StructField("steuer_id", StringType(), nullable=True),
        StructField("sozialversicherungsnummer", StringType(), nullable=True),
        StructField("iban", StringType(), nullable=True),
        StructField("street_address", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("work_location_code", StringType(), nullable=True),
        StructField("work_location_name", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("pay_type", StringType(), nullable=True),
        StructField("employment_type", StringType(), nullable=True),
        StructField("contracted_hours_per_month", DecimalType(6, 2), nullable=True),
        StructField("annual_gross_eur", DecimalType(10, 2), nullable=True),
        StructField("hourly_rate_eur", DecimalType(5, 2), nullable=True),
        StructField("steuerklasse", StringType(), nullable=True),
        StructField("num_children", IntegerType(), nullable=True),
        StructField("childless_surcharge_applies", BooleanType(), nullable=True),
        StructField("religious_affiliation", StringType(), nullable=True),
        StructField("church_tax_applies", BooleanType(), nullable=True),
        StructField("health_insurance_type", StringType(), nullable=True),
        StructField("health_insurance_fund", StringType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("hire_date", DateType(), nullable=True),
        StructField("termination_date", DateType(), nullable=True),
        StructField("job_level", StringType(), nullable=True),
        StructField("extract_timestamp", TimestampType(), nullable=True),
        StructField("is_version_start", BooleanType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("employee_version_id", StringType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=True),
        StructField("birth_year", IntegerType(), nullable=True),
    ]
)

DROP_PII_COLUMNS_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("work_location_code", StringType(), nullable=True),
        StructField("work_location_name", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("pay_type", StringType(), nullable=True),
        StructField("employment_type", StringType(), nullable=True),
        StructField("contracted_hours_per_month", DecimalType(6, 2), nullable=True),
        StructField("annual_gross_eur", DecimalType(10, 2), nullable=True),
        StructField("hourly_rate_eur", DecimalType(5, 2), nullable=True),
        StructField("steuerklasse", StringType(), nullable=True),
        StructField("num_children", IntegerType(), nullable=True),
        StructField("childless_surcharge_applies", BooleanType(), nullable=True),
        StructField("church_tax_applies", BooleanType(), nullable=True),
        StructField("health_insurance_type", StringType(), nullable=True),
        StructField("health_insurance_fund", StringType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("hire_date", DateType(), nullable=True),
        StructField("termination_date", DateType(), nullable=True),
        StructField("job_level", StringType(), nullable=True),
        StructField("is_version_start", BooleanType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("employee_version_id", StringType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=True),
        StructField("birth_year", IntegerType(), nullable=True),
    ]
)

ADJUST_FIRST_VERSION_VALID_FROM_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("work_location_code", StringType(), nullable=True),
        StructField("work_location_name", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("pay_type", StringType(), nullable=True),
        StructField("employment_type", StringType(), nullable=True),
        StructField("contracted_hours_per_month", DecimalType(6, 2), nullable=True),
        StructField("annual_gross_eur", DecimalType(10, 2), nullable=True),
        StructField("hourly_rate_eur", DecimalType(5, 2), nullable=True),
        StructField("steuerklasse", StringType(), nullable=True),
        StructField("num_children", IntegerType(), nullable=True),
        StructField("childless_surcharge_applies", BooleanType(), nullable=True),
        StructField("church_tax_applies", BooleanType(), nullable=True),
        StructField("health_insurance_type", StringType(), nullable=True),
        StructField("health_insurance_fund", StringType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("hire_date", DateType(), nullable=True),
        StructField("termination_date", DateType(), nullable=True),
        StructField("job_level", StringType(), nullable=True),
        StructField("is_version_start", BooleanType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("employee_version_id", StringType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=True),
        StructField("birth_year", IntegerType(), nullable=True),
    ]
)


def test_add_birth_year(spark: SparkSession) -> None:
    """add_birth_year returns exactly its reviewed expected rows."""
    assert inputs.ADD_IS_CURRENT_FLAG_REVIEWED, review_message(
        "employee_master",
        "ADD_IS_CURRENT_FLAG",
    )
    assert expected.ADD_BIRTH_YEAR_REVIEWED, review_message("dim_employee", "ADD_BIRTH_YEAR")
    df_actual = job.add_birth_year(
        rows_to_df(spark, inputs.EMPLOYEE_MASTER_ROWS, schema_definitions.employee_master_schema),
    )
    df_expected = rows_to_df(spark, expected.ADD_BIRTH_YEAR_ROWS, ADD_BIRTH_YEAR_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_drop_pii_columns(spark: SparkSession) -> None:
    """drop_pii_columns returns exactly its reviewed expected rows."""
    assert inputs.ADD_IS_CURRENT_FLAG_REVIEWED, review_message(
        "employee_master",
        "ADD_IS_CURRENT_FLAG",
    )
    assert expected.DROP_PII_COLUMNS_REVIEWED, review_message("dim_employee", "DROP_PII_COLUMNS")
    df_actual = job.drop_pii_columns(
        rows_to_df(spark, expected.ADD_BIRTH_YEAR_ROWS, ADD_BIRTH_YEAR_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.DROP_PII_COLUMNS_ROWS, DROP_PII_COLUMNS_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_adjust_first_version_valid_from(spark: SparkSession) -> None:
    """adjust_first_version_valid_from returns exactly its reviewed expected rows."""
    assert inputs.ADD_IS_CURRENT_FLAG_REVIEWED, review_message(
        "employee_master",
        "ADD_IS_CURRENT_FLAG",
    )
    assert expected.ADJUST_FIRST_VERSION_VALID_FROM_REVIEWED, review_message(
        "dim_employee",
        "ADJUST_FIRST_VERSION_VALID_FROM",
    )
    df_actual = job.adjust_first_version_valid_from(
        rows_to_df(spark, expected.DROP_PII_COLUMNS_ROWS, DROP_PII_COLUMNS_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.ADJUST_FIRST_VERSION_VALID_FROM_ROWS,
        ADJUST_FIRST_VERSION_VALID_FROM_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_rename_employee_version_id(spark: SparkSession) -> None:
    """rename_employee_version_id returns exactly its reviewed expected rows."""
    assert inputs.ADD_IS_CURRENT_FLAG_REVIEWED, review_message(
        "employee_master",
        "ADD_IS_CURRENT_FLAG",
    )
    assert expected.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    df_actual = job.rename_employee_version_id(
        rows_to_df(
            spark,
            expected.ADJUST_FIRST_VERSION_VALID_FROM_ROWS,
            ADJUST_FIRST_VERSION_VALID_FROM_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.RENAME_EMPLOYEE_VERSION_ID_ROWS,
        schema_definitions.dim_employee_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
