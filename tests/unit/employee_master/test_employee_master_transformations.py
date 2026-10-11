"""Unit tests of the employee_master transformations.

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

from payroll_pipeline_prototype.jobs.employee_master import (
    employee_master_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.employee_master import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
DEDUPLICATE_EMPLOYEE_MASTER_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
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
    ]
)

CREATE_TRACKED_COLUMNS_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
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
    ]
)

FILTER_VERSION_STARTS_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
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
    ]
)

ADD_VALIDITY_WINDOW_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
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
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
    ]
)

CREATE_EMPLOYEE_VERSION_ID_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("gender", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
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
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("employee_version_id", StringType(), nullable=True),
    ]
)


def test_deduplicate_employee_master(spark: SparkSession) -> None:
    """deduplicate_employee_master returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("employee_master")
    assert expected.DEDUPLICATE_EMPLOYEE_MASTER_REVIEWED, review_message(
        "employee_master",
        "DEDUPLICATE_EMPLOYEE_MASTER",
    )
    df_actual = job.deduplicate_employee_master(
        rows_to_df(
            spark,
            inputs.EMPLOYEE_MASTER_RAW_ROWS,
            schema_definitions.employee_master_raw_schema,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.DEDUPLICATE_EMPLOYEE_MASTER_ROWS,
        DEDUPLICATE_EMPLOYEE_MASTER_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_tracked_columns(spark: SparkSession) -> None:
    """create_tracked_columns returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("employee_master")
    assert expected.CREATE_TRACKED_COLUMNS_REVIEWED, review_message(
        "employee_master",
        "CREATE_TRACKED_COLUMNS",
    )
    df_actual = job.create_tracked_columns(
        rows_to_df(
            spark,
            expected.DEDUPLICATE_EMPLOYEE_MASTER_ROWS,
            DEDUPLICATE_EMPLOYEE_MASTER_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_TRACKED_COLUMNS_ROWS,
        CREATE_TRACKED_COLUMNS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_filter_version_starts(spark: SparkSession) -> None:
    """filter_version_starts returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("employee_master")
    assert expected.FILTER_VERSION_STARTS_REVIEWED, review_message(
        "employee_master",
        "FILTER_VERSION_STARTS",
    )
    df_actual = job.filter_version_starts(
        rows_to_df(spark, expected.CREATE_TRACKED_COLUMNS_ROWS, CREATE_TRACKED_COLUMNS_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.FILTER_VERSION_STARTS_ROWS,
        FILTER_VERSION_STARTS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_add_validity_window(spark: SparkSession) -> None:
    """add_validity_window returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("employee_master")
    assert expected.ADD_VALIDITY_WINDOW_REVIEWED, review_message(
        "employee_master",
        "ADD_VALIDITY_WINDOW",
    )
    df_actual = job.add_validity_window(
        rows_to_df(spark, expected.FILTER_VERSION_STARTS_ROWS, FILTER_VERSION_STARTS_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.ADD_VALIDITY_WINDOW_ROWS, ADD_VALIDITY_WINDOW_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_employee_version_id(spark: SparkSession) -> None:
    """create_employee_version_id returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("employee_master")
    assert expected.CREATE_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "employee_master",
        "CREATE_EMPLOYEE_VERSION_ID",
    )
    df_actual = job.create_employee_version_id(
        rows_to_df(spark, expected.ADD_VALIDITY_WINDOW_ROWS, ADD_VALIDITY_WINDOW_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_EMPLOYEE_VERSION_ID_ROWS,
        CREATE_EMPLOYEE_VERSION_ID_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_add_is_current_flag(spark: SparkSession) -> None:
    """add_is_current_flag returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("employee_master")
    assert expected.ADD_IS_CURRENT_FLAG_REVIEWED, review_message(
        "employee_master",
        "ADD_IS_CURRENT_FLAG",
    )
    df_actual = job.add_is_current_flag(
        rows_to_df(
            spark,
            expected.CREATE_EMPLOYEE_VERSION_ID_ROWS,
            CREATE_EMPLOYEE_VERSION_ID_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.ADD_IS_CURRENT_FLAG_ROWS,
        schema_definitions.employee_master_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
