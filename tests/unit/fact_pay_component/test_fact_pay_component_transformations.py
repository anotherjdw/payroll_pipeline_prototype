"""Unit tests of the fact_pay_component transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from payroll_pipeline_prototype.jobs.fact_pay_component import (
    fact_pay_component_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.fact_pay_component import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
ADD_PERIOD_END_DATE_SCHEMA = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=True),
        StructField("pay_period", StringType(), nullable=True),
        StructField("pay_date", DateType(), nullable=True),
        StructField("run_type", StringType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("location_code", StringType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("component_name", StringType(), nullable=True),
        StructField("bearer", StringType(), nullable=True),
        StructField("gl_account", StringType(), nullable=True),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=True),
        StructField("period_end_date", DateType(), nullable=True),
    ]
)

SELECT_EMPLOYEE_DATA_SCHEMA = StructType(
    [
        StructField("employee_id", StringType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("employee_key", StringType(), nullable=True),
    ]
)

SELECT_COST_CENTER_DATA_SCHEMA = StructType(
    [
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("cost_center_key", StringType(), nullable=True),
    ]
)

SELECT_LOCATION_DATA_SCHEMA = StructType(
    [
        StructField("location_code", StringType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("location_key", StringType(), nullable=True),
    ]
)

SELECT_PAY_COMPONENT_DATA_SCHEMA = StructType(
    [
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("pay_component_key", StringType(), nullable=True),
    ]
)

ADD_DATE_KEY_SCHEMA = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=True),
        StructField("pay_period", StringType(), nullable=True),
        StructField("pay_date", DateType(), nullable=True),
        StructField("run_type", StringType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("location_code", StringType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("component_name", StringType(), nullable=True),
        StructField("bearer", StringType(), nullable=True),
        StructField("gl_account", StringType(), nullable=True),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=True),
        StructField("period_end_date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
    ]
)

ADD_PAY_RUN_KEY_SCHEMA = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=True),
        StructField("pay_period", StringType(), nullable=True),
        StructField("pay_date", DateType(), nullable=True),
        StructField("run_type", StringType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("location_code", StringType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("component_name", StringType(), nullable=True),
        StructField("bearer", StringType(), nullable=True),
        StructField("gl_account", StringType(), nullable=True),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=True),
        StructField("period_end_date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("pay_run_key", StringType(), nullable=True),
    ]
)

PAYROLL_DATA_SCHEMA = StructType(
    [
        StructField("pay_period", StringType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("location_code", StringType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("gl_account", StringType(), nullable=True),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=True),
        StructField("period_end_date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("pay_run_key", StringType(), nullable=True),
    ]
)

JOIN_EMPLOYEE_KEY_TO_PAYROLL_SCHEMA = StructType(
    [
        StructField("pay_period", StringType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("location_code", StringType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("gl_account", StringType(), nullable=True),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=True),
        StructField("period_end_date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("pay_run_key", StringType(), nullable=True),
        StructField("employee_key", StringType(), nullable=True),
    ]
)

JOIN_COST_CENTER_KEY_TO_PAYROLL_SCHEMA = StructType(
    [
        StructField("pay_period", StringType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("location_code", StringType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("gl_account", StringType(), nullable=True),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=True),
        StructField("period_end_date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("pay_run_key", StringType(), nullable=True),
        StructField("employee_key", StringType(), nullable=True),
        StructField("cost_center_key", StringType(), nullable=True),
    ]
)

JOIN_LOCATION_KEY_TO_PAYROLL_SCHEMA = StructType(
    [
        StructField("pay_period", StringType(), nullable=True),
        StructField("employee_id", StringType(), nullable=True),
        StructField("cost_center_code", StringType(), nullable=True),
        StructField("location_code", StringType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("gl_account", StringType(), nullable=True),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=True),
        StructField("period_end_date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("pay_run_key", StringType(), nullable=True),
        StructField("employee_key", StringType(), nullable=True),
        StructField("cost_center_key", StringType(), nullable=True),
        StructField("location_key", StringType(), nullable=True),
    ]
)


def test_add_period_end_date(spark: SparkSession) -> None:
    """add_period_end_date returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.ADD_PERIOD_END_DATE_REVIEWED, review_message(
        "fact_pay_component",
        "ADD_PERIOD_END_DATE",
    )
    df_actual = job.add_period_end_date(
        rows_to_df(spark, inputs.PAYROLL_REGISTER_ROWS, schema_definitions.payroll_register_schema),
    )
    df_expected = rows_to_df(spark, expected.ADD_PERIOD_END_DATE_ROWS, ADD_PERIOD_END_DATE_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_select_employee_data(spark: SparkSession) -> None:
    """select_employee_data returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.SELECT_EMPLOYEE_DATA_REVIEWED, review_message(
        "fact_pay_component",
        "SELECT_EMPLOYEE_DATA",
    )
    df_actual = job.select_employee_data(
        rows_to_df(spark, inputs.DIM_EMPLOYEE_ROWS, schema_definitions.dim_employee_schema),
    )
    df_expected = rows_to_df(spark, expected.SELECT_EMPLOYEE_DATA_ROWS, SELECT_EMPLOYEE_DATA_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_select_cost_center_data(spark: SparkSession) -> None:
    """select_cost_center_data returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.SELECT_COST_CENTER_DATA_REVIEWED, review_message(
        "fact_pay_component",
        "SELECT_COST_CENTER_DATA",
    )
    df_actual = job.select_cost_center_data(
        rows_to_df(spark, inputs.DIM_COST_CENTER_ROWS, schema_definitions.dim_cost_center_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.SELECT_COST_CENTER_DATA_ROWS,
        SELECT_COST_CENTER_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_select_location_data(spark: SparkSession) -> None:
    """select_location_data returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.SELECT_LOCATION_DATA_REVIEWED, review_message(
        "fact_pay_component",
        "SELECT_LOCATION_DATA",
    )
    df_actual = job.select_location_data(
        rows_to_df(spark, inputs.DIM_LOCATION_ROWS, schema_definitions.dim_location_schema),
    )
    df_expected = rows_to_df(spark, expected.SELECT_LOCATION_DATA_ROWS, SELECT_LOCATION_DATA_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_select_pay_component_data(spark: SparkSession) -> None:
    """select_pay_component_data returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.SELECT_PAY_COMPONENT_DATA_REVIEWED, review_message(
        "fact_pay_component",
        "SELECT_PAY_COMPONENT_DATA",
    )
    df_actual = job.select_pay_component_data(
        rows_to_df(
            spark,
            inputs.DIM_PAY_COMPONENT_ROWS,
            schema_definitions.dim_pay_component_schema,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.SELECT_PAY_COMPONENT_DATA_ROWS,
        SELECT_PAY_COMPONENT_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_add_date_key(spark: SparkSession) -> None:
    """add_date_key returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.ADD_DATE_KEY_REVIEWED, review_message("fact_pay_component", "ADD_DATE_KEY")
    df_actual = job.add_date_key(
        rows_to_df(spark, expected.ADD_PERIOD_END_DATE_ROWS, ADD_PERIOD_END_DATE_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.ADD_DATE_KEY_ROWS, ADD_DATE_KEY_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_add_pay_run_key(spark: SparkSession) -> None:
    """add_pay_run_key returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.ADD_PAY_RUN_KEY_REVIEWED, review_message(
        "fact_pay_component",
        "ADD_PAY_RUN_KEY",
    )
    df_actual = job.add_pay_run_key(
        rows_to_df(spark, expected.ADD_DATE_KEY_ROWS, ADD_DATE_KEY_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.ADD_PAY_RUN_KEY_ROWS, ADD_PAY_RUN_KEY_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_payroll_data(spark: SparkSession) -> None:
    """payroll_data returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.PAYROLL_DATA_REVIEWED, review_message("fact_pay_component", "PAYROLL_DATA")
    df_actual = job.payroll_data(
        rows_to_df(spark, expected.ADD_PAY_RUN_KEY_ROWS, ADD_PAY_RUN_KEY_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.PAYROLL_DATA_ROWS, PAYROLL_DATA_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_employee_key_to_payroll(spark: SparkSession) -> None:
    """join_employee_key_to_payroll returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.JOIN_EMPLOYEE_KEY_TO_PAYROLL_REVIEWED, review_message(
        "fact_pay_component",
        "JOIN_EMPLOYEE_KEY_TO_PAYROLL",
    )
    df_actual = job.join_employee_key_to_payroll(
        rows_to_df(spark, expected.PAYROLL_DATA_ROWS, PAYROLL_DATA_SCHEMA),
        rows_to_df(spark, expected.SELECT_EMPLOYEE_DATA_ROWS, SELECT_EMPLOYEE_DATA_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_EMPLOYEE_KEY_TO_PAYROLL_ROWS,
        JOIN_EMPLOYEE_KEY_TO_PAYROLL_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_cost_center_key_to_payroll(spark: SparkSession) -> None:
    """join_cost_center_key_to_payroll returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.JOIN_COST_CENTER_KEY_TO_PAYROLL_REVIEWED, review_message(
        "fact_pay_component",
        "JOIN_COST_CENTER_KEY_TO_PAYROLL",
    )
    df_actual = job.join_cost_center_key_to_payroll(
        rows_to_df(
            spark,
            expected.JOIN_EMPLOYEE_KEY_TO_PAYROLL_ROWS,
            JOIN_EMPLOYEE_KEY_TO_PAYROLL_SCHEMA,
        ),
        rows_to_df(spark, expected.SELECT_COST_CENTER_DATA_ROWS, SELECT_COST_CENTER_DATA_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_COST_CENTER_KEY_TO_PAYROLL_ROWS,
        JOIN_COST_CENTER_KEY_TO_PAYROLL_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_location_key_to_payroll(spark: SparkSession) -> None:
    """join_location_key_to_payroll returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.JOIN_LOCATION_KEY_TO_PAYROLL_REVIEWED, review_message(
        "fact_pay_component",
        "JOIN_LOCATION_KEY_TO_PAYROLL",
    )
    df_actual = job.join_location_key_to_payroll(
        rows_to_df(
            spark,
            expected.JOIN_COST_CENTER_KEY_TO_PAYROLL_ROWS,
            JOIN_COST_CENTER_KEY_TO_PAYROLL_SCHEMA,
        ),
        rows_to_df(spark, expected.SELECT_LOCATION_DATA_ROWS, SELECT_LOCATION_DATA_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_LOCATION_KEY_TO_PAYROLL_ROWS,
        JOIN_LOCATION_KEY_TO_PAYROLL_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_pay_component_key_to_payroll(spark: SparkSession) -> None:
    """join_pay_component_key_to_payroll returns exactly its reviewed expected rows."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.JOIN_PAY_COMPONENT_KEY_TO_PAYROLL_REVIEWED, review_message(
        "fact_pay_component",
        "JOIN_PAY_COMPONENT_KEY_TO_PAYROLL",
    )
    df_actual = job.join_pay_component_key_to_payroll(
        rows_to_df(
            spark,
            expected.JOIN_LOCATION_KEY_TO_PAYROLL_ROWS,
            JOIN_LOCATION_KEY_TO_PAYROLL_SCHEMA,
        ),
        rows_to_df(
            spark,
            expected.SELECT_PAY_COMPONENT_DATA_ROWS,
            SELECT_PAY_COMPONENT_DATA_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_PAY_COMPONENT_KEY_TO_PAYROLL_ROWS,
        schema_definitions.fact_pay_component_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
