"""Unit tests of the payroll_register transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from payroll_pipeline_prototype.jobs.payroll_register import (
    payroll_register_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.payroll_register import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
GENERATE_RETRO_PAY_DATA_SCHEMA = StructType(
    [
        StructField("employee_id", StringType(), nullable=True),
        StructField("pay_period", StringType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("amount_eur_retro", DecimalType(14, 2), nullable=True),
    ]
)

FILTER_REGULAR_PAY_DATA_SCHEMA = StructType(
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
    ]
)

JOIN_RETRO_PAY_DATA_SCHEMA = StructType(
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
        StructField("amount_eur_retro", DecimalType(14, 2), nullable=True),
    ]
)


def test_generate_retro_pay_data(spark: SparkSession) -> None:
    """generate_retro_pay_data returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("payroll_register")
    assert expected.GENERATE_RETRO_PAY_DATA_REVIEWED, review_message(
        "payroll_register",
        "GENERATE_RETRO_PAY_DATA",
    )
    df_actual = job.generate_retro_pay_data(
        rows_to_df(
            spark,
            inputs.PAYROLL_REGISTER_RAW_ROWS,
            schema_definitions.payroll_register_raw_schema,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.GENERATE_RETRO_PAY_DATA_ROWS,
        GENERATE_RETRO_PAY_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_filter_regular_pay_data(spark: SparkSession) -> None:
    """filter_regular_pay_data returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("payroll_register")
    assert expected.FILTER_REGULAR_PAY_DATA_REVIEWED, review_message(
        "payroll_register",
        "FILTER_REGULAR_PAY_DATA",
    )
    df_actual = job.filter_regular_pay_data(
        rows_to_df(
            spark,
            inputs.PAYROLL_REGISTER_RAW_ROWS,
            schema_definitions.payroll_register_raw_schema,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.FILTER_REGULAR_PAY_DATA_ROWS,
        FILTER_REGULAR_PAY_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_retro_pay_data(spark: SparkSession) -> None:
    """join_retro_pay_data returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("payroll_register")
    assert expected.JOIN_RETRO_PAY_DATA_REVIEWED, review_message(
        "payroll_register",
        "JOIN_RETRO_PAY_DATA",
    )
    df_actual = job.join_retro_pay_data(
        rows_to_df(spark, expected.FILTER_REGULAR_PAY_DATA_ROWS, FILTER_REGULAR_PAY_DATA_SCHEMA),
        rows_to_df(spark, expected.GENERATE_RETRO_PAY_DATA_ROWS, GENERATE_RETRO_PAY_DATA_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.JOIN_RETRO_PAY_DATA_ROWS, JOIN_RETRO_PAY_DATA_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_combine_pay_columns(spark: SparkSession) -> None:
    """combine_pay_columns returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("payroll_register")
    assert expected.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    df_actual = job.combine_pay_columns(
        rows_to_df(spark, expected.JOIN_RETRO_PAY_DATA_ROWS, JOIN_RETRO_PAY_DATA_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.COMBINE_PAY_COLUMNS_ROWS,
        schema_definitions.payroll_register_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
