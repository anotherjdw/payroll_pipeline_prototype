"""Unit tests of the dim_pay_run transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
)

from payroll_pipeline_prototype.jobs.dim_pay_run import (
    dim_pay_run_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.dim_pay_run import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
SELECT_PAY_RUN_COLUMNS_SCHEMA = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=True),
        StructField("pay_date", DateType(), nullable=True),
        StructField("run_type", StringType(), nullable=True),
    ]
)

DEDUPLICATE_PAY_RUN_DATA_SCHEMA = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=True),
        StructField("pay_date", DateType(), nullable=True),
        StructField("run_type", StringType(), nullable=True),
    ]
)


def test_select_pay_run_columns(spark: SparkSession) -> None:
    """select_pay_run_columns returns exactly its reviewed expected rows."""
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.SELECT_PAY_RUN_COLUMNS_REVIEWED, review_message(
        "dim_pay_run",
        "SELECT_PAY_RUN_COLUMNS",
    )
    df_actual = job.select_pay_run_columns(
        rows_to_df(spark, inputs.PAYROLL_REGISTER_ROWS, schema_definitions.payroll_register_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.SELECT_PAY_RUN_COLUMNS_ROWS,
        SELECT_PAY_RUN_COLUMNS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_deduplicate_pay_run_data(spark: SparkSession) -> None:
    """deduplicate_pay_run_data returns exactly its reviewed expected rows."""
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.DEDUPLICATE_PAY_RUN_DATA_REVIEWED, review_message(
        "dim_pay_run",
        "DEDUPLICATE_PAY_RUN_DATA",
    )
    df_actual = job.deduplicate_pay_run_data(
        rows_to_df(spark, expected.SELECT_PAY_RUN_COLUMNS_ROWS, SELECT_PAY_RUN_COLUMNS_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.DEDUPLICATE_PAY_RUN_DATA_ROWS,
        DEDUPLICATE_PAY_RUN_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_pay_run_key(spark: SparkSession) -> None:
    """create_pay_run_key returns exactly its reviewed expected rows."""
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.CREATE_PAY_RUN_KEY_REVIEWED, review_message("dim_pay_run", "CREATE_PAY_RUN_KEY")
    df_actual = job.create_pay_run_key(
        rows_to_df(spark, expected.DEDUPLICATE_PAY_RUN_DATA_ROWS, DEDUPLICATE_PAY_RUN_DATA_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_PAY_RUN_KEY_ROWS,
        schema_definitions.dim_pay_run_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
