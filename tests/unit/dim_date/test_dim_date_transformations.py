"""Unit tests of the dim_date transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from payroll_pipeline_prototype.jobs.dim_date import (
    dim_date_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.dim_date import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
CREATE_YEAR_COLUMN_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
    ]
)

CREATE_QUARTER_COLUMN_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
    ]
)

CREATE_MONTH_NAME_COLUMN_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
        StructField("month_name", StringType(), nullable=True),
    ]
)

CREATE_PAY_PERIOD_DIMENSION_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
        StructField("month_name", StringType(), nullable=True),
        StructField("pay_period", StringType(), nullable=True),
    ]
)

ADD_MONTH_BOUNDARIES_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
        StructField("month_name", StringType(), nullable=True),
        StructField("pay_period", StringType(), nullable=True),
        StructField("month_begin", DateType(), nullable=True),
        StructField("month_end", DateType(), nullable=True),
        StructField("month_end_flag", BooleanType(), nullable=True),
    ]
)

ADD_PEAK_FLAG_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
        StructField("month_name", StringType(), nullable=True),
        StructField("pay_period", StringType(), nullable=True),
        StructField("month_begin", DateType(), nullable=True),
        StructField("month_end", DateType(), nullable=True),
        StructField("month_end_flag", BooleanType(), nullable=True),
        StructField("peak_flag", BooleanType(), nullable=True),
    ]
)


def test_create_year_column(spark: SparkSession) -> None:
    """create_year_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_date")
    assert expected.CREATE_YEAR_COLUMN_REVIEWED, review_message("dim_date", "CREATE_YEAR_COLUMN")
    df_actual = job.create_year_column(
        rows_to_df(spark, inputs.DATES_ROWS, schema_definitions.dates_schema),
    )
    df_expected = rows_to_df(spark, expected.CREATE_YEAR_COLUMN_ROWS, CREATE_YEAR_COLUMN_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_quarter_column(spark: SparkSession) -> None:
    """create_quarter_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_date")
    assert expected.CREATE_QUARTER_COLUMN_REVIEWED, review_message(
        "dim_date",
        "CREATE_QUARTER_COLUMN",
    )
    df_actual = job.create_quarter_column(
        rows_to_df(spark, expected.CREATE_YEAR_COLUMN_ROWS, CREATE_YEAR_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_QUARTER_COLUMN_ROWS,
        CREATE_QUARTER_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_month_name_column(spark: SparkSession) -> None:
    """create_month_name_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_date")
    assert expected.CREATE_MONTH_NAME_COLUMN_REVIEWED, review_message(
        "dim_date",
        "CREATE_MONTH_NAME_COLUMN",
    )
    df_actual = job.create_month_name_column(
        rows_to_df(spark, expected.CREATE_QUARTER_COLUMN_ROWS, CREATE_QUARTER_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_MONTH_NAME_COLUMN_ROWS,
        CREATE_MONTH_NAME_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_pay_period_dimension(spark: SparkSession) -> None:
    """create_pay_period_dimension returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_date")
    assert expected.CREATE_PAY_PERIOD_DIMENSION_REVIEWED, review_message(
        "dim_date",
        "CREATE_PAY_PERIOD_DIMENSION",
    )
    df_actual = job.create_pay_period_dimension(
        rows_to_df(spark, expected.CREATE_MONTH_NAME_COLUMN_ROWS, CREATE_MONTH_NAME_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_PAY_PERIOD_DIMENSION_ROWS,
        CREATE_PAY_PERIOD_DIMENSION_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_add_month_boundaries(spark: SparkSession) -> None:
    """add_month_boundaries returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_date")
    assert expected.ADD_MONTH_BOUNDARIES_REVIEWED, review_message(
        "dim_date",
        "ADD_MONTH_BOUNDARIES",
    )
    df_actual = job.add_month_boundaries(
        rows_to_df(
            spark,
            expected.CREATE_PAY_PERIOD_DIMENSION_ROWS,
            CREATE_PAY_PERIOD_DIMENSION_SCHEMA,
        ),
    )
    df_expected = rows_to_df(spark, expected.ADD_MONTH_BOUNDARIES_ROWS, ADD_MONTH_BOUNDARIES_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_add_peak_flag(spark: SparkSession) -> None:
    """add_peak_flag returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_date")
    assert expected.ADD_PEAK_FLAG_REVIEWED, review_message("dim_date", "ADD_PEAK_FLAG")
    df_actual = job.add_peak_flag(
        rows_to_df(spark, expected.ADD_MONTH_BOUNDARIES_ROWS, ADD_MONTH_BOUNDARIES_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.ADD_PEAK_FLAG_ROWS, ADD_PEAK_FLAG_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_add_date_key(spark: SparkSession) -> None:
    """add_date_key returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_date")
    assert expected.ADD_DATE_KEY_REVIEWED, review_message("dim_date", "ADD_DATE_KEY")
    df_actual = job.add_date_key(
        rows_to_df(spark, expected.ADD_PEAK_FLAG_ROWS, ADD_PEAK_FLAG_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.ADD_DATE_KEY_ROWS, schema_definitions.dim_date_schema)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
