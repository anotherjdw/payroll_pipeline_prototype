"""Unit tests of the dim_pay_component transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from payroll_pipeline_prototype.jobs.dim_pay_component import (
    dim_pay_component_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.dim_pay_component import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
CREATE_PAY_COMPONENT_KEY_SCHEMA = StructType(
    [
        StructField("reference_version", StringType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("lohnart_code", StringType(), nullable=True),
        StructField("name_de", StringType(), nullable=True),
        StructField("name_en", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("bearer", StringType(), nullable=True),
        StructField("gl_account", StringType(), nullable=True),
        StructField("applies_to", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
        StructField("employee_rate_pct", DecimalType(5, 2), nullable=True),
        StructField("employer_rate_pct", DecimalType(5, 2), nullable=True),
        StructField("ceiling_basis", StringType(), nullable=True),
        StructField("ceiling_monthly_eur", DecimalType(10, 2), nullable=True),
        StructField("employer_amount_cap_eur", DecimalType(10, 2), nullable=True),
        StructField("rate_on_lohnsteuer_pct", DecimalType(5, 2), nullable=True),
        StructField("pv_childless_surcharge_pct", DecimalType(5, 2), nullable=True),
        StructField("pv_child_reduction_pct", DecimalType(5, 2), nullable=True),
        StructField("employer_rate_asymmetric", BooleanType(), nullable=True),
        StructField("rate_note", StringType(), nullable=True),
        StructField("pay_component_key", StringType(), nullable=True),
    ]
)


def test_create_pay_component_key(spark: SparkSession) -> None:
    """create_pay_component_key returns exactly its reviewed expected rows."""
    assert inputs.DEDUPLICATE_PAY_COMPONENT_REFERENCE_REVIEWED, review_message(
        "pay_component_reference",
        "DEDUPLICATE_PAY_COMPONENT_REFERENCE",
    )
    assert expected.CREATE_PAY_COMPONENT_KEY_REVIEWED, review_message(
        "dim_pay_component",
        "CREATE_PAY_COMPONENT_KEY",
    )
    df_actual = job.create_pay_component_key(
        rows_to_df(
            spark,
            inputs.PAY_COMPONENT_REFERENCE_ROWS,
            schema_definitions.pay_component_reference_schema,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_PAY_COMPONENT_KEY_ROWS,
        CREATE_PAY_COMPONENT_KEY_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_add_ceiling_capped_flag(spark: SparkSession) -> None:
    """add_ceiling_capped_flag returns exactly its reviewed expected rows."""
    assert inputs.DEDUPLICATE_PAY_COMPONENT_REFERENCE_REVIEWED, review_message(
        "pay_component_reference",
        "DEDUPLICATE_PAY_COMPONENT_REFERENCE",
    )
    assert expected.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    df_actual = job.add_ceiling_capped_flag(
        rows_to_df(spark, expected.CREATE_PAY_COMPONENT_KEY_ROWS, CREATE_PAY_COMPONENT_KEY_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.ADD_CEILING_CAPPED_FLAG_ROWS,
        schema_definitions.dim_pay_component_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
