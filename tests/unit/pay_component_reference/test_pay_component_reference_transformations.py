"""Unit tests of the pay_component_reference transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession

from payroll_pipeline_prototype.jobs.pay_component_reference import (
    pay_component_reference_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.pay_component_reference import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)


def test_deduplicate_pay_component_reference(spark: SparkSession) -> None:
    """deduplicate_pay_component_reference returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("pay_component_reference")
    assert expected.DEDUPLICATE_PAY_COMPONENT_REFERENCE_REVIEWED, review_message(
        "pay_component_reference",
        "DEDUPLICATE_PAY_COMPONENT_REFERENCE",
    )
    df_actual = job.deduplicate_pay_component_reference(
        rows_to_df(
            spark,
            inputs.PAY_COMPONENT_REFERENCE_RAW_ROWS,
            schema_definitions.pay_component_reference_raw_schema,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.DEDUPLICATE_PAY_COMPONENT_REFERENCE_ROWS,
        schema_definitions.pay_component_reference_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
