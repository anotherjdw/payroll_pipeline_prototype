"""Unit tests of the cost_center_reference transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession

from payroll_pipeline_prototype.jobs.cost_center_reference import (
    cost_center_reference_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.cost_center_reference import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)


def test_deduplicate_cost_center_reference(spark: SparkSession) -> None:
    """deduplicate_cost_center_reference returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("cost_center_reference")
    assert expected.DEDUPLICATE_COST_CENTER_REFERENCE_REVIEWED, review_message(
        "cost_center_reference",
        "DEDUPLICATE_COST_CENTER_REFERENCE",
    )
    df_actual = job.deduplicate_cost_center_reference(
        rows_to_df(
            spark,
            inputs.COST_CENTER_REFERENCE_RAW_ROWS,
            schema_definitions.cost_center_reference_raw_schema,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.DEDUPLICATE_COST_CENTER_REFERENCE_ROWS,
        schema_definitions.cost_center_reference_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
