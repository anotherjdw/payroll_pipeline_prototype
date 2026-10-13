"""Unit tests of the dim_cost_center transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession

from payroll_pipeline_prototype.jobs.dim_cost_center import (
    dim_cost_center_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.dim_cost_center import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)


def test_create_cost_center_key(spark: SparkSession) -> None:
    """create_cost_center_key returns exactly its reviewed expected rows."""
    assert inputs.DEDUPLICATE_COST_CENTER_REFERENCE_REVIEWED, review_message(
        "cost_center_reference",
        "DEDUPLICATE_COST_CENTER_REFERENCE",
    )
    assert expected.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    df_actual = job.create_cost_center_key(
        rows_to_df(
            spark,
            inputs.COST_CENTER_REFERENCE_ROWS,
            schema_definitions.cost_center_reference_schema,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_COST_CENTER_KEY_ROWS,
        schema_definitions.dim_cost_center_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
