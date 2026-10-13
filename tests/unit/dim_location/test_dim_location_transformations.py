"""Unit tests of the dim_location transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession

from payroll_pipeline_prototype.jobs.dim_location import (
    dim_location_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.dim_location import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)


def test_create_location_reference_key(spark: SparkSession) -> None:
    """create_location_reference_key returns exactly its reviewed expected rows."""
    assert inputs.DEDUPLICATE_LOCATION_REFERENCE_REVIEWED, review_message(
        "location_reference",
        "DEDUPLICATE_LOCATION_REFERENCE",
    )
    assert expected.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    df_actual = job.create_location_reference_key(
        rows_to_df(
            spark,
            inputs.LOCATION_REFERENCE_ROWS,
            schema_definitions.location_reference_schema,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_LOCATION_REFERENCE_KEY_ROWS,
        schema_definitions.dim_location_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
