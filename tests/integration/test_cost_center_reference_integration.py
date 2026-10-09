"""Integration tests of the cost_center_reference run steps on local Spark.

Each test writes the job's input rows from tests/fixtures/ to disk as the job reads them: in
each source's file format and partition layout. test_transform and test_write compare with
the job's output, the reviewed expected rows of its last transformation.
"""

from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from payroll_pipeline_prototype.jobs.cost_center_reference import (
    cost_center_reference_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.cost_center_reference import expected, inputs
from tests.helpers import (
    SourceFixture,
    column_types,
    create_catalog_table,
    fixture_dates,
    inputs_message,
    processing_dates,
    registered_partitions,
    review_message,
    row_key,
    rows_to_df,
    write_sources,
)

# How the job reads each source, and the input rows the tests write for it.
SOURCES = {
    "cost_center_reference_raw": SourceFixture(
        argument="COST_CENTER_REFERENCE_RAW_INPUT_PATH",
        rows=inputs.COST_CENTER_REFERENCE_RAW_ROWS,
        schema=schema_definitions.cost_center_reference_raw_schema,
        file_format="json",
        partition_key="snapshot_date",
    ),
}

OUTPUT_SCHEMA = schema_definitions.cost_center_reference_schema

# The output contract's non-nullable columns.
NOT_NULL_COLUMNS = [
    "snapshot_date",
    "cost_center_code",
    "cost_center_name",
    "department_name",
    "cost_center_type",
    "default_location_code",
    "accident_insurance_rate_pct",
    "accident_risk_class",
    "gl_cost_center_segment",
    "planned_headcount_fte",
    "is_active",
    "valid_from",
    "valid_to",
]


@pytest.fixture
def output_table(spark: SparkSession, test_database: str, test_dir: Path) -> Iterator[str]:
    """A local table shaped like the job's Glue table, at the folder the test writes to.

    It starts with no partitions registered, like the Glue table before the job's first run.
    """
    table = f"{test_database}.cost_center_reference"
    create_catalog_table(spark, table, OUTPUT_SCHEMA, "snapshot_date", test_dir)
    yield table
    # An external table: dropping it removes the catalog entry, not the files.
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_read(spark: SparkSession, test_dir: Path) -> None:
    """read() loads every input row of every source, in its contract's column types."""
    assert inputs.INPUTS_FILLED, inputs_message("cost_center_reference")
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    sources = job.read(args, processing_dates(SOURCES))
    assert set(sources) == set(SOURCES)
    for alias, source in SOURCES.items():
        assert sources[alias].count() == len(source.rows), alias
        assert column_types(sources[alias].schema) == column_types(source.schema), alias


def test_read_no_data_raises(spark: SparkSession, test_dir: Path) -> None:
    """read() fails for a date with no input rows, since a source may not be empty."""
    assert inputs.INPUTS_FILLED, inputs_message("cost_center_reference")
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    day_before = date.fromisoformat(processing_dates(SOURCES)[0]) - timedelta(days=1)
    with pytest.raises(ValueError, match="read no rows"):
        job.read(args, [day_before.isoformat()])


def test_transform(spark: SparkSession, test_dir: Path) -> None:
    """transform() over the sources read returns the job's reviewed output rows."""
    assert inputs.INPUTS_FILLED, inputs_message("cost_center_reference")
    assert expected.DEDUPLICATE_COST_CENTER_REFERENCE_REVIEWED, review_message(
        "cost_center_reference",
        "DEDUPLICATE_COST_CENTER_REFERENCE",
    )
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    df_actual = job.transform(**job.read(args, processing_dates(SOURCES)))
    df_expected = rows_to_df(spark, expected.DEDUPLICATE_COST_CENTER_REFERENCE_ROWS, OUTPUT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_write(spark: SparkSession, test_dir: Path, output_table: str) -> None:
    """write() stores the reviewed output rows as parquet and registers them in the table."""
    assert inputs.INPUTS_FILLED, inputs_message("cost_center_reference")
    assert expected.DEDUPLICATE_COST_CENTER_REFERENCE_REVIEWED, review_message(
        "cost_center_reference",
        "DEDUPLICATE_COST_CENTER_REFERENCE",
    )
    output = rows_to_df(spark, expected.DEDUPLICATE_COST_CENTER_REFERENCE_ROWS, OUTPUT_SCHEMA)
    job.write(output, str(test_dir), output_table)
    result = spark.read.parquet(str(test_dir))
    assert column_types(result.schema) == column_types(OUTPUT_SCHEMA)
    assert result.count() == len(expected.DEDUPLICATE_COST_CENTER_REFERENCE_ROWS)
    for column in NOT_NULL_COLUMNS:
        assert result.filter(result[column].isNull()).isEmpty(), column
    # One folder per partition value.
    partition_key = "snapshot_date"
    folders = sorted(path.name for path in test_dir.iterdir() if path.is_dir())
    dates = fixture_dates(expected.DEDUPLICATE_COST_CENTER_REFERENCE_ROWS, partition_key)
    assert folders == [f"{partition_key}={day}" for day in dates]
    # The table, which Athena reads, lists one partition per folder and holds every row.
    assert registered_partitions(spark, output_table) == folders
    assert spark.table(output_table).count() == result.count()


def test_write_incorrect_schema(spark: SparkSession, test_dir: Path, output_table: str) -> None:
    """write() refuses output whose column types differ from the output contract's."""
    column, other_type = "snapshot_date", "string"
    output = spark.createDataFrame([], OUTPUT_SCHEMA)
    output = output.withColumn(column, output[column].cast(other_type))
    with pytest.raises(ValueError, match="Schema validation failed"):
        job.write(output, str(test_dir), output_table)
    # A write that fails its checks registers nothing.
    assert registered_partitions(spark, output_table) == []
