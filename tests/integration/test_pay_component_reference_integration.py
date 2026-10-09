"""Integration tests of the pay_component_reference run steps on local Spark.

Each test writes the job's input rows from tests/fixtures/ to disk as the job reads them: in
each source's file format and partition layout. test_transform and test_write compare with
the job's output, the reviewed expected rows of its last transformation.
"""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from payroll_pipeline_prototype.jobs.pay_component_reference import (
    pay_component_reference_transformations as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.pay_component_reference import expected, inputs
from tests.helpers import (
    SourceFixture,
    column_types,
    fixture_dates,
    inputs_message,
    processing_dates,
    review_message,
    row_key,
    rows_to_df,
    write_sources,
)

# How the job reads each source, and the input rows the tests write for it.
SOURCES = {
    "pay_component_reference_raw": SourceFixture(
        argument="PAY_COMPONENT_REFERENCE_RAW_INPUT_PATH",
        rows=inputs.PAY_COMPONENT_REFERENCE_RAW_ROWS,
        schema=schema_definitions.pay_component_reference_raw_schema,
        file_format="json",
        partition_key="valid_from",
    ),
}

OUTPUT_SCHEMA = schema_definitions.pay_component_reference_schema

# The output contract's non-nullable columns.
NOT_NULL_COLUMNS = [
    "reference_version",
    "valid_from",
    "valid_to",
    "lohnart_code",
    "name_de",
    "name_en",
    "category",
    "bearer",
    "gl_account",
    "applies_to",
    "is_active",
    "employer_rate_asymmetric",
]


def test_read(spark: SparkSession, test_dir: Path) -> None:
    """read() loads every input row of every source, in its contract's column types."""
    assert inputs.INPUTS_FILLED, inputs_message("pay_component_reference")
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    sources = job.read(args, processing_dates(SOURCES))
    assert set(sources) == set(SOURCES)
    for alias, source in SOURCES.items():
        assert sources[alias].count() == len(source.rows), alias
        assert column_types(sources[alias].schema) == column_types(source.schema), alias


def test_transform(spark: SparkSession, test_dir: Path) -> None:
    """transform() over the sources read returns the job's reviewed output rows."""
    assert inputs.INPUTS_FILLED, inputs_message("pay_component_reference")
    assert expected.DEDUPLICATE_PAY_COMPONENT_REFERENCE_REVIEWED, review_message(
        "pay_component_reference",
        "DEDUPLICATE_PAY_COMPONENT_REFERENCE",
    )
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    df_actual = job.transform(**job.read(args, processing_dates(SOURCES)))
    df_expected = rows_to_df(
        spark,
        expected.DEDUPLICATE_PAY_COMPONENT_REFERENCE_ROWS,
        OUTPUT_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_write(spark: SparkSession, test_dir: Path) -> None:
    """write() stores the reviewed output rows as parquet with the contract's column types."""
    assert inputs.INPUTS_FILLED, inputs_message("pay_component_reference")
    assert expected.DEDUPLICATE_PAY_COMPONENT_REFERENCE_REVIEWED, review_message(
        "pay_component_reference",
        "DEDUPLICATE_PAY_COMPONENT_REFERENCE",
    )
    output = rows_to_df(spark, expected.DEDUPLICATE_PAY_COMPONENT_REFERENCE_ROWS, OUTPUT_SCHEMA)
    job.write(output, str(test_dir))
    result = spark.read.parquet(str(test_dir))
    assert column_types(result.schema) == column_types(OUTPUT_SCHEMA)
    assert result.count() == len(expected.DEDUPLICATE_PAY_COMPONENT_REFERENCE_ROWS)
    for column in NOT_NULL_COLUMNS:
        assert result.filter(result[column].isNull()).isEmpty(), column
    # One folder per partition value.
    partition_key = "valid_from"
    folders = sorted(path.name for path in test_dir.iterdir() if path.is_dir())
    dates = fixture_dates(expected.DEDUPLICATE_PAY_COMPONENT_REFERENCE_ROWS, partition_key)
    assert folders == [f"{partition_key}={day}" for day in dates]


def test_write_incorrect_schema(spark: SparkSession, test_dir: Path) -> None:
    """write() refuses output whose column types differ from the output contract's."""
    column, other_type = "reference_version", "int"
    output = spark.createDataFrame([], OUTPUT_SCHEMA)
    output = output.withColumn(column, output[column].cast(other_type))
    with pytest.raises(ValueError, match="Schema validation failed"):
        job.write(output, str(test_dir))
