"""End-to-end test of the fact_pay_component job on local Spark.

The test runs the job's Glue entry point, run(), which parses its arguments from sys.argv,
reads the job's input rows from tests/fixtures/, which the test writes to disk as the job
reads them, and writes the output. The rows read back must be the job's reviewed output
rows: the expected rows of its last transformation.

The run also registers the dates it wrote in a local table standing in for the job's Glue
table, which must then hold the same rows.
"""

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from payroll_pipeline_prototype.jobs.fact_pay_component import (
    fact_pay_component as job,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from tests.fixtures.fact_pay_component import expected, inputs
from tests.helpers import (
    SourceFixture,
    column_types,
    create_catalog_table,
    fixture_dates,
    processing_dates,
    registered_partitions,
    review_message,
    row_key,
    rows_to_df,
    write_sources,
)

# How the job reads each source, and the input rows the test writes for it.
SOURCES = {
    "payroll_register": SourceFixture(
        argument="PAYROLL_REGISTER_INPUT_PATH",
        rows=inputs.PAYROLL_REGISTER_ROWS,
        schema=schema_definitions.payroll_register_schema,
        file_format="parquet",
        partition_key="pay_date",
    ),
    "dim_employee": SourceFixture(
        argument="DIM_EMPLOYEE_INPUT_PATH",
        rows=inputs.DIM_EMPLOYEE_ROWS,
        schema=schema_definitions.dim_employee_schema,
        file_format="parquet",
        partition_key="snapshot_date",
    ),
    "dim_cost_center": SourceFixture(
        argument="DIM_COST_CENTER_INPUT_PATH",
        rows=inputs.DIM_COST_CENTER_ROWS,
        schema=schema_definitions.dim_cost_center_schema,
        file_format="parquet",
        partition_key="snapshot_date",
    ),
    "dim_location": SourceFixture(
        argument="DIM_LOCATION_INPUT_PATH",
        rows=inputs.DIM_LOCATION_ROWS,
        schema=schema_definitions.dim_location_schema,
        file_format="parquet",
        partition_key="snapshot_date",
    ),
    "dim_pay_component": SourceFixture(
        argument="DIM_PAY_COMPONENT_INPUT_PATH",
        rows=inputs.DIM_PAY_COMPONENT_ROWS,
        schema=schema_definitions.dim_pay_component_schema,
        file_format="parquet",
        partition_key="valid_from",
    ),
}

OUTPUT_SCHEMA = schema_definitions.fact_pay_component_schema


@pytest.fixture
def output_table(spark: SparkSession, test_database: str, test_dir: Path) -> Iterator[str]:
    """A local table shaped like the job's Glue table, at the folder the run writes to.

    It starts with no partitions registered, like the Glue table before the job's first run.
    """
    table = f"{test_database}.fact_pay_component"
    location = test_dir / "output"
    create_catalog_table(spark, table, OUTPUT_SCHEMA, "period_end_date", location)
    yield table
    # An external table: dropping it removes the catalog entry, not the files.
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_run_end_to_end(
    spark: SparkSession, test_dir: Path, monkeypatch: pytest.MonkeyPatch, output_table: str
) -> None:
    """run() writes exactly the job's reviewed output rows and registers them in the table."""
    assert inputs.CREATE_COST_CENTER_KEY_REVIEWED, review_message(
        "dim_cost_center",
        "CREATE_COST_CENTER_KEY",
    )
    assert inputs.RENAME_EMPLOYEE_VERSION_ID_REVIEWED, review_message(
        "dim_employee",
        "RENAME_EMPLOYEE_VERSION_ID",
    )
    assert inputs.CREATE_LOCATION_REFERENCE_KEY_REVIEWED, review_message(
        "dim_location",
        "CREATE_LOCATION_REFERENCE_KEY",
    )
    assert inputs.ADD_CEILING_CAPPED_FLAG_REVIEWED, review_message(
        "dim_pay_component",
        "ADD_CEILING_CAPPED_FLAG",
    )
    assert inputs.COMBINE_PAY_COLUMNS_REVIEWED, review_message(
        "payroll_register",
        "COMBINE_PAY_COLUMNS",
    )
    assert expected.JOIN_PAY_COMPONENT_KEY_TO_PAYROLL_REVIEWED, review_message(
        "fact_pay_component",
        "JOIN_PAY_COMPONENT_KEY_TO_PAYROLL",
    )
    dates = processing_dates(SOURCES)
    output_path = str(test_dir / "output")
    argv = [
        "fact_pay_component.py",
        *write_sources(spark, SOURCES, test_dir / "sources"),
        "--OUTPUT_PATH",
        output_path,
        "--OUTPUT_TABLE",
        output_table,
        # Each partitioned source reads the days its input rows fall on.
        "--PROCESSING_TYPE",
        "backfill",
        "--PAYROLL_REGISTER_START_DATE",
        dates["payroll_register"][0],
        "--PAYROLL_REGISTER_END_DATE",
        dates["payroll_register"][-1],
        "--DIM_EMPLOYEE_START_DATE",
        dates["dim_employee"][0],
        "--DIM_EMPLOYEE_END_DATE",
        dates["dim_employee"][-1],
        "--DIM_COST_CENTER_START_DATE",
        dates["dim_cost_center"][0],
        "--DIM_COST_CENTER_END_DATE",
        dates["dim_cost_center"][-1],
        "--DIM_LOCATION_START_DATE",
        dates["dim_location"][0],
        "--DIM_LOCATION_END_DATE",
        dates["dim_location"][-1],
        "--DIM_PAY_COMPONENT_START_DATE",
        dates["dim_pay_component"][0],
        "--DIM_PAY_COMPONENT_END_DATE",
        dates["dim_pay_component"][-1],
    ]
    monkeypatch.setattr(sys, "argv", argv)
    job.run()
    df_actual = spark.read.parquet(output_path)
    df_expected = rows_to_df(spark, expected.JOIN_PAY_COMPONENT_KEY_TO_PAYROLL_ROWS, OUTPUT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
    # The run registered one partition per date it wrote, so the table, which Athena reads,
    # holds the same rows as the files.
    partition_key = "period_end_date"
    output_dates = fixture_dates(expected.JOIN_PAY_COMPONENT_KEY_TO_PAYROLL_ROWS, partition_key)
    partitions = [f"{partition_key}={day}" for day in output_dates]
    assert registered_partitions(spark, output_table) == partitions
    df_table = spark.table(output_table)
    assert column_types(df_table.schema) == column_types(df_expected.schema)
    from_table = df_table.select(columns).collect()
    assert sorted(from_table, key=row_key) == sorted(expected_rows, key=row_key)
