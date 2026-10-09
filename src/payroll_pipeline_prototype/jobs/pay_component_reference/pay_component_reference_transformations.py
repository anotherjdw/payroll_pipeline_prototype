"""Transformations and run steps of the pay_component_reference ETL job.

parse_arguments, read, transform and write are scaffold-owned: `generate create-job` renders
them from the job spec, and `generate codegen` never changes them. Every other function is a
transformation whose body `generate codegen` writes.
"""

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from payroll_pipeline_prototype.utils.dataframes import (
    create_pyspark_dataframe,
    validate_pyspark_dataframe,
    write_pyspark_dataframe,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from payroll_pipeline_prototype.utils.spark import spark_session


def deduplicate_pay_component_reference(pay_component_reference_raw: DataFrame) -> DataFrame:
    """Deduplicate pay component reference data by lohnart_code and valid_from.

    Removes duplicate rows from the input DataFrame, keeping one record per
    unique combination of lohnart_code and valid_from, retaining the first
    occurrence ordered by reference_version descending.

    Args:
        pay_component_reference_raw: Raw pay component reference DataFrame
            containing potentially duplicate records keyed on lohnart_code
            and valid_from.

    Returns:
        A deduplicated DataFrame with exactly one row per unique combination
        of lohnart_code and valid_from, preserving the full output schema.
    """
    window = Window.partitionBy("lohnart_code", "valid_from").orderBy(
        F.col("reference_version").desc()
    )
    return (
        pay_component_reference_raw.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--PAY_COMPONENT_REFERENCE_RAW_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--LOOKBACK_DAYS", type=int)
    parser.add_argument("--START_DATE", type=str)
    parser.add_argument("--END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: list[str]) -> dict[str, DataFrame]:
    """Read every source for the processing dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "pay_component_reference_raw": create_pyspark_dataframe(
            spark=spark,
            file_path=args.PAY_COMPONENT_REFERENCE_RAW_INPUT_PATH,
            schema=schema_definitions.pay_component_reference_raw_schema,
            file_format="json",
            date_ranges=processing_dates,
            partition_key="valid_from",
        ),
    }
    return sources


def transform(pay_component_reference_raw: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    deduplicate_pay_component_reference_result = deduplicate_pay_component_reference(
        pay_component_reference_raw,
    )
    return deduplicate_pay_component_reference_result


def write(output: DataFrame, output_file_path: str) -> None:
    """Check the output against its contract, then write it as parquet."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.pay_component_reference_schema,
            unique_key=["lohnart_code", "valid_from"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="valid_from",
        )
    finally:
        output.unpersist()
