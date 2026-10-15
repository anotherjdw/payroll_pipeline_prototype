"""Transformations and run steps of the dim_location ETL job.

parse_arguments, read, transform and write are scaffold-owned: `generate create-job` renders
them from the job spec, and `generate codegen` never changes them. Every other function is a
transformation whose body `generate codegen` writes.
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from payroll_pipeline_prototype.utils.catalog import register_partitions
from payroll_pipeline_prototype.utils.dataframes import (
    create_pyspark_dataframe,
    validate_pyspark_dataframe,
    write_pyspark_dataframe,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from payroll_pipeline_prototype.utils.spark import spark_session


def create_location_reference_key(location_reference: DataFrame) -> DataFrame:
    """Creates an MD5 hashed surrogate key column for the location reference DataFrame.

    Computes an MD5 hash over the concatenation of `location_code` and `valid_from`
    and appends it as a new column named `location_key`.

    Args:
        location_reference: DataFrame containing location reference data including
            `location_code` (string) and `valid_from` (date) columns.

    Returns:
        DataFrame with all original columns plus a new `location_key` column
        containing the MD5 hash of the concatenated `location_code` and `valid_from`.
    """
    return location_reference.withColumn(
        "location_key", F.md5(F.concat(F.col("location_code"), F.col("valid_from").cast("string")))
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--LOCATION_REFERENCE_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--LOCATION_REFERENCE_LOOKBACK_DAYS", type=int)
    parser.add_argument("--LOCATION_REFERENCE_START_DATE", type=str)
    parser.add_argument("--LOCATION_REFERENCE_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "location_reference": create_pyspark_dataframe(
            spark=spark,
            file_path=args.LOCATION_REFERENCE_INPUT_PATH,
            schema=schema_definitions.location_reference_schema,
            file_format="parquet",
            date_ranges=processing_dates["location_reference"],
            partition_key="snapshot_date",
        ),
    }
    if sources["location_reference"].isEmpty():
        raise ValueError(
            f"Source 'location_reference' read no rows for "
            f"{processing_dates['location_reference']}. "
            "To allow this, set allow_empty: true on this source in "
            "dim_location.yaml."
        )
    return sources


def transform(location_reference: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_location_reference_key_result = create_location_reference_key(location_reference)
    return create_location_reference_key_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.dim_location_schema,
            unique_key=["location_code", "valid_from"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="snapshot_date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="snapshot_date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
