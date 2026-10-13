"""Transformations and run steps of the dim_cost_center ETL job.

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


def create_cost_center_key(cost_center_reference: DataFrame) -> DataFrame:
    """Add an MD5-hashed surrogate key column derived from cost center code and valid_from date.

    Args:
        cost_center_reference: DataFrame containing cost center reference data including
            cost_center_code and valid_from columns used to generate the key.

    Returns:
        DataFrame with all original columns plus a new cost_center_key column containing
        the MD5 hash of the concatenation of cost_center_code and valid_from.
    """
    return cost_center_reference.withColumn(
        "cost_center_key",
        F.md5(F.concat(F.col("cost_center_code"), F.col("valid_from").cast("string"))),
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--COST_CENTER_REFERENCE_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
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
        "cost_center_reference": create_pyspark_dataframe(
            spark=spark,
            file_path=args.COST_CENTER_REFERENCE_INPUT_PATH,
            schema=schema_definitions.cost_center_reference_schema,
            file_format="parquet",
            date_ranges=processing_dates,
            partition_key="snapshot_date",
        ),
    }
    if sources["cost_center_reference"].isEmpty():
        raise ValueError(
            f"Source 'cost_center_reference' read no rows for {processing_dates}. "
            "To allow this, set allow_empty: true on this source in "
            "dim_cost_center.yaml."
        )
    return sources


def transform(cost_center_reference: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_cost_center_key_result = create_cost_center_key(cost_center_reference)
    return create_cost_center_key_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.dim_cost_center_schema,
            unique_key=["cost_center_code", "valid_from"],
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
