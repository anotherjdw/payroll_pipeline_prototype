"""Transformations and run steps of the dim_pay_component ETL job.

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


def create_pay_component_key(pay_component_reference: DataFrame) -> DataFrame:
    """Creates an MD5 hashed surrogate key for pay component records.

    Adds a pay_component_key column derived from an MD5 hash of the
    lohnart_code and valid_from columns concatenated together.

    Args:
        pay_component_reference: DataFrame containing pay component reference
            data including lohnart_code and valid_from columns.

    Returns:
        DataFrame with all original columns plus a pay_component_key string
        column containing the MD5 hash of lohnart_code and valid_from.
    """
    return pay_component_reference.withColumn(
        "pay_component_key",
        F.md5(F.concat(F.col("lohnart_code"), F.col("valid_from").cast("string"))),
    )


def add_ceiling_capped_flag(create_pay_component_key_result: DataFrame) -> DataFrame:
    """Add a boolean flag indicating whether a ceiling monthly EUR value is present.

    Args:
        create_pay_component_key_result: DataFrame containing pay component data
            including a ceiling_monthly_eur column.

    Returns:
        DataFrame with all original columns plus is_ceiling_capped boolean column,
        which is True when ceiling_monthly_eur is not null.
    """
    return create_pay_component_key_result.withColumn(
        "is_ceiling_capped", F.col("ceiling_monthly_eur").isNotNull()
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--PAY_COMPONENT_REFERENCE_INPUT_PATH", type=str)
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
        "pay_component_reference": create_pyspark_dataframe(
            spark=spark,
            file_path=args.PAY_COMPONENT_REFERENCE_INPUT_PATH,
            schema=schema_definitions.pay_component_reference_schema,
            file_format="parquet",
            date_ranges=processing_dates,
            partition_key="valid_from",
        ),
    }
    if sources["pay_component_reference"].isEmpty():
        raise ValueError(
            f"Source 'pay_component_reference' read no rows for {processing_dates}. "
            "To allow this, set allow_empty: true on this source in "
            "dim_pay_component.yaml."
        )
    return sources


def transform(pay_component_reference: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_pay_component_key_result = create_pay_component_key(pay_component_reference)
    add_ceiling_capped_flag_result = add_ceiling_capped_flag(create_pay_component_key_result)
    return add_ceiling_capped_flag_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.dim_pay_component_schema,
            unique_key=["lohnart_code", "valid_from"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="valid_from",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="valid_from",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
