"""Transformations and run steps of the dim_pay_run ETL job.

parse_arguments, read, transform and write are scaffold-owned: `generate create-job` renders
them from the job spec, and `generate codegen` never changes them. Every other function is a
transformation whose body `generate codegen` writes.
"""

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from payroll_pipeline_prototype.utils.catalog import register_partitions
from payroll_pipeline_prototype.utils.dataframes import (
    create_pyspark_dataframe,
    validate_pyspark_dataframe,
    write_pyspark_dataframe,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from payroll_pipeline_prototype.utils.spark import spark_session


def select_pay_run_columns(payroll_register: DataFrame) -> DataFrame:
    """Select pay run identifying columns from the payroll register.

    Args:
        payroll_register: DataFrame containing payroll register data with
            pay run, employee, and compensation component details.

    Returns:
        DataFrame containing only the pay_run_id, pay_date, and run_type columns.
    """
    return payroll_register.select(F.col("pay_run_id"), F.col("pay_date"), F.col("run_type"))


def deduplicate_pay_run_data(select_pay_run_columns_result: DataFrame) -> DataFrame:
    """Deduplicate rows in the pay run DataFrame by pay_run_id.

    Args:
        select_pay_run_columns_result: DataFrame containing pay run data with
            columns pay_run_id, pay_date, and run_type, potentially containing
            duplicate pay_run_id values.

    Returns:
        DataFrame with the same schema but with only one row per pay_run_id,
        retaining columns pay_run_id, pay_date, and run_type.
    """
    window = Window.partitionBy(F.col("pay_run_id")).orderBy(F.col("pay_run_id"))
    return (
        select_pay_run_columns_result.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def create_pay_run_key(deduplicate_pay_run_data_result: DataFrame) -> DataFrame:
    """Creates an MD5 hashed key column from the pay_run_id column.

    Args:
        deduplicate_pay_run_data_result: DataFrame containing pay run data with
            pay_run_id, pay_date, and run_type columns.

    Returns:
        DataFrame with the original columns plus a pay_run_key column containing
        the MD5 hash of the pay_run_id column.
    """
    return deduplicate_pay_run_data_result.withColumn("pay_run_key", F.md5(F.col("pay_run_id")))


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--PAYROLL_REGISTER_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--PAYROLL_REGISTER_LOOKBACK_DAYS", type=int)
    parser.add_argument("--PAYROLL_REGISTER_START_DATE", type=str)
    parser.add_argument("--PAYROLL_REGISTER_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "payroll_register": create_pyspark_dataframe(
            spark=spark,
            file_path=args.PAYROLL_REGISTER_INPUT_PATH,
            schema=schema_definitions.payroll_register_schema,
            file_format="parquet",
            date_ranges=processing_dates["payroll_register"],
            partition_key="pay_date",
        ),
    }
    if sources["payroll_register"].isEmpty():
        raise ValueError(
            f"Source 'payroll_register' read no rows for "
            f"{processing_dates['payroll_register']}. "
            "To allow this, set allow_empty: true on this source in "
            "dim_pay_run.yaml."
        )
    return sources


def transform(payroll_register: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    select_pay_run_columns_result = select_pay_run_columns(payroll_register)
    deduplicate_pay_run_data_result = deduplicate_pay_run_data(select_pay_run_columns_result)
    create_pay_run_key_result = create_pay_run_key(deduplicate_pay_run_data_result)
    return create_pay_run_key_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.dim_pay_run_schema,
            unique_key=["pay_run_id"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="pay_date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="pay_date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
