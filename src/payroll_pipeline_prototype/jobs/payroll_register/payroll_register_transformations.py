"""Transformations and run steps of the payroll_register ETL job.

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


def generate_retro_pay_data(payroll_register_raw: DataFrame) -> DataFrame:
    """Filter retro pay runs and aggregate amount by employee, period, and wage type.

    Filters the payroll register to rows where run_type is 'retro', then groups
    by employee_id, pay_period, and lohnart_code, summing amount_eur into
    amount_eur_retro.

    Args:
        payroll_register_raw: Raw payroll register DataFrame containing pay run
            details including run_type, employee identifiers, and monetary amounts.

    Returns:
        DataFrame with columns employee_id, pay_period, lohnart_code, and
        amount_eur_retro representing the summed retro pay amounts per group.
    """
    return (
        payroll_register_raw.filter(F.col("run_type") == "retro")
        .groupBy("employee_id", "pay_period", "lohnart_code")
        .agg(F.sum("amount_eur").cast("decimal(14,2)").alias("amount_eur_retro"))
    )


def filter_regular_pay_data(payroll_register_raw: DataFrame) -> DataFrame:
    """Filter payroll register data to include only regular pay runs.

    Args:
        payroll_register_raw: Raw payroll register DataFrame containing all run types.

    Returns:
        DataFrame containing only rows where run_type equals 'regular', with the
        same schema as the input.
    """
    return payroll_register_raw.filter(F.col("run_type") == "regular")


def join_retro_pay_data(
    filter_regular_pay_data_result: DataFrame, generate_retro_pay_data_result: DataFrame
) -> DataFrame:
    """Left join retro pay data onto regular pay data and fill missing retro amounts with zero.

    Args:
        filter_regular_pay_data_result: DataFrame containing regular pay data with columns
            including employee_id, pay_period, and lohnart_code as join keys.
        generate_retro_pay_data_result: DataFrame containing retro pay amounts keyed by
            employee_id, pay_period, and lohnart_code.

    Returns:
        DataFrame with all columns from filter_regular_pay_data_result plus amount_eur_retro,
        where missing retro amounts are filled with zero.
    """
    joined = filter_regular_pay_data_result.join(
        generate_retro_pay_data_result, on=["employee_id", "pay_period", "lohnart_code"], how="left"
    )
    result = joined.withColumn(
        "amount_eur_retro", F.coalesce(F.col("amount_eur_retro"), F.lit(0).cast("decimal(14,2)"))
    )
    return result.select(
        F.col("pay_run_id"),
        F.col("pay_period"),
        F.col("pay_date"),
        F.col("run_type"),
        F.col("employee_id"),
        F.col("cost_center_code"),
        F.col("location_code"),
        F.col("lohnart_code"),
        F.col("component_name"),
        F.col("bearer"),
        F.col("gl_account"),
        F.col("quantity"),
        F.col("rate"),
        F.col("assessment_base_eur"),
        F.col("amount_eur"),
        F.col("amount_eur_retro"),
    )


def combine_pay_columns(join_retro_pay_data_result: DataFrame) -> DataFrame:
    """Combines amount_eur and amount_eur_retro into a single amount_eur column.

    Adds the amount_eur and amount_eur_retro columns together, storing the
    result in amount_eur, and drops the amount_eur_retro column from the
    resulting DataFrame.

    Args:
        join_retro_pay_data_result: DataFrame containing pay run data with
            separate amount_eur and amount_eur_retro columns.

    Returns:
        DataFrame with the same schema as the input except amount_eur_retro
        is removed and amount_eur reflects the sum of both original columns.
    """
    return join_retro_pay_data_result.withColumn(
        "amount_eur", (F.col("amount_eur") + F.col("amount_eur_retro")).cast("decimal(14,2)")
    ).drop("amount_eur_retro")


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--PAYROLL_REGISTER_RAW_INPUT_PATH", type=str)
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
        "payroll_register_raw": create_pyspark_dataframe(
            spark=spark,
            file_path=args.PAYROLL_REGISTER_RAW_INPUT_PATH,
            schema=schema_definitions.payroll_register_raw_schema,
            file_format="json",
            date_ranges=processing_dates,
            partition_key="pay_date",
        ),
    }
    if sources["payroll_register_raw"].isEmpty():
        raise ValueError(
            f"Source 'payroll_register_raw' read no rows for {processing_dates}. "
            "To allow this, set allow_empty: true on this source in "
            "payroll_register.yaml."
        )
    return sources


def transform(payroll_register_raw: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    generate_retro_pay_data_result = generate_retro_pay_data(payroll_register_raw)
    filter_regular_pay_data_result = filter_regular_pay_data(payroll_register_raw)
    join_retro_pay_data_result = join_retro_pay_data(
        filter_regular_pay_data_result,
        generate_retro_pay_data_result,
    )
    combine_pay_columns_result = combine_pay_columns(join_retro_pay_data_result)
    return combine_pay_columns_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.payroll_register_schema,
            unique_key=["employee_id", "pay_run_id", "lohnart_code"],
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
