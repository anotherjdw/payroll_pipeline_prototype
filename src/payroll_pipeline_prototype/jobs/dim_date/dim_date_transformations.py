"""Transformations and run steps of the dim_date ETL job.

parse_arguments, read, transform and write are scaffold-owned: `generate create-job` renders
them from the job spec, and `generate codegen` never changes them. Every other function is a
transformation whose body `generate codegen` writes.
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from payroll_pipeline_prototype.utils.dataframes import (
    create_pyspark_dataframe,
    validate_pyspark_dataframe,
    write_pyspark_dataframe,
)
from payroll_pipeline_prototype.utils.schema import schema_definitions
from payroll_pipeline_prototype.utils.spark import spark_session


def create_year_column(dates: DataFrame) -> DataFrame:
    """Extracts the year from the date column and adds it as a new integer column.

    Args:
        dates: DataFrame containing a date column of type date.

    Returns:
        DataFrame with the original date column and an additional year column
        of type int representing the year extracted from the date.
    """
    return dates.withColumn("year", F.year(F.col("date")).cast("int"))


def create_quarter_column(create_year_column_result: DataFrame) -> DataFrame:
    """Creates a quarter column extracted from the date column.

    Args:
        create_year_column_result: DataFrame containing date and year columns.

    Returns:
        DataFrame with an additional quarter column representing the quarter
        of the year extracted from the date column.
    """
    return create_year_column_result.withColumn("quarter", F.quarter(F.col("date")))


def create_month_name_column(create_quarter_column_result: DataFrame) -> DataFrame:
    """Creates a month_name column containing the full month name extracted from the date column.

    Args:
        create_quarter_column_result: DataFrame containing date, year, and quarter columns.

    Returns:
        DataFrame with an additional month_name column containing the full name of the month
        derived from the date column using F.date_format().
    """
    return create_quarter_column_result.withColumn(
        "month_name", F.date_format(F.col("date"), "MMMM")
    )


def create_pay_period_dimension(create_month_name_column_result: DataFrame) -> DataFrame:
    """Creates a pay_period column from the date column using yyyy-MM format.

    Args:
        create_month_name_column_result: DataFrame containing date, year, quarter,
            and month_name columns.

    Returns:
        DataFrame with the original columns plus a pay_period column containing
        the year and month in yyyy-MM format extracted from the date column.
    """
    return create_month_name_column_result.withColumn(
        "pay_period", F.date_format(F.col("date"), "yyyy-MM")
    )


def add_month_boundaries(create_pay_period_dimension_result: DataFrame) -> DataFrame:
    """Adds month boundary columns and a month-end flag to a pay period dimension DataFrame.

    Computes the first and last dates of the month for each row's date value,
    and derives a boolean flag indicating whether the row's date falls on the
    last day of its month.

    Args:
        create_pay_period_dimension_result: DataFrame containing pay period
            dimension data with a date column and associated attributes.

    Returns:
        DataFrame with the original columns plus month_begin (first day of the
        month), month_end (last day of the month), and month_end_flag (True
        when date equals month_end).
    """
    df = create_pay_period_dimension_result
    df = df.withColumn("month_begin", F.trunc(F.col("date"), "month").cast("date"))
    df = df.withColumn("month_end", F.last_day(F.col("date")).cast("date"))
    df = df.withColumn("month_end_flag", F.col("date") == F.col("month_end"))
    return df.select(
        F.col("date"),
        F.col("year"),
        F.col("quarter"),
        F.col("month_name"),
        F.col("pay_period"),
        F.col("month_begin"),
        F.col("month_end"),
        F.col("month_end_flag"),
    )


def add_peak_flag(add_month_boundaries_result: DataFrame) -> DataFrame:
    """Adds a boolean peak_flag column marking peak retail/floral season dates.

    Marks Valentine's Day (Feb 14), Mother's Day in Germany (second Sunday in May),
    and the period from the last Friday in November through Christmas Eve (Dec 24)
    as peak season dates. All other dates are marked False.

    Args:
        add_month_boundaries_result: DataFrame containing date dimension columns
            including a date column used to evaluate peak season membership.

    Returns:
        DataFrame with all original columns plus a peak_flag boolean column
        indicating whether each date falls within a peak season period.
    """
    valentines = (F.month(F.col("date")) == 2) & (F.dayofmonth(F.col("date")) == 14)
    in_may = F.month(F.col("date")) == 5
    is_sunday = F.dayofweek(F.col("date")) == 1
    day_of_month = F.dayofmonth(F.col("date"))
    mothers_day = in_may & is_sunday & (day_of_month >= 8) & (day_of_month <= 14)
    nov30_dow = F.dayofweek(F.make_date(F.col("year"), F.lit(11), F.lit(30)))
    days_back = F.when(nov30_dow >= 6, nov30_dow - 6).otherwise(nov30_dow + 1)
    last_friday_of_nov = F.date_sub(F.make_date(F.col("year"), F.lit(11), F.lit(30)), days_back)
    christmas_eve = F.make_date(F.col("year"), F.lit(12), F.lit(24))
    black_friday_to_xmas = (F.col("date") >= last_friday_of_nov) & (F.col("date") <= christmas_eve)
    peak_flag = valentines | mothers_day | black_friday_to_xmas
    return add_month_boundaries_result.withColumn("peak_flag", peak_flag)


def add_date_key(add_peak_flag_result: DataFrame) -> DataFrame:
    """Adds an integer date key in yyyymmdd format derived from the date column.

    Args:
        add_peak_flag_result: DataFrame containing date and calendar dimension
            columns including peak and month-end flags.

    Returns:
        DataFrame with all original columns plus a date_key integer column
        representing each date in yyyymmdd format.
    """
    return add_peak_flag_result.withColumn(
        "date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int")
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--DATES_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "dates": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DATES_INPUT_PATH,
            schema=schema_definitions.dates_schema,
            file_format="json",
        ),
    }
    if sources["dates"].isEmpty():
        raise ValueError(
            "Source 'dates' read no rows from its whole dataset. "
            "To allow this, set allow_empty: true on this source in "
            "dim_date.yaml."
        )
    return sources


def transform(dates: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_year_column_result = create_year_column(dates)
    create_quarter_column_result = create_quarter_column(create_year_column_result)
    create_month_name_column_result = create_month_name_column(create_quarter_column_result)
    create_pay_period_dimension_result = create_pay_period_dimension(
        create_month_name_column_result,
    )
    add_month_boundaries_result = add_month_boundaries(create_pay_period_dimension_result)
    add_peak_flag_result = add_peak_flag(add_month_boundaries_result)
    add_date_key_result = add_date_key(add_peak_flag_result)
    return add_date_key_result


def write(output: DataFrame, output_file_path: str) -> None:
    """Check the output against its contract, then write it as parquet."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.dim_date_schema,
            unique_key=["date"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key=None,
        )
    finally:
        output.unpersist()
