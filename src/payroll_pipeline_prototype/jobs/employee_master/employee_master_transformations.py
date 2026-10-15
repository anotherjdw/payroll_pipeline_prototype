"""Transformations and run steps of the employee_master ETL job.

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


def deduplicate_employee_master(employee_master_raw: DataFrame) -> DataFrame:
    """Deduplicate employee master records by employee_id and snapshot_date.

    Retains the most recent record for each (employee_id, snapshot_date) pair
    based on extract_timestamp in descending order.

    Args:
        employee_master_raw: Raw employee master DataFrame potentially containing
            duplicate rows for the same employee_id and snapshot_date combination.

    Returns:
        A DataFrame with one row per (employee_id, snapshot_date) pair, keeping
        the record with the latest extract_timestamp, and the same schema as the
        input.
    """
    window = Window.partitionBy("employee_id", "snapshot_date").orderBy(
        F.col("extract_timestamp").desc()
    )
    return (
        employee_master_raw.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def create_tracked_columns(deduplicate_employee_master_result: DataFrame) -> DataFrame:
    """Add is_version_start column based on tracked field changes per employee.

    Compares a struct of tracked fields against the previous row's struct using
    eqNullSafe over a window partitioned by employee_id and ordered by
    snapshot_date. The first row per employee and any row where tracked fields
    differ from the previous snapshot are marked as version starts.

    Args:
        deduplicate_employee_master_result: DataFrame containing deduplicated
            employee master records with all required input columns.

    Returns:
        DataFrame with all input columns plus is_version_start (boolean) which
        is True when any tracked field has changed relative to the previous
        snapshot for the same employee, or for the employee's first record.
    """
    tracked_cols = [
        "cost_center_code",
        "department",
        "work_location_code",
        "role",
        "job_level",
        "employment_type",
        "pay_type",
        "contracted_hours_per_month",
        "annual_gross_eur",
        "hourly_rate_eur",
        "steuerklasse",
        "church_tax_applies",
        "num_children",
        "childless_surcharge_applies",
        "health_insurance_type",
        "health_insurance_fund",
        "termination_date",
    ]
    window = Window.partitionBy("employee_id").orderBy("snapshot_date")
    current_struct = F.struct(*[F.col(c) for c in tracked_cols])
    prev_struct = F.lag(current_struct).over(window)
    is_version_start = (
        F.when(prev_struct.isNull(), F.lit(True))
        .otherwise(~current_struct.eqNullSafe(prev_struct))
        .alias("is_version_start")
    )
    return deduplicate_employee_master_result.withColumn("is_version_start", is_version_start)


def filter_version_starts(create_tracked_columns_result: DataFrame) -> DataFrame:
    """Filter rows where a new version starts and remove the version flag column.

    Args:
        create_tracked_columns_result: DataFrame containing employee snapshot data
            with an `is_version_start` boolean column indicating version boundaries.

    Returns:
        DataFrame containing only rows where `is_version_start` is True, with the
        `is_version_start` column dropped.
    """
    return create_tracked_columns_result.filter(F.col("is_version_start")).drop("is_version_start")


def add_validity_window(filter_version_starts_result: DataFrame) -> DataFrame:
    """Adds valid_from and valid_to columns based on snapshot_date per employee.

    Computes a validity window for each employee record by setting valid_from
    equal to snapshot_date and valid_to equal to the next snapshot_date for the
    same employee, determined via a lead window function partitioned by
    employee_id and ordered by snapshot_date.

    Args:
        filter_version_starts_result: DataFrame containing employee snapshot
            records with snapshot_date and employee_id columns among others.

    Returns:
        DataFrame with all original columns plus valid_from (date) set to
        snapshot_date and valid_to (date) set to the lead snapshot_date within
        each employee partition ordered by snapshot_date.
    """
    window_spec = Window.partitionBy("employee_id").orderBy("snapshot_date")
    return filter_version_starts_result.withColumn("valid_from", F.col("snapshot_date")).withColumn(
        "valid_to", F.lead("snapshot_date").over(window_spec)
    )


def create_employee_version_id(add_validity_window_result: DataFrame) -> DataFrame:
    """Creates an MD5-hashed version identifier for each employee record.

    Computes the employee_version_id column by applying MD5 hashing to the
    concatenation of employee_id and valid_from for each row.

    Args:
        add_validity_window_result: DataFrame containing employee records with
            validity window columns including employee_id and valid_from.

    Returns:
        DataFrame with all original columns plus employee_version_id, which is
        an MD5 hash of the concatenation of employee_id and valid_from.
    """
    return add_validity_window_result.withColumn(
        "employee_version_id",
        F.md5(F.concat(F.col("employee_id"), F.col("valid_from").cast("string"))),
    )


def add_is_current_flag(create_employee_version_id_result: DataFrame) -> DataFrame:
    """Adds is_current and is_version_start flags to the employee versioned DataFrame.

    Computes is_current as True when valid_to is null or valid_to is strictly
    greater than snapshot_date. Also adds is_version_start as a boolean column
    derived from whether valid_from equals snapshot_date.

    Args:
        create_employee_version_id_result: DataFrame containing employee version
            records with valid_from, valid_to, and snapshot_date columns.

    Returns:
        DataFrame with the original columns plus is_version_start and is_current
        boolean columns appended, matching the required output schema.
    """
    return (
        create_employee_version_id_result.withColumn(
            "is_version_start", F.col("valid_from") == F.col("snapshot_date")
        )
        .withColumn(
            "is_current", F.col("valid_to").isNull() | (F.col("valid_to") > F.col("snapshot_date"))
        )
        .select(
            "snapshot_date",
            "employee_id",
            "first_name",
            "last_name",
            "gender",
            "date_of_birth",
            "nationality",
            "marital_status",
            "steuer_id",
            "sozialversicherungsnummer",
            "iban",
            "street_address",
            "postal_code",
            "city",
            "work_location_code",
            "work_location_name",
            "department",
            "cost_center_code",
            "role",
            "pay_type",
            "employment_type",
            "contracted_hours_per_month",
            "annual_gross_eur",
            "hourly_rate_eur",
            "steuerklasse",
            "num_children",
            "childless_surcharge_applies",
            "religious_affiliation",
            "church_tax_applies",
            "health_insurance_type",
            "health_insurance_fund",
            "manager_id",
            "hire_date",
            "termination_date",
            "job_level",
            "extract_timestamp",
            "is_version_start",
            "valid_from",
            "valid_to",
            "employee_version_id",
            "is_current",
        )
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--EMPLOYEE_MASTER_RAW_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--EMPLOYEE_MASTER_RAW_LOOKBACK_DAYS", type=int)
    parser.add_argument("--EMPLOYEE_MASTER_RAW_START_DATE", type=str)
    parser.add_argument("--EMPLOYEE_MASTER_RAW_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "employee_master_raw": create_pyspark_dataframe(
            spark=spark,
            file_path=args.EMPLOYEE_MASTER_RAW_INPUT_PATH,
            schema=schema_definitions.employee_master_raw_schema,
            file_format="json",
            date_ranges=processing_dates["employee_master_raw"],
            partition_key="snapshot_date",
        ),
    }
    if sources["employee_master_raw"].isEmpty():
        raise ValueError(
            f"Source 'employee_master_raw' read no rows for "
            f"{processing_dates['employee_master_raw']}. "
            "To allow this, set allow_empty: true on this source in "
            "employee_master.yaml."
        )
    return sources


def transform(employee_master_raw: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    deduplicate_employee_master_result = deduplicate_employee_master(employee_master_raw)
    create_tracked_columns_result = create_tracked_columns(deduplicate_employee_master_result)
    filter_version_starts_result = filter_version_starts(create_tracked_columns_result)
    add_validity_window_result = add_validity_window(filter_version_starts_result)
    create_employee_version_id_result = create_employee_version_id(add_validity_window_result)
    add_is_current_flag_result = add_is_current_flag(create_employee_version_id_result)
    return add_is_current_flag_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.employee_master_schema,
            unique_key=["employee_id", "snapshot_date"],
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
