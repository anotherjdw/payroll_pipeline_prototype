"""Transformations and run steps of the dim_employee ETL job.

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


def add_birth_year(employee_master: DataFrame) -> DataFrame:
    """Extracts birth year from date_of_birth and replaces the column with birth_year.

    Args:
        employee_master: DataFrame containing employee master data including a
            date_of_birth column of type date.

    Returns:
        DataFrame with the date_of_birth column removed and a new integer
        birth_year column containing the year extracted from date_of_birth.
    """
    return employee_master.withColumn("birth_year", F.year(F.col("date_of_birth"))).drop(
        "date_of_birth"
    )


def drop_pii_columns(add_birth_year_result: DataFrame) -> DataFrame:
    """Drops personally identifiable information columns from the employee DataFrame.

    Removes columns containing personal data including names, financial identifiers,
    address details, and sensitive personal attributes, retaining only non-PII fields.

    Args:
        add_birth_year_result: DataFrame containing full employee data including PII columns
            such as first_name, last_name, iban, nationality, sozialversicherungsnummer,
            street_address, postal_code, marital_status, steuer_id, city,
            religious_affiliation, and extract_timestamp.

    Returns:
        DataFrame with PII columns removed, retaining only non-sensitive employee fields.
    """
    pii_columns = [
        "first_name",
        "last_name",
        "iban",
        "nationality",
        "sozialversicherungsnummer",
        "street_address",
        "postal_code",
        "marital_status",
        "steuer_id",
        "city",
        "religious_affiliation",
        "extract_timestamp",
    ]
    return add_birth_year_result.drop(*pii_columns)


def adjust_first_version_valid_from(drop_pii_columns_result: DataFrame) -> DataFrame:
    """Adjusts valid_from for each employee's earliest version to cover pre-snapshot periods.

    For each employee, identifies the earliest record (by valid_from) and sets its
    valid_from to the minimum of the existing valid_from and the hire_date, ensuring
    that periods before the first snapshot are still matched to a version.

    Args:
        drop_pii_columns_result: DataFrame containing employee versioned records with
            valid_from, hire_date, and employee_id columns among others.

    Returns:
        DataFrame with the same schema as the input, where the valid_from of each
        employee's earliest record is adjusted to be the earlier of valid_from or
        hire_date.
    """
    window_spec = Window.partitionBy("employee_id").orderBy("valid_from")
    df = (
        drop_pii_columns_result.withColumn("_row_num", F.row_number().over(window_spec))
        .withColumn(
            "valid_from",
            F.when(
                F.col("_row_num") == 1, F.least(F.col("valid_from"), F.col("hire_date"))
            ).otherwise(F.col("valid_from")),
        )
        .drop("_row_num")
    )
    return df


def rename_employee_version_id(adjust_first_version_valid_from_result: DataFrame) -> DataFrame:
    """Renames the employee_version_id column to employee_key and selects the
    required output columns.

    Args:
        adjust_first_version_valid_from_result: DataFrame containing employee version data
            with an employee_version_id column and various employee attributes.

    Returns:
        DataFrame with employee_version_id renamed to employee_key, columns reordered
        and filtered to match the required output schema, with steuerklasse and
        num_children columns added as null placeholders if not present.
    """
    return adjust_first_version_valid_from_result.withColumnRenamed(
        "employee_version_id", "employee_key"
    ).select(
        F.col("snapshot_date"),
        F.col("employee_id"),
        F.col("birth_year"),
        F.col("work_location_code"),
        F.col("work_location_name"),
        F.col("department"),
        F.col("cost_center_code"),
        F.col("role"),
        F.col("pay_type"),
        F.col("employment_type"),
        F.col("contracted_hours_per_month"),
        F.col("annual_gross_eur"),
        F.col("hourly_rate_eur"),
        F.col("steuerklasse"),
        F.col("num_children"),
        F.col("childless_surcharge_applies"),
        F.col("church_tax_applies"),
        F.col("health_insurance_type"),
        F.col("health_insurance_fund"),
        F.col("manager_id"),
        F.col("hire_date"),
        F.col("termination_date"),
        F.col("job_level"),
        F.col("is_version_start"),
        F.col("valid_from"),
        F.col("valid_to"),
        F.col("employee_key"),
        F.col("is_current"),
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--EMPLOYEE_MASTER_INPUT_PATH", type=str)
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
        "employee_master": create_pyspark_dataframe(
            spark=spark,
            file_path=args.EMPLOYEE_MASTER_INPUT_PATH,
            schema=schema_definitions.employee_master_schema,
            file_format="parquet",
            date_ranges=processing_dates,
            partition_key="snapshot_date",
        ),
    }
    if sources["employee_master"].isEmpty():
        raise ValueError(
            f"Source 'employee_master' read no rows for {processing_dates}. "
            "To allow this, set allow_empty: true on this source in "
            "dim_employee.yaml."
        )
    return sources


def transform(employee_master: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    add_birth_year_result = add_birth_year(employee_master)
    drop_pii_columns_result = drop_pii_columns(add_birth_year_result)
    adjust_first_version_valid_from_result = adjust_first_version_valid_from(
        drop_pii_columns_result,
    )
    rename_employee_version_id_result = rename_employee_version_id(
        adjust_first_version_valid_from_result,
    )
    return rename_employee_version_id_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.dim_employee_schema,
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
