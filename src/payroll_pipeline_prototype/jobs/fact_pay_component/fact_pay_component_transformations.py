"""Transformations and run steps of the fact_pay_component ETL job.

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


def add_period_end_date(payroll_register: DataFrame) -> DataFrame:
    """Adds a period_end_date column representing the last day of the month of pay_date.

    Args:
        payroll_register: DataFrame containing payroll register records with a pay_date column.

    Returns:
        DataFrame with all original columns plus period_end_date, which is the last
        calendar day of the month corresponding to pay_date.
    """
    return payroll_register.withColumn("period_end_date", F.last_day(F.col("pay_date")))


def select_employee_data(dim_employee: DataFrame) -> DataFrame:
    """Selects key employee columns needed for joining employee data to payroll data.

    Args:
        dim_employee: DataFrame containing the full employee dimension with SCD2
            versioning columns and all employee attributes.

    Returns:
        DataFrame containing only employee_id, valid_from, valid_to, and
        employee_key columns required for payroll data joins.
    """
    return dim_employee.select(
        F.col("employee_id"), F.col("valid_from"), F.col("valid_to"), F.col("employee_key")
    )


def select_cost_center_data(dim_cost_center: DataFrame) -> DataFrame:
    """Selects cost center columns needed for joining to payroll data.

    Args:
        dim_cost_center: DataFrame containing dimensional cost center data
            with snapshot, validity, and key columns.

    Returns:
        DataFrame containing only cost_center_code, valid_from, valid_to,
        and cost_center_key columns for use in payroll data joins.
    """
    return dim_cost_center.select(
        F.col("cost_center_code"), F.col("valid_from"), F.col("valid_to"), F.col("cost_center_key")
    )


def select_location_data(dim_location: DataFrame) -> DataFrame:
    """Selects location columns needed for joining location data to payroll data.

    Args:
        dim_location: DataFrame containing dimensional location data with full
            location attributes including validity period and location identifiers.

    Returns:
        DataFrame containing only location_code, valid_from, valid_to, and
        location_key columns required for joining to payroll data.
    """
    return dim_location.select(
        F.col("location_code"), F.col("valid_from"), F.col("valid_to"), F.col("location_key")
    )


def select_pay_component_data(dim_pay_component: DataFrame) -> DataFrame:
    """Selects pay component columns needed for joining to payroll data.

    Args:
        dim_pay_component: DataFrame containing dimensional pay component data
            with multiple attributes including lohnart_code, pay_component_key,
            valid_from, and valid_to columns.

    Returns:
        DataFrame containing only valid_from, valid_to, lohnart_code, and
        pay_component_key columns required for joining to payroll data.
    """
    return dim_pay_component.select(
        F.col("valid_from"), F.col("valid_to"), F.col("lohnart_code"), F.col("pay_component_key")
    )


def add_date_key(add_period_end_date_result: DataFrame) -> DataFrame:
    """Adds an integer date key column derived from period_end_date.

    Converts the period_end_date column into an integer in yyyymmdd format,
    enabling a join to a dim_date dimension table.

    Args:
        add_period_end_date_result: DataFrame containing payroll data with a
            period_end_date column of type date.

    Returns:
        DataFrame with all original columns plus a date_key column of type int
        representing period_end_date in yyyymmdd format.
    """
    return add_period_end_date_result.withColumn(
        "date_key", F.date_format(F.col("period_end_date"), "yyyyMMdd").cast("int")
    )


def add_pay_run_key(add_date_key_result: DataFrame) -> DataFrame:
    """Adds an MD5-hashed surrogate key column derived from pay_run_id.

    Args:
        add_date_key_result: DataFrame containing payroll data with a pay_run_id column.

    Returns:
        DataFrame with all original columns plus a pay_run_key column containing
        the MD5 hash of pay_run_id.
    """
    return add_date_key_result.withColumn("pay_run_key", F.md5(F.col("pay_run_id")))


def payroll_data(add_pay_run_key_result: DataFrame) -> DataFrame:
    """Selects relevant payroll columns for the output fact table.

    Args:
        add_pay_run_key_result: DataFrame containing full payroll data including
            pay run keys, period information, employee details, and financial amounts.

    Returns:
        DataFrame containing only the columns required for the payroll fact table,
        including period identifiers, employee and cost information, and financial metrics.
    """
    return add_pay_run_key_result.select(
        F.col("pay_period"),
        F.col("employee_id"),
        F.col("cost_center_code"),
        F.col("location_code"),
        F.col("lohnart_code"),
        F.col("gl_account"),
        F.col("quantity"),
        F.col("rate"),
        F.col("assessment_base_eur"),
        F.col("amount_eur"),
        F.col("period_end_date"),
        F.col("date_key"),
        F.col("pay_run_key"),
    )


def join_employee_key_to_payroll(
    payroll_data_result: DataFrame, select_employee_data_result: DataFrame
) -> DataFrame:
    """Left join employee data to payroll data on employee_id with date range filtering.

    Performs a left join from payroll data to employee data matching on employee_id,
    where period_end_date falls within the employee record's validity window
    (>= valid_from and either valid_to is null or < valid_to).

    Args:
        payroll_data_result: DataFrame containing payroll records with period_end_date
            and employee_id columns among others.
        select_employee_data_result: DataFrame containing employee dimension records
            with employee_id, valid_from, valid_to, and employee_key columns.

    Returns:
        DataFrame with the original payroll columns plus employee_key, joined using
        a temporal left join on employee_id and period_end_date validity range.
    """
    join_condition = (
        (payroll_data_result["employee_id"] == select_employee_data_result["employee_id"])
        & (payroll_data_result["period_end_date"] >= select_employee_data_result["valid_from"])
        & (
            select_employee_data_result["valid_to"].isNull()
            | (payroll_data_result["period_end_date"] < select_employee_data_result["valid_to"])
        )
    )
    return payroll_data_result.join(
        select_employee_data_result, on=join_condition, how="left"
    ).select(
        payroll_data_result["pay_period"],
        payroll_data_result["employee_id"],
        payroll_data_result["cost_center_code"],
        payroll_data_result["location_code"],
        payroll_data_result["lohnart_code"],
        payroll_data_result["gl_account"],
        payroll_data_result["quantity"],
        payroll_data_result["rate"],
        payroll_data_result["assessment_base_eur"],
        payroll_data_result["amount_eur"],
        payroll_data_result["period_end_date"],
        payroll_data_result["date_key"],
        payroll_data_result["pay_run_key"],
        select_employee_data_result["employee_key"],
    )


def join_cost_center_key_to_payroll(
    join_employee_key_to_payroll_result: DataFrame, select_cost_center_data_result: DataFrame
) -> DataFrame:
    """Left join cost center dimension data to payroll data on cost_center_code and date validity.

    Args:
        join_employee_key_to_payroll_result: Payroll DataFrame containing employee and pay period
        data.
        select_cost_center_data_result: Cost center dimension DataFrame with validity date
        ranges.

    Returns:
        DataFrame with payroll data enriched with cost_center_key from the cost center dimension,
        matched on cost_center_code and where period_end_date falls within the valid_from/valid_to
        range.
    """
    payroll = join_employee_key_to_payroll_result
    cost_center = select_cost_center_data_result
    cc = cost_center.alias("cc")
    pr = payroll.alias("pr")
    join_condition = (
        (F.col("pr.cost_center_code") == F.col("cc.cost_center_code"))
        & (F.col("pr.period_end_date") >= F.col("cc.valid_from"))
        & (F.col("cc.valid_to").isNull() | (F.col("pr.period_end_date") < F.col("cc.valid_to")))
    )
    result = pr.join(cc, join_condition, "left").select(
        F.col("pr.pay_period"),
        F.col("pr.employee_id"),
        F.col("pr.cost_center_code"),
        F.col("pr.location_code"),
        F.col("pr.lohnart_code"),
        F.col("pr.gl_account"),
        F.col("pr.quantity"),
        F.col("pr.rate"),
        F.col("pr.assessment_base_eur"),
        F.col("pr.amount_eur"),
        F.col("pr.period_end_date"),
        F.col("pr.date_key"),
        F.col("pr.pay_run_key"),
        F.col("pr.employee_key"),
        F.col("cc.cost_center_key"),
    )
    return result


def join_location_key_to_payroll(
    join_cost_center_key_to_payroll_result: DataFrame, select_location_data_result: DataFrame
) -> DataFrame:
    """Left join location dimension data to payroll data on location_code and date validity.

    Args:
        join_cost_center_key_to_payroll_result: Payroll DataFrame containing cost center key
            and related payroll fields including period_end_date and location_code.
        select_location_data_result: Location dimension DataFrame containing location_key,
            location_code, valid_from, and valid_to date range columns.

    Returns:
        DataFrame with all payroll columns plus location_key from the location dimension,
        joined on location_code where period_end_date falls within the location's validity range.
    """
    join_condition = (
        (
            join_cost_center_key_to_payroll_result["location_code"]
            == select_location_data_result["location_code"]
        )
        & (
            join_cost_center_key_to_payroll_result["period_end_date"]
            >= select_location_data_result["valid_from"]
        )
        & (
            select_location_data_result["valid_to"].isNull()
            | (
                join_cost_center_key_to_payroll_result["period_end_date"]
                < select_location_data_result["valid_to"]
            )
        )
    )
    return join_cost_center_key_to_payroll_result.join(
        select_location_data_result, join_condition, "left"
    ).select(
        join_cost_center_key_to_payroll_result["pay_period"],
        join_cost_center_key_to_payroll_result["employee_id"],
        join_cost_center_key_to_payroll_result["cost_center_code"],
        join_cost_center_key_to_payroll_result["location_code"],
        join_cost_center_key_to_payroll_result["lohnart_code"],
        join_cost_center_key_to_payroll_result["gl_account"],
        join_cost_center_key_to_payroll_result["quantity"],
        join_cost_center_key_to_payroll_result["rate"],
        join_cost_center_key_to_payroll_result["assessment_base_eur"],
        join_cost_center_key_to_payroll_result["amount_eur"],
        join_cost_center_key_to_payroll_result["period_end_date"],
        join_cost_center_key_to_payroll_result["date_key"],
        join_cost_center_key_to_payroll_result["pay_run_key"],
        join_cost_center_key_to_payroll_result["employee_key"],
        join_cost_center_key_to_payroll_result["cost_center_key"],
        select_location_data_result["location_key"],
    )


def join_pay_component_key_to_payroll(
    join_location_key_to_payroll_result: DataFrame, select_pay_component_data_result: DataFrame
) -> DataFrame:
    """Left joins dim_pay_component data to payroll data on lohnart_code with date range filtering.

    Args:
        join_location_key_to_payroll_result: Payroll DataFrame containing lohnart_code and
        period_end_date.
        select_pay_component_data_result: Pay component DataFrame with validity date range and
        lohnart_code.

    Returns:
        DataFrame with all payroll columns plus pay_component_key from the left join.
    """
    payroll = join_location_key_to_payroll_result
    pay_comp = select_pay_component_data_result
    join_condition = (
        (payroll["lohnart_code"] == pay_comp["lohnart_code"])
        & (payroll["period_end_date"] >= pay_comp["valid_from"])
        & (pay_comp["valid_to"].isNull() | (payroll["period_end_date"] < pay_comp["valid_to"]))
    )
    result = payroll.join(pay_comp, on=join_condition, how="left").select(
        payroll["pay_period"],
        payroll["employee_id"],
        payroll["cost_center_code"],
        payroll["location_code"],
        payroll["lohnart_code"],
        payroll["gl_account"],
        payroll["quantity"],
        payroll["rate"],
        payroll["assessment_base_eur"],
        payroll["amount_eur"],
        payroll["period_end_date"],
        payroll["date_key"],
        payroll["pay_run_key"],
        payroll["employee_key"],
        payroll["cost_center_key"],
        payroll["location_key"],
        pay_comp["pay_component_key"],
    )
    return result


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--PAYROLL_REGISTER_INPUT_PATH", type=str)
    parser.add_argument("--DIM_EMPLOYEE_INPUT_PATH", type=str)
    parser.add_argument("--DIM_COST_CENTER_INPUT_PATH", type=str)
    parser.add_argument("--DIM_LOCATION_INPUT_PATH", type=str)
    parser.add_argument("--DIM_PAY_COMPONENT_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--PAYROLL_REGISTER_LOOKBACK_DAYS", type=int)
    parser.add_argument("--PAYROLL_REGISTER_START_DATE", type=str)
    parser.add_argument("--PAYROLL_REGISTER_END_DATE", type=str)
    parser.add_argument("--DIM_EMPLOYEE_LOOKBACK_DAYS", type=int)
    parser.add_argument("--DIM_EMPLOYEE_START_DATE", type=str)
    parser.add_argument("--DIM_EMPLOYEE_END_DATE", type=str)
    parser.add_argument("--DIM_COST_CENTER_LOOKBACK_DAYS", type=int)
    parser.add_argument("--DIM_COST_CENTER_START_DATE", type=str)
    parser.add_argument("--DIM_COST_CENTER_END_DATE", type=str)
    parser.add_argument("--DIM_LOCATION_LOOKBACK_DAYS", type=int)
    parser.add_argument("--DIM_LOCATION_START_DATE", type=str)
    parser.add_argument("--DIM_LOCATION_END_DATE", type=str)
    parser.add_argument("--DIM_PAY_COMPONENT_LOOKBACK_DAYS", type=int)
    parser.add_argument("--DIM_PAY_COMPONENT_START_DATE", type=str)
    parser.add_argument("--DIM_PAY_COMPONENT_END_DATE", type=str)
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
        "dim_employee": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DIM_EMPLOYEE_INPUT_PATH,
            schema=schema_definitions.dim_employee_schema,
            file_format="parquet",
            date_ranges=processing_dates["dim_employee"],
            partition_key="snapshot_date",
        ),
        "dim_cost_center": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DIM_COST_CENTER_INPUT_PATH,
            schema=schema_definitions.dim_cost_center_schema,
            file_format="parquet",
            date_ranges=processing_dates["dim_cost_center"],
            partition_key="snapshot_date",
        ),
        "dim_location": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DIM_LOCATION_INPUT_PATH,
            schema=schema_definitions.dim_location_schema,
            file_format="parquet",
            date_ranges=processing_dates["dim_location"],
            partition_key="snapshot_date",
        ),
        "dim_pay_component": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DIM_PAY_COMPONENT_INPUT_PATH,
            schema=schema_definitions.dim_pay_component_schema,
            file_format="parquet",
            date_ranges=processing_dates["dim_pay_component"],
            partition_key="valid_from",
        ),
    }
    if sources["payroll_register"].isEmpty():
        raise ValueError(
            f"Source 'payroll_register' read no rows for "
            f"{processing_dates['payroll_register']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_pay_component.yaml."
        )
    if sources["dim_employee"].isEmpty():
        raise ValueError(
            f"Source 'dim_employee' read no rows for "
            f"{processing_dates['dim_employee']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_pay_component.yaml."
        )
    if sources["dim_cost_center"].isEmpty():
        raise ValueError(
            f"Source 'dim_cost_center' read no rows for "
            f"{processing_dates['dim_cost_center']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_pay_component.yaml."
        )
    if sources["dim_location"].isEmpty():
        raise ValueError(
            f"Source 'dim_location' read no rows for "
            f"{processing_dates['dim_location']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_pay_component.yaml."
        )
    if sources["dim_pay_component"].isEmpty():
        raise ValueError(
            f"Source 'dim_pay_component' read no rows for "
            f"{processing_dates['dim_pay_component']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_pay_component.yaml."
        )
    return sources


def transform(
    payroll_register: DataFrame,
    dim_employee: DataFrame,
    dim_cost_center: DataFrame,
    dim_location: DataFrame,
    dim_pay_component: DataFrame,
) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    add_period_end_date_result = add_period_end_date(payroll_register)
    select_employee_data_result = select_employee_data(dim_employee)
    select_cost_center_data_result = select_cost_center_data(dim_cost_center)
    select_location_data_result = select_location_data(dim_location)
    select_pay_component_data_result = select_pay_component_data(dim_pay_component)
    add_date_key_result = add_date_key(add_period_end_date_result)
    add_pay_run_key_result = add_pay_run_key(add_date_key_result)
    payroll_data_result = payroll_data(add_pay_run_key_result)
    join_employee_key_to_payroll_result = join_employee_key_to_payroll(
        payroll_data_result,
        select_employee_data_result,
    )
    join_cost_center_key_to_payroll_result = join_cost_center_key_to_payroll(
        join_employee_key_to_payroll_result,
        select_cost_center_data_result,
    )
    join_location_key_to_payroll_result = join_location_key_to_payroll(
        join_cost_center_key_to_payroll_result,
        select_location_data_result,
    )
    join_pay_component_key_to_payroll_result = join_pay_component_key_to_payroll(
        join_location_key_to_payroll_result,
        select_pay_component_data_result,
    )
    return join_pay_component_key_to_payroll_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.fact_pay_component_schema,
            unique_key=["employee_id", "pay_run_key", "lohnart_code"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="period_end_date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="period_end_date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
