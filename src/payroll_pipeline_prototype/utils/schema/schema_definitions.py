"""One Spark StructType per data contract used by a rendered job.

Tool-owned and rebuilt whole from the contracts in `contracts/`. Edit the contracts, not
this file.
"""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

payroll_register_schema = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=False),
        StructField("pay_period", StringType(), nullable=False),
        StructField("pay_date", DateType(), nullable=False),
        StructField("run_type", StringType(), nullable=False),
        StructField("employee_id", StringType(), nullable=False),
        StructField("cost_center_code", StringType(), nullable=False),
        StructField("location_code", StringType(), nullable=False),
        StructField("lohnart_code", StringType(), nullable=False),
        StructField("component_name", StringType(), nullable=False),
        StructField("bearer", StringType(), nullable=False),
        StructField("gl_account", StringType(), nullable=False),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=False),
    ]
)

payroll_register_raw_schema = StructType(
    [
        StructField("pay_run_id", StringType(), nullable=False),
        StructField("pay_period", StringType(), nullable=False),
        StructField("pay_date", DateType(), nullable=False),
        StructField("run_type", StringType(), nullable=False),
        StructField("employee_id", StringType(), nullable=False),
        StructField("cost_center_code", StringType(), nullable=False),
        StructField("location_code", StringType(), nullable=False),
        StructField("lohnart_code", StringType(), nullable=False),
        StructField("component_name", StringType(), nullable=False),
        StructField("bearer", StringType(), nullable=False),
        StructField("gl_account", StringType(), nullable=False),
        StructField("quantity", DecimalType(12, 4), nullable=True),
        StructField("rate", DecimalType(12, 6), nullable=True),
        StructField("assessment_base_eur", DecimalType(14, 2), nullable=True),
        StructField("amount_eur", DecimalType(14, 2), nullable=False),
    ]
)
