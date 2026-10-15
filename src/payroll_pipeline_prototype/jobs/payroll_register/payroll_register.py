"""Glue entry point of the payroll_register ETL job.

Scaffold-owned: `generate create-job` renders it from the job spec, and it is uploaded
unchanged as the Glue job's script.
"""

from payroll_pipeline_prototype.jobs.payroll_register import (
    payroll_register_transformations as job,
)
from payroll_pipeline_prototype.runtime.job_settings import job_run, phase
from payroll_pipeline_prototype.utils.python import resolve_processing_dates


def run() -> None:
    """Parse arguments and resolve each source's dates, then read, transform and write once."""
    args = job.parse_arguments()
    processing_dates: dict[str, list[str]] = {
        "payroll_register_raw": resolve_processing_dates(
            processing_type=args.PROCESSING_TYPE,
            lookback_days=args.PAYROLL_REGISTER_RAW_LOOKBACK_DAYS,
            start_date=args.PAYROLL_REGISTER_RAW_START_DATE,
            end_date=args.PAYROLL_REGISTER_RAW_END_DATE,
        ),
    }
    with job_run("payroll_register", args, processing_dates) as record:
        with phase(record, "read"):
            sources = job.read(args, processing_dates)
        with phase(record, "transform"):
            output = job.transform(**sources)
        with phase(record, "write"):
            job.write(output, args.OUTPUT_PATH, args.OUTPUT_TABLE)


if __name__ == "__main__":
    run()
