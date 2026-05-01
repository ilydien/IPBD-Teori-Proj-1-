"""Master flow for running all crash ETL steps for a single state."""

from prefect import flow

from flows.crash_pipeline.flow_01_extract_fars_to_bronze import (
    extract_crash_data_to_bronze,
)
from flows.crash_pipeline.flow_02_parse_crash_cases_to_silver import (
    transform_crashes_to_silver,
)
from flows.crash_pipeline.flow_03_aggregate_daily import agregate_crashes_to_daily


@flow(name="run_crash_etl", log_prints=True)
def run_crash_etl(
    stateCode: int, startYear: int, endYear: int, formatData: str = "json"
) -> dict:
    """
    Run complete crash ETL pipeline: API -> Bronze -> Silver -> Daily.

    Step 1: Fetch from FARS API and load to Bronze layer
    Step 2: Transform Bronze data to Silver layer
    Step 3: Aggregate to daily level

    Args:
        stateCode: FARS state code (1-56)
        startYear: Start year
        endYear: End year
        formatData: API response format

    Returns:
        Dict with step results
    """
    # Step 1: Extract crash data from API to Bronze layer
    step1_count = extract_crash_data_to_bronze(
        stateCode=stateCode,
        startYear=startYear,
        endYear=endYear,
        formatData=formatData,
    )

    # Step 2: Transform Bronze data to Silver layer
    step2_count = transform_crashes_to_silver(
        start_year=startYear,
        end_year=endYear,
        state_code=stateCode,
    )

    # Step 3: Aggregate Silver data to daily level
    step3_count = agregate_crashes_to_daily(
        start_year=startYear,
        end_year=endYear,
        state_code=stateCode,
    )

    return {
        "bronze_inserted": step1_count,
        "silver_upserted": step2_count,
        "daily_upserted": step3_count,
    }


if __name__ == "__main__":
    run_crash_etl.serve(name="run-crash-etl")
