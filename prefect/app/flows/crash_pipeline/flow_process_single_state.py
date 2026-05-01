"""Flow for processing a single state through full ETL: Bronze -> Silver -> Daily."""

import time
from typing import Optional

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from flows.crash_pipeline.flow_01_extract_fars_to_bronze import (
    extract_crash_data_to_bronze,
)
from flows.crash_pipeline.flow_02_parse_crash_cases_to_silver import (
    transform_crashes_to_silver,
)
from flows.crash_pipeline.flow_03_aggregate_daily import agregate_crashes_to_daily


@task(name="process_state_year", log_prints=True)
def process_state_year(
    state_code: int,
    year: int,
    format_data: str = "json",
) -> dict:
    """
    Process a single (state, year) combination through ETL.
    
    Args:
        state_code: FARS state code
        year: Year to process
        format_data: API response format
    
    Returns:
        Dict with counts for this (state, year)
    """
    result = {"state_code": state_code, "year": year}

    try:
        # Step 1: Extract to Bronze
        bronze_count = extract_crash_data_to_bronze(
            state_code=state_code,
            year=year,
            format_data=format_data,
        )
        result["bronze"] = bronze_count

        # Step 2: Transform to Silver
        silver_count = transform_crashes_to_silver(
            state_code=state_code,
            year=year,
        )
        result["silver"] = silver_count

        # Step 3: Aggregate to Daily
        daily_count = agregate_crashes_to_daily(
            state_code=state_code,
            year=year,
        )
        result["daily"] = daily_count

    except Exception as e:
        print(f"Error processing state {state_code}, year {year}: {e}")
        result["error"] = str(e)

    return result


@flow(name="process_single_state", task_runner=ConcurrentTaskRunner(), log_prints=True)
def process_single_state(
    state_code: int,
    state_name: str,
    start_year: int = 2012,
    end_year: int = 2015,
    format_data: str = "json",
) -> dict:
    """
    Process a single state for all years.
    
    Args:
        state_code: FARS state code
        state_name: State name for logging
        start_year: Start year
        end_year: End year
        format_data: API response format
    
    Returns:
        Dict with results for all years
    """
    print(f"Processing {state_name} ({state_code}): years {start_year}-{end_year}")

    # Submit all years for parallel processing
    futures = []
    for year in range(start_year, end_year + 1):
        future = process_state_year.submit(
            state_code=state_code,
            year=year,
            format_data=format_data,
        )
        futures.append(future)

        # Rate limiting: 1 second between submissions
        if year < end_year:
            time.sleep(1)

    # Wait for all years to complete
    results = []
    for future in futures:
        try:
            result = future.result()
            results.append(result)
        except Exception as e:
            print(f"Error getting result: {e}")
            results.append({"error": str(e)})

    # Aggregate
    total_bronze = sum(r.get("bronze", 0) for r in results if "bronze" in r)
    total_silver = sum(r.get("silver", 0) for r in results if "silver" in r)
    total_daily = sum(r.get("daily", 0) for r in results if "daily" in r)

    return {
        "state_code": state_code,
        "state_name": state_name,
        "bronze_inserted": total_bronze,
        "silver_upserted": total_silver,
        "daily_upserted": total_daily,
        "results": results,
    }


if __name__ == "__main__":
    process_single_state.serve(name="process-single-state")
