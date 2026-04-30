"""Flow for running full crash ETL pipeline for all states to Silver + Daily."""

import sys
import time
from pathlib import Path

sys.path.insert(0, "/prefect")

from prefect import flow

from flows.extract_crash_data_to_bronze import extract_crash_data_to_bronze
from flows.transform_crashes_to_silver import transform_crashes_to_silver
from flows.agregate_to_daily import agregate_crashes_to_daily
from utils.locations import locations


@flow(name="run_all_crashes_to_silver", log_prints=True)
def run_all_crashes_to_silver(
    start_year: int = 2012,
    end_year: int = 2015,
    format_data: str = "json",
) -> dict:
    """
    Run complete crash ETL pipeline for all states: API -> Bronze -> Silver -> Daily.

    Args:
        start_year: Start year (default: 2012)
        end_year: End year (default: 2015)
        format_data: API response format (default: json)

    Returns:
        Dict with step results: bronze_inserted, silver_upserted, daily_upserted, failed
    """
    total_bronze = 0
    total_silver = 0
    total_daily = 0
    failed = 0
    total_states = len(locations)
    total_years = end_year - start_year + 1
    total_tasks = total_states * total_years
    current_task = 0

    print(
        f"Starting full ETL pipeline: {total_states} states x {total_years} years = {total_tasks} tasks"
    )

    for loc in locations:
        state_code = loc["code"]
        state_name = loc["name"]

        for year in range(start_year, end_year + 1):
            current_task += 1
            print(
                f"[{current_task}/{total_tasks}] Processing {state_name} ({state_code}), year {year}..."
            )

            try:
                # Step 1: Extract to Bronze
                bronze_count = extract_crash_data_to_bronze(
                    state_code=state_code,
                    year=year,
                    format_data=format_data,
                )
                total_bronze += bronze_count

                # Step 2: Transform to Silver (no check, allow updates)
                silver_count = transform_crashes_to_silver(
                    state_code=state_code,
                    year=year,
                )
                total_silver += silver_count

                # Step 3: Aggregate to Daily
                daily_count = agregate_crashes_to_daily(
                    state_code=state_code,
                    year=year,
                )
                total_daily += daily_count

            except Exception as e:
                print(f" Error for {state_name} ({state_code}), year {year}: {e}")
                failed += 1

            # Rate limiting: 2 second delay (skip after last call)
            if not (state_code == locations[-1]["code"] and year == end_year):
                time.sleep(2)

    return {
        "bronze_inserted": total_bronze,
        "silver_upserted": total_silver,
        "daily_upserted": total_daily,
        "failed": failed,
        "total_states": total_states,
        "total_years": total_years,
        "total_tasks": total_tasks,
    }


if __name__ == "__main__":
    run_all_crashes_to_silver.serve(name="run-all-crashes-to-silver")
