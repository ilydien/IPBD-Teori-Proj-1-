"""Flow for extracting crash data for all states to Bronze layer."""

import sys
import time
from pathlib import Path

sys.path.insert(0, "/prefect")

from prefect import flow

from flows.extract_crash_data_to_bronze import extract_crash_data_to_bronze
from utils.locations import locations


@flow(name="extract_all_crashes_to_bronze", log_prints=True)
def extract_all_crashes_to_bronze(
    start_year: int = 2012,
    end_year: int = 2015,
    format_data: str = "json",
) -> dict:
    """
    Extract crash data from FARS API for all states, year range to Bronze layer.
    
    Args:
        start_year: Start year (default: 2012)
        end_year: End year (default: 2015)
        format_data: API response format (default: json)
    
    Returns:
        Dict with summary: total_inserted, skipped, failed, total_states, total_years
    """
    # Step 1: Initialize counters
    total_inserted = 0
    skipped = 0
    failed = 0
    total_states = len(locations)
    total_years = end_year - start_year + 1
    total_tasks = total_states * total_years
    current_task = 0

    print(f"Starting extraction: {total_states} states x {total_years} years = {total_tasks} tasks")

    # Step 2: Loop through all states and years
    for loc in locations:
        state_code = loc["code"]
        state_name = loc["name"]

        for year in range(start_year, end_year + 1):
            current_task += 1
            print(f"[{current_task}/{total_tasks}] Processing {state_name} ({state_code}), year {year}...")

            try:
                # Step 3: Extract crash data for each state/year
                count = extract_crash_data_to_bronze(
                    state_code=state_code,
                    year=year,
                    format_data=format_data,
                )

                if count == 0:
                    skipped += 1
                else:
                    total_inserted += count

            except Exception as e:
                print(f" Error for {state_name} ({state_code}), year {year}: {e}")
                failed += 1

            # Rate limiting: 2 second delay (skip after last call)
            if not (state_code == locations[-1]["code"] and year == end_year):
                time.sleep(2)

    # Step 4: Return summary of extraction results
    return {
        "total_inserted": total_inserted,
        "skipped": skipped,
        "failed": failed,
        "total_states": total_states,
        "total_years": total_years,
        "total_tasks": total_tasks,
    }


if __name__ == "__main__":
    extract_all_crashes_to_bronze.serve(name="extract-all-crashes-to-bronze")
