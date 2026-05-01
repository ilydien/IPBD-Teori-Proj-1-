"""Flow for running full crash ETL pipeline for all states to Silver + Daily with parallelization."""

import sys
import time
from pathlib import Path
from typing import Optional

sys.path.insert(0, "/prefect")

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from flows.crash_pipeline.flow_process_single_state import process_state_year
from utils.locations import locations


@flow(name="run_all_states_pipeline", task_runner=ConcurrentTaskRunner(), log_prints=True)
def run_all_states_pipeline(
    start_year: int = 2012,
    end_year: int = 2015,
    states: Optional[list[int]] = None,
    format_data: str = "json",
) -> dict:
    """
    Run complete crash ETL pipeline for all states in parallel.
    
    Args:
        start_year: Start year (default: 2012)
        end_year: End year (default: 2015)
        states: List of state codes (None = all states)
        format_data: API response format (default: json)
    
    Returns:
        Dict with aggregated results from all states
    """
    # Determine which states to process
    if states is None:
        states = [loc["code"] for loc in locations]
        state_names = {loc["code"]: loc["name"] for loc in locations}
    else:
        state_names = {loc["code"]: loc["name"] for loc in locations if loc["code"] in states}

    print(f"Starting parallel ETL pipeline: {len(states)} states, years {start_year}-{end_year}")

    # Submit each (state, year) combination for parallel processing
    futures = []
    for state_code in states:
        state_name = state_names.get(state_code, f"State {state_code}")
        for year in range(start_year, end_year + 1):
            future = process_state_year.submit(
                state_code=state_code,
                year=year,
                format_data=format_data,
            )
            futures.append(future)
            # Rate limiting: 1 second delay between submissions
            if not (state_code == states[-1] and year == end_year):
                time.sleep(1)

    # Wait for all states to complete and collect results
    results = []
    for future in futures:
        try:
            result = future.result()
            results.append(result)
        except Exception as e:
            print(f"Error getting result: {e}")
            results.append({"failed": 1, "error": str(e)})

    # Aggregate results
    total_bronze = sum(r.get("bronze_inserted", 0) for r in results)
    total_silver = sum(r.get("silver_upserted", 0) for r in results)
    total_daily = sum(r.get("daily_upserted", 0) for r in results)
    total_failed = sum(r.get("failed", 0) for r in results)

    print(f"\nPipeline complete: {len(results)} states processed")
    print(f"  Bronze: {total_bronze} records")
    print(f"  Silver: {total_silver} records")
    print(f"  Daily: {total_daily} records")
    print(f"  Failed: {total_failed}")

    return {
        "bronze_inserted": total_bronze,
        "silver_upserted": total_silver,
        "daily_upserted": total_daily,
        "failed": total_failed,
        "total_states": len(states),
        "total_years": end_year - start_year + 1,
        "results": results,
    }


if __name__ == "__main__":
    run_all_states_pipeline.serve(name="run-all-states-pipeline")
