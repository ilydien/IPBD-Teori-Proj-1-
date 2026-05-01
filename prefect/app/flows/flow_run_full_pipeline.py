"""Master flow for running full pipeline: API -> Bronze -> Silver -> Gold with parallelization."""

import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, "/prefect")

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

from flows.crash_pipeline.run_all_states_pipeline import run_all_states_pipeline
from flows.weather_pipeline.flow_01_extract_openmeteo_to_silver import (
    extract_weather_to_silver,
)
from flows.weather_pipeline.flow_02_aggregate_weather_daily import (
    agregate_weather_to_daily,
)
from flows.gold_pipeline.build_gold_layer import build_gold_layer


@task(name="run_crash_pipeline_task", log_prints=True)
def run_crash_pipeline_task(
    start_year: int = 2012,
    end_year: int = 2015,
    states: Optional[list[int]] = None,
    format_data: str = "json",
) -> dict:
    """Task wrapper for crash pipeline (supports parallel per state)."""
    return run_all_states_pipeline(
        start_year=start_year,
        end_year=end_year,
        states=states,
        format_data=format_data,
    )


@task(name="run_weather_pipeline_task", log_prints=True)
def run_weather_pipeline_task(
    start_year: int = 2012,
    end_year: int = 2015,
) -> dict:
    """Task wrapper for weather pipeline."""
    weather_count = extract_weather_to_silver(start_year=start_year, end_year=end_year)
    agregate_weather_to_daily(start_year=start_year, end_year=end_year)
    return {"weather_inserted": weather_count}


@flow(name="run_full_pipeline", task_runner=ConcurrentTaskRunner())
def run_full_pipeline(
    start_year: int = 2012,
    end_year: int = 2015,
    states: Optional[list[int]] = None,
    format_data: str = "json",
) -> dict:
    """
    Run full pipeline: Crash (parallel per state) + Weather (parallel) -> Gold.

    Args:
        start_year: Start year (default: 2012)
        end_year: End year (default: 2015)
        states: List of state codes (None = all states)
        format_data: API response format (default: json)

    Returns:
        Dict with all pipeline results
    """
    print(f"Starting full pipeline: {start_year}-{end_year}, states={states or 'ALL'}")

    # Step 1: Submit crash pipeline (parallel per state) using task wrapper
    crash_future = run_crash_pipeline_task.submit(
        start_year=start_year,
        end_year=end_year,
        states=states,
        format_data=format_data,
    )

    # Step 2: Submit weather pipeline (runs parallel with crash) using task wrapper
    weather_future = run_weather_pipeline_task.submit(
        start_year=start_year,
        end_year=end_year,
    )

    # Step 3: Wait for both pipelines to complete
    crash_result = crash_future.result()
    weather_result = weather_future.result()

    print(f"Crash pipeline completed: {crash_result}")
    print(f"Weather pipeline completed: {weather_result}")

    # Step 4: Build Gold layer (after both pipelines complete)
    gold_result = build_gold_layer()

    return {
        "crash": crash_result,
        "weather": weather_result,
        "gold": gold_result,
    }


if __name__ == "__main__":
    run_full_pipeline.serve(name="run-full-pipeline")
