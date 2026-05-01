"""Flow for building daily crashes weather Gold table."""

import sys
from pathlib import Path

sys.path.insert(0, "/prefect")

from prefect import flow

from tasks.crash.gold.daily_crashes_weather.create_gold import (
    create_gold_table,
    populate_gold_table,
    get_gold_count,
)


@flow(name="build_daily_crashes_weather", log_prints=True)
def build_daily_crashes_weather(
    year: int | None = None,
    state_name: str | None = None,
) -> dict:
    """
    Build daily crashes weather Gold table by joining Silver crash and weather data.
    
    Args:
        year: Filter by year (optional)
        state_name: Filter by state (optional)
    
    Returns:
        Dict with result summary
    """
    # Step 1: Create Gold table if not exists
    create_gold_table()

    # Step 2: Populate Gold table by joining Silver crash and weather data
    upserted = populate_gold_table(
        year=year,
        state_name=state_name,
    )

    # Step 3: Get total count from Gold table
    total_count = get_gold_count()

    return {
        "upserted": upserted,
        "total_records": total_count,
    }


if __name__ == "__main__":
    build_daily_crashes_weather.serve(name="build-daily-crashes-weather")
