"""Flow for building weather-crash correlation Gold table."""

import sys
from pathlib import Path

sys.path.insert(0, "/prefect")

from prefect import flow

from tasks.crash.gold.weather_crash_correlation.correlation import (
    create_correlation_table,
    calculate_weather_crash_correlation,
    get_correlation_summary,
)


@flow(name="build_weather_crash_correlation", log_prints=True)
def build_weather_crash_correlation() -> dict:
    """
    Build weather-crash correlation Gold table.

    Returns:
        Dict with result summary
    """
    # Step 1: Create correlation table if not exists
    create_correlation_table()

    # Step 2: Calculate weather-crash correlation
    correlation_count = calculate_weather_crash_correlation()

    # Step 3: Get correlation summary
    summary_df = get_correlation_summary()

    return {
        "correlation_categories": correlation_count,
        "summary": summary_df.to_dict(orient="records") if not summary_df.empty else [],
    }


if __name__ == "__main__":
    build_weather_crash_correlation.serve(name="build-weather-crash-correlation")
