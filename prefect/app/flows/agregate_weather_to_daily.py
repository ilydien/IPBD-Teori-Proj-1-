"""Flow for aggregating weather data to daily level."""

import sys
from pathlib import Path
sys.path.insert(0, "/prefect")

from prefect import flow

from tasks.weather.agreate_to_daily.extract import select_from_silver_weather
from tasks.weather.agreate_to_daily.load import query_daily_weather_summary


@flow(name="agregate_weather_to_daily", log_prints=True)
def agregate_weather_to_daily(
    start_year: int = 2012,
    end_year: int = 2015,
    location_name: str | None = None,
) -> int:
    """
    Aggregate weather data to daily level via query mapping.

    Args:
        start_year: Start year (default: 2012)
        end_year: End year (default: 2015)
        location_name: Filter by location (optional)

    Returns:
        Number of aggregated records
    """
    aggregated_dfs = []
    for year in range(start_year, end_year + 1):
        df = query_daily_weather_summary(year=year, location_name=location_name)
        if not df.empty:
            aggregated_dfs.append(df)

    if not aggregated_dfs:
        print("No data to aggregate")
        return 0

    import pandas as pd

    total_df = pd.concat(aggregated_dfs, ignore_index=True)
    print(f"Total aggregated records: {len(total_df)}")

    return len(total_df)


if __name__ == "__main__":
    agregate_weather_to_daily.serve(name="agregate-weather-to-daily")