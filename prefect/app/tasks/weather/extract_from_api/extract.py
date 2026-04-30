"""Extract tasks for fetching weather data from Open-Meteo API."""

import sys
from pathlib import Path
sys.path.insert(0, "/prefect")

from typing import Any

import requests
from prefect import task
from prefect.tasks import task_input_hash
from datetime import timedelta

from settings import settings


@task(
    name="fetch_weather_from_api",
    retries=settings.API_MAX_RETRIES,
    retry_delay_seconds=settings.API_RETRY_DELAYS,
    log_prints=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    tags=["extract", "api", "weather"],
)
def fetch_weather_from_api(
    loc: dict[str, Any],
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    """
    Fetch weather data from Open-Meteo API for a location.
    
    Args:
        loc: Location dict with 'name', 'lat', 'lon'
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Returns:
        List of weather records
    """
    # Step 1: Prepare API request parameters
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": loc["lat"],
        "longitude": loc["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "auto",
    }

    print(f"Fetching weather data for {loc['name']} ({start_date} to {end_date})...")

    # Step 2: GET request to Open-Meteo API
    response = requests.get(url, params=params, timeout=settings.API_TIMEOUT)
    response.raise_for_status()
    data = response.json()

    # Step 3: Parse daily weather data
    records = []
    daily = data.get("daily", {})
    for i in range(len(daily.get("time", []))):
        records.append(
            {
                "location_name": loc["name"],
                "latitude": loc["lat"],
                "longitude": loc["lon"],
                "date": daily["time"][i],
                "temperature_2m_max": daily["temperature_2m_max"][i],
                "temperature_2m_min": daily["temperature_2m_min"][i],
                "precipitation_sum": daily["precipitation_sum"][i],
            }
        )

    # Step 4: Return fetched records
    print(f"Fetched {len(records)} records for {loc['name']}")
    return records