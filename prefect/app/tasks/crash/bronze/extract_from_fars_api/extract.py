"""Extract tasks for fetching data from APIs."""

import requests
from datetime import timedelta
from typing import Any

import httpx

from prefect import task
from prefect.tasks import task_input_hash

from config.settings import settings

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://crashviewer.nhtsa.dot.gov/",
}


@task(
    name="fetch_crash_incidents",
    retries=0,
    retry_delay_seconds=settings.API_RETRY_DELAYS,
    log_prints=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(seconds=3600),
    tags=["extract", "api"],
)
def fetch_crash_incidents(
    stateCode: int, year: int, formatData: str = "json"
) -> list[dict[str, Any]]:
    """
    Fetch crash data from FARS API.

    Args:
        stateCode: FARS state code (1-56)
        year: Year to fetch data for
        formatData: Response format (default: json)

    Returns:
        List of crash records from FARS API
    """
    # Step 1: Prepare API request parameters
    url = "https://crashviewer.nhtsa.dot.gov/crashviewer/CrashAPI/FARSData/GetFARSData"
    params = {
        "dataset": "Accident",
        "FromYear": year,
        "ToYear": year,
        "state": stateCode,
        "format": formatData,
    }

    print(f"Fetching data from {url}...")

    # Step 2: GET request to FARS API
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        # Step 3: Parse JSON response
        raw_json = response.json()
    except httpx.HTTPStatusError as e:
        print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        raise
    except httpx.RequestError as e:
        print(f"Request error: {str(e)}")
        raise

    print(
        f"Successfully fetched crashes in state code {stateCode} in year {year}, total {len(raw_json.get('Results', [[]])[0])} crashes."
    )
    # Step 4: Return fetched records
    return raw_json
