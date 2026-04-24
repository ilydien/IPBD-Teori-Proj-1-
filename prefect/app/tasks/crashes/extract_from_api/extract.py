"""Extract tasks for fetching data from APIs."""

from datetime import timedelta
from typing import Any

import httpx

from prefect import task
from prefect.tasks import task_input_hash

from settings import settings
from config.api import APIClient


@task(
    name="fetch_crash_incidents",
    retries=settings.API_MAX_RETRIES,
    retry_delay_seconds=settings.API_RETRY_DELAYS,
    log_prints=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(seconds=3600),
    tags=["extract", "api"],
)
def fetch_crash_incidents(
    stateCode: int, startYear: int, endYear: int, formatData: str = "json"
) -> list[dict[str, Any]]:
    """
    Fetch posts from a REST API endpoint.

    Args:
        api_url: Optional custom API URL (defaults to settings.API_BASE_URL)
        limit: Maximum number of posts to fetch

    Returns:
        List of post dictionaries
    """
    url = "https://crashviewer.nhtsa.dot.gov/crashviewer/CrashAPI/FARSData/GetFARSData"
    params = {
        "dataset": "Accident",
        "FromYear": startYear,
        "ToYear": endYear,
        "state": stateCode,
        "format": formatData,
    }

    print(f"Fetching data from {url}...")

    try:
        with httpx.Client(timeout=settings.API_TIMEOUT) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            raw_json = response.json()
    except httpx.HTTPStatusError as e:
        print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        raise
    except httpx.RequestError as e:
        print(f"Request error: {str(e)}")
        raise

    print(
        f"Successfully fetched crashes in state code {stateCode} from year {startYear} to year {endYear}, total {len(raw_json.get('Results', [[]])[0])} crashes."
    )
    return raw_json


# @task(
#     name="fetch_posts_with_client",
#     retries=settings.API_MAX_RETRIES,
#     retry_delay_seconds=settings.API_RETRY_DELAYS,
#     log_prints=True,
#     tags=["extract", "api"],
# )
# def fetch_posts_with_client(limit: int = 10) -> list[dict[str, Any]]:
#     """
#     Alternative extraction using the APIClient class.
#     Better for complex API interactions.
#     """
#     with APIClient() as client:
#         posts = client.get("/posts", params={"_limit": limit})
#
#     print(f"Successfully fetched {len(posts)} posts")
#     return posts
