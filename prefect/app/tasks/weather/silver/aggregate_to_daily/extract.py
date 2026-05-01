"""Extract tasks for reading weather data from Silver layer."""

import sys
from pathlib import Path

sys.path.insert(0, "/prefect")

from typing import Any

import pandas as pd
from sqlalchemy import text

from prefect import task
from prefect.tasks import task_input_hash

from config.settings import settings
from utils.database import db_manager


@task(
    name="select_from_silver_weather",
    log_prints=True,
    cache_key_fn=task_input_hash,
    tags=["extract", "database", "weather"],
)
def select_from_silver_weather(
    schema_name: str = "silver",
    table_name: str = "daily_weather",
    year: int | None = None,
    location_name: str | None = None,
) -> pd.DataFrame:
    """
    Read weather data from Silver layer.

    Args:
        schema_name: Schema name (default: silver)
        table_name: Table name (default: daily_weather)
        year: Filter by year (optional)
        location_name: Filter by location name (optional)

    Returns:
        DataFrame with weather data
    """
    # Step1: Build SELECT query for Silver table
    query = text(f"SELECT * FROM {schema_name}.{table_name}")
    params = {}

    conditions = []
    if year is not None:
        conditions.append("year = :year")
        params["year"] = year
    if location_name is not None:
        conditions.append("location_name = :location_name")
        params["location_name"] = location_name

    # Step2: Add WHERE conditions if filters exist
    if conditions:
        query = text(f"{query} WHERE " + " AND ".join(conditions))

    # Step3: Execute query and fetch results
    with db_manager.get_connection() as connection:
        result = connection.execute(query, params)
        rows = result.fetchall()
        columns = result.keys()

    # Step4: Convert results to DataFrame
    df = pd.DataFrame(rows, columns=columns)
    print(f"Loaded {len(df)} rows from {schema_name}.{table_name}")
    return df

