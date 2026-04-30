"""Extract tasks for reading crash data from Silver layer."""

from typing import Any

import pandas as pd
from sqlalchemy import text

from prefect import task
from prefect.tasks import task_input_hash

from settings import settings
from config.database import db_manager


@task(
    name="select_from_silver_crashes",
    retries=settings.API_MAX_RETRIES,
    retry_delay_seconds=settings.API_RETRY_DELAYS,
    log_prints=True,
    cache_key_fn=task_input_hash,
    tags=["extract", "database"],
)
def select_from_silver_crashes(
    schema_name: str = "silver",
    table_name: str = "parsed_crashes_array",
    state_code: int | None = None,
    year: int | None = None,
) -> pd.DataFrame:
    """
    Read flattened crash data from Silver layer.
    
    Args:
        schema_name: Schema name (default: silver)
        table_name: Table name (default: parsed_crashes_array)
        start_year: Filter by year (optional)
        end_year: Filter by year (optional)
        state_code: Filter by state (optional)
    
    Returns:
        DataFrame with flattened crash columns
    """
    # Step 1: Build SELECT query for Silver table
    query = text(f"SELECT * FROM {schema_name}.{table_name}")
    params = {}

    conditions = []
    if year is not None:
        conditions.append("year >= :start_year")
        params["start_year"] = year
    if state_code is not None:
        if isinstance(state_code, list):
            conditions.append("state_code IN :state_codes")
            params["state_codes"] = tuple(state_code)
        else:
            conditions.append("state_code = :state_code")
            params["state_code"] = state_code

    # Step 2: Add WHERE conditions if filters exist
    if conditions:
        query = text(f"{query} WHERE " + " AND ".join(conditions))

    # Step 3: Execute query and fetch results
    with db_manager.get_connection() as connection:
        result = connection.execute(query, params)
        rows = result.fetchall()
        columns = result.keys()

    # Step 4: Convert results to DataFrame
    df = pd.DataFrame(rows, columns=columns)
    print(f"Loaded {len(df)} rows from silver.{table_name}")
    return df
