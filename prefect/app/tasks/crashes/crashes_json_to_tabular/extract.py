"""Extract tasks for reading crash data from Bronze layer."""

from typing import Any

import pandas as pd
from sqlalchemy import text

from prefect import task
from prefect.tasks import task_input_hash

from settings import settings
from config.database import db_manager


@task(
    name="select_from_bronze_crashes",
    retries=settings.API_MAX_RETRIES,
    retry_delay_seconds=settings.API_RETRY_DELAYS,
    log_prints=True,
    cache_key_fn=task_input_hash,
    tags=["extract", "database"],
)
def select_from_bronze_crashes(
    schema_name: str = "bronze",
    table_name: str = "fars_crashes",
    year: int | None = None,
    state_code: int | list[int] | None = None,
) -> pd.DataFrame:
    """
    Read crash JSON data from Bronze layer.

    Args:
        schema_name: Schema name (default: bronze)
        table_name: Table name (default: fars_crashes)
        year: Filter by year
        state_code: Filter by state_code(s)

    Returns:
        DataFrame with columns:  year, state_code, state_name, count, message, results
    """
    query = text(
        f"SELECT year, state_code, state_name, count, message, results FROM {schema_name}.{table_name}"
    )
    params = {}

    conditions = []
    if year is not None:
        conditions.append("year = :year")
        params["year"] = year
    if state_code is not None:
        if isinstance(state_code, list):
            conditions.append("state_code IN :state_codes")
            params["state_codes"] = tuple(state_code)
        else:
            conditions.append("state_code = :state_code")
            params["state_code"] = state_code

    if conditions:
        query = text(f"{query} WHERE " + " AND ".join(conditions))

    with db_manager.get_connection() as connection:
        result = connection.execute(query, params)
        rows = result.fetchall()

    df = pd.DataFrame(
        rows,
        columns=[
            "year",
            "state_code",
            "state_name",
            "count",
            "message",
            "results",
        ],
    )
    print(f"Loaded {len(df)} rows from bronze.{table_name}")
    return df
