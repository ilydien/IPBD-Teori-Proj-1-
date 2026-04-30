"""Tasks for creating and populating Gold layer tables."""

import sys
from pathlib import Path

sys.path.insert(0, "/prefect")

from typing import Any

import pandas as pd
from sqlalchemy import text

from prefect import task

from settings import settings
from config.database import db_manager


@task(
    name="create_gold_table",
    log_prints=True,
    tags=["gold", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def create_gold_table() -> str:
    """
    Create gold.daily_crashes_weather table if not exists.

    Args:
        (None)
    Returns:
        Table name
    """
    table_name = "daily_crashes_weather"
    schema_name = "gold"

    create_schema_sql = text("CREATE SCHEMA IF NOT EXISTS gold")

    create_table_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            day INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            state TEXT NOT NULL,
            longitude DECIMAL(11, 8),
            latitude DECIMAL(10, 8),
            temperature_2m_max DECIMAL(5, 2),
            temperature_2m_min DECIMAL(5, 2),
            precipitation_sum DECIMAL(6, 2),
            temperature_2m_avg DECIMAL(5, 2),
            total_crashes INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, month, day, state)
        )
    """)

    create_index_sql = text(f"""
        CREATE INDEX IF NOT EXISTS idx_gold_daily_crashes_weather_date_state
            ON {schema_name}.{table_name} (year, month, day, state)
    """)

    with db_manager.get_connection() as connection:
        connection.execute(create_schema_sql)
        connection.execute(create_table_sql)
        connection.execute(create_index_sql)

    print(f"Created/verified table {schema_name}.{table_name}")
    return table_name


@task(
    name="populate_gold_table",
    log_prints=True,
    tags=["gold", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def populate_gold_table(
    year: int | None = None,
    state_name: str | None = None,
) -> int:
    """
    Populate Gold table by joining Silver crash and weather data.

    Args:
        year: Filter by year (optional)
        state_name: Filter by state (optional)

    Returns:
        Number of records upserted
    """
    table_name = "daily_crashes_weather"
    schema_name = "gold"

    conditions = []
    params = {}

    if year is not None:
        conditions.append("dw.year = :year")
        params["year"] = year

    if state_name is not None:
        conditions.append("dw.location_name = :state_name")
        params["state_name"] = state_name

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    upsert_sql = text(f"""
        INSERT INTO {schema_name}.{table_name}
            (day, month, year, state, longitude, latitude,
             temperature_2m_max, temperature_2m_min, precipitation_sum,
             temperature_2m_avg, total_crashes)
        SELECT
            dw.day, dw.month, dw.year, dw.location_name AS state,
            dw.longitude, dw.latitude,
            dw.temperature_2m_max, dw.temperature_2m_min, dw.precipitation_sum,
            (dw.temperature_2m_max + dw.temperature_2m_min) / 2 AS temperature_2m_avg,
            COALESCE(dc.total_crashes, 0) AS total_crashes
        FROM silver.daily_weather dw
        LEFT JOIN silver.daily_crashes dc
            ON dc.year = dw.year
            AND dc.month = dw.month
            AND dc.day = dw.day
            AND dc.state_name = dw.location_name
        {where_clause}
        ON CONFLICT (year, month, day, state) DO UPDATE SET
            longitude = EXCLUDED.longitude,
            latitude = EXCLUDED.latitude,
            temperature_2m_max = EXCLUDED.temperature_2m_max,
            temperature_2m_min = EXCLUDED.temperature_2m_min,
            precipitation_sum = EXCLUDED.precipitation_sum,
            temperature_2m_avg = EXCLUDED.temperature_2m_avg,
            total_crashes = EXCLUDED.total_crashes,
            created_at = CURRENT_TIMESTAMP
    """)

    with db_manager.get_connection() as connection:
        result = connection.execute(upsert_sql, params)
        count = result.rowcount

    print(f"Upserted {count} records to {schema_name}.{table_name}")
    return count


@task(
    name="get_gold_count",
    log_prints=True,
    tags=["gold", "database"],
)
def get_gold_count(
    year: int | None = None,
    state_name: str | None = None,
) -> int:
    """
    Get the number of rows in Gold table.

    Args:
        year: Filter by year (optional)
        state_name: Filter by state (optional)

    Returns:
        Number of records
    """
    table_name = "daily_crashes_weather"
    schema_name = "gold"

    query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
    params = {}

    conditions = []
    if year is not None:
        conditions.append("year = :year")
        params["year"] = year

    if state_name is not None:
        conditions.append("state = :state")
        params["state"] = state_name

    if conditions:
        query = text(f"{query.text} WHERE " + " AND ".join(conditions))

    with db_manager.get_connection() as connection:
        result = connection.execute(query, params)
        count = result.scalar()

    print(f"Gold table has {count} records")
    return count
