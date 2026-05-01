"""Load tasks for inserting weather data to Silver layer."""

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
    name="create_weather_table",
    log_prints=True,
    tags=["load", "database", "weather"],
)
def create_weather_table(
    schema_name: str = "silver",
    table_name: str = "daily_weather",
) -> str:
    """
    Create weather table if not exists.

    Args:
        schema_name: Schema name (default: silver)
        table_name: Table name (default: daily_weather)

    Returns:
        Table name
    """
    # Step 1: Create Silver schema if not exists
    create_schema_sql = text("CREATE SCHEMA IF NOT EXISTS silver")

    # Step 2: Create daily_weather table with unique constraint
    create_table_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            location_name VARCHAR(100),
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            date DATE,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            temperature_2m_max DECIMAL(5, 2),
            temperature_2m_min DECIMAL(5, 2),
            precipitation_sum DECIMAL(6, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(location_name, date)
        )
    """)

    # Step 3: Execute DDL statements
    with db_manager.get_connection() as connection:
        connection.execute(create_schema_sql)
        connection.execute(create_table_sql)

    print(f"Created/verified table {schema_name}.{table_name}")
    return table_name


@task(
    name="insert_weather_to_silver",
    log_prints=True,
    tags=["load", "database", "weather"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def insert_weather_to_silver(
    records: list[dict[str, Any]],
    schema_name: str = "silver",
    table_name: str = "daily_weather",
) -> int:
    """
    Insert weather data to Silver layer with date split (year, month, day).

    Args:
        records: List of weather records from API
        schema_name: Target schema (default: silver)
        table_name: Target table (default: daily_weather)

    Returns:
        Number of records inserted
    """
    # Step 1: Return if no records
    if not records:
        print("No records to insert")
        return 0

    # Step 2: Convert records to DataFrame
    df = pd.DataFrame(records)

    # Parse date to extract year, month, day in Python
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # Step 3: Build upsert SQL query
    insert_sql = text(f"""
        INSERT INTO {schema_name}.{table_name}
            (location_name, latitude, longitude, date, year, month, day,
             temperature_2m_max, temperature_2m_min, precipitation_sum)
        VALUES (
            :location_name, :latitude, :longitude, :date,
            :year,
            :month,
            :day,
            :temperature_2m_max, :temperature_2m_min, :precipitation_sum
        )
        ON CONFLICT (location_name, date) DO UPDATE SET
            temperature_2m_max = EXCLUDED.temperature_2m_max,
            temperature_2m_min = EXCLUDED.temperature_2m_min,
            precipitation_sum = EXCLUDED.precipitation_sum,
            year = EXCLUDED.year,
            month = EXCLUDED.month,
            day = EXCLUDED.day,
            created_at = CURRENT_TIMESTAMP
    """)

    # Step 4: Convert to records format
    records_dict = df.to_dict(orient="records")

    # Step 5: Execute upsert for each record
    with db_manager.get_connection() as connection:
        for r in records_dict:
            connection.execute(insert_sql, r)

    # Step 6: Log result and return count
    print(f"Inserted {len(records_dict)} records to {schema_name}.{table_name}")
    return len(records_dict)

