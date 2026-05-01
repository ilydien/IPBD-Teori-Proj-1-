"""Tasks for weather-crash correlation analysis."""

import sys
from pathlib import Path

sys.path.insert(0, "/prefect")

from typing import Any

import pandas as pd
from sqlalchemy import text

from prefect import task

from config.settings import settings
from utils.database import db_manager


@task(
    name="create_correlation_table",
    log_prints=True,
    tags=["gold", "database", "correlation"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def create_correlation_table() -> str:
    """
    Create gold.weather_crash_correlation table if not exists.

    Returns:
        Table name
    """
    table_name = "weather_crash_correlation"
    schema_name = "gold"

    # Step 1: Create Gold schema if not exists
    create_schema_sql = text("CREATE SCHEMA IF NOT EXISTS gold")

    # Step 2: Create correlation table with weather categories
    create_table_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            weather_name TEXT NOT NULL,
            total_crashes INTEGER NOT NULL,
            total_days INTEGER NOT NULL,
            avg_crashes_per_day DECIMAL(5, 2) NOT NULL,
            avg_temp_max DECIMAL(5, 2),
            avg_temp_min DECIMAL(5, 2),
            avg_precipitation DECIMAL(6, 2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(weather_name)
        )
    """)

    # Step 3: Create index for performance
    create_index_sql = text(f"""
        CREATE INDEX IF NOT EXISTS idx_gold_correlation_weather
            ON {schema_name}.{table_name} (weather_name)
    """)

    # Step 4: Execute all DDL statements
    with db_manager.get_connection() as connection:
        connection.execute(create_schema_sql)
        connection.execute(create_table_sql)
        connection.execute(create_index_sql)

    print(f"Created/verified table {schema_name}.{table_name}")
    return table_name


@task(
    name="calculate_weather_crash_correlation",
    log_prints=True,
    tags=["gold", "database", "correlation"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def calculate_weather_crash_correlation() -> int:
    """
    Calculate weather-crash correlation and populate Gold table.

    Aggregates crash rates by weather category (weathername) across all states and years.

    Returns:
        Number of records upserted
    """
    table_name = "weather_crash_correlation"
    schema_name = "gold"

    # Query to aggregate crash data by weathername with weather metrics
    query = text("""
        SELECT
            dc.weathername as weather_name,
            COUNT(DISTINCT dc.st_case) as total_crashes,
            COUNT(DISTINCT dw.date) as total_days,
            ROUND(COUNT(DISTINCT dc.st_case) * 1.0 / COUNT(DISTINCT dw.date), 2) as avg_crashes_per_day,
            AVG(dw.temperature_2m_max) as avg_temp_max,
            AVG(dw.temperature_2m_min) as avg_temp_min,
            AVG(dw.precipitation_sum) as avg_precipitation
        FROM silver.parsed_crashes_array dc
        JOIN silver.daily_weather dw
            ON dc.year = dw.year
            AND dc.month = dw.month
            AND dc.day = dw.day
            AND dc.state_name = dw.location_name
        WHERE dc.weathername IS NOT NULL
        GROUP BY dc.weathername
        ORDER BY avg_crashes_per_day DESC
    """)

    with db_manager.get_connection() as connection:
        result = connection.execute(query)
        rows = result.fetchall()
        columns = result.keys()

    if not rows:
        print("No data to calculate correlation")
        return 0

    df = pd.DataFrame(rows, columns=columns)

    # Upsert into Gold table
    upsert_sql = text(f"""
        INSERT INTO {schema_name}.{table_name}
            (weather_name, total_crashes, total_days, avg_crashes_per_day,
             avg_temp_max, avg_temp_min, avg_precipitation)
        VALUES
            (:weather_name, :total_crashes, :total_days, :avg_crashes_per_day,
             :avg_temp_max, :avg_temp_min, :avg_precipitation)
        ON CONFLICT (weather_name) DO UPDATE SET
            total_crashes = EXCLUDED.total_crashes,
            total_days = EXCLUDED.total_days,
            avg_crashes_per_day = EXCLUDED.avg_crashes_per_day,
            avg_temp_max = EXCLUDED.avg_temp_max,
            avg_temp_min = EXCLUDED.avg_temp_min,
            avg_precipitation = EXCLUDED.avg_precipitation,
            created_at = CURRENT_TIMESTAMP
    """)

    records = df.to_dict(orient="records")

    with db_manager.get_connection() as connection:
        connection.execute(upsert_sql, records)

    print(f"Upserted {len(records)} weather categories to {schema_name}.{table_name}")

    # Print summary for quick insight
    print("\n=== Weather-Crash Correlation Summary ===")
    for _, row in df.iterrows():
        print(
            f"{row['weather_name']}: {row['avg_crashes_per_day']} crashes/day ({row['total_crashes']} total crashes in {row['total_days']} days)"
        )

    return len(records)


@task(
    name="get_correlation_summary",
    log_prints=True,
    tags=["gold", "database", "correlation"],
)
def get_correlation_summary() -> pd.DataFrame:
    """
    Get the weather-crash correlation summary from Gold table.

    Args:
        (None)
    Returns:
        DataFrame with correlation data
    """
    table_name = "weather_crash_correlation"
    schema_name = "gold"

    query = text(f"""
        SELECT *
        FROM {schema_name}.{table_name}
        ORDER BY avg_crashes_per_day DESC
    """)

    with db_manager.get_connection() as connection:
        result = connection.execute(query)
        rows = result.fetchall()
        columns = result.keys()

    df = pd.DataFrame(rows, columns=columns)
    print(f"Retrieved {len(df)} weather categories from Gold table")
    return df
