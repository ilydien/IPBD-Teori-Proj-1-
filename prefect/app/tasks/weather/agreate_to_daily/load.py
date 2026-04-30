"""Load tasks for weather daily aggregation (query mapping)."""

import sys
from pathlib import Path
sys.path.insert(0, "/prefect")

from typing import Any

import pandas as pd
from sqlalchemy import text

from prefect import task
from prefect.tasks import task_input_hash

from settings import settings
from config.database import db_manager


@task(
    name="aggregate_weather_to_daily",
    log_prints=True,
    tags=["load", "database", "weather"],
)
def aggregate_weather_to_daily(
    weather_df: pd.DataFrame,
    schema_name: str = "silver",
    table_name: str = "daily_weather",
) -> pd.DataFrame:
    """
    Aggregate weather data to daily level via query mapping.

    Since the data is already daily, this performs additional aggregation
    by location_name and year/month/day (e.g., average temperatures, sum precipitation).

    Args:
        weather_df: DataFrame from Silver layer
        schema_name: Schema name (default: silver)
        table_name: Table name (default: daily_weather)

    Returns:
        Aggregated DataFrame
    """
    # Step1: Return empty DataFrame if input is empty
    if weather_df.empty:
        print("No data to aggregate")
        return pd.DataFrame(
            columns=[
                "location_name",
                "year",
                "month",
                "day",
                "avg_temperature_2m_max",
                "avg_temperature_2m_min",
                "total_precipitation_sum",
                "record_count",
            ]
        )
    
    # Step2: Validate required columns exist
    required_cols = ["location_name", "year", "month", "day"]
    for col in required_cols:
        if col not in weather_df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Step3: Group by location and date, aggregate metrics
    aggregated = (
        weather_df.groupby(["location_name", "year", "month", "day"])
        .agg(
            avg_temperature_2m_max=("temperature_2m_max", "mean"),
            avg_temperature_2m_min=("temperature_2m_min", "mean"),
            total_precipitation_sum=("precipitation_sum", "sum"),
            record_count=("temperature_2m_max", "count"),
        )
        .reset_index()
    )
    
    # Step4: Return aggregated DataFrame
    print(
        f"Aggregated {len(weather_df)} records to {len(aggregated)} daily weather records"
    )
    return aggregated


@task(
    name="query_daily_weather_summary",
    log_prints=True,
    tags=["load", "database", "weather"],
)
def query_daily_weather_summary(
    schema_name: str = "silver",
    table_name: str = "daily_weather",
    year: int | None = None,
    location_name: str | None = None,
) -> pd.DataFrame:
    """
    Query daily weather summary directly from database.

    Performs aggregation via SQL GROUP BY for efficiency.

    Args:
        schema_name: Schema name (default: silver)
        table_name: Table name (default: daily_weather)
        year: Filter by year (optional)
        location_name: Filter by location (optional)

    Returns:
        Aggregated DataFrame
    """
    # Step1: Build SELECT query with aggregation
    query = text(f"""
        SELECT
            location_name,
            year,
            month,
            day,
            AVG(temperature_2m_max) as avg_temperature_2m_max,
            AVG(temperature_2m_min) as avg_temperature_2m_min,
            SUM(precipitation_sum) as total_precipitation_sum,
            COUNT(*) as record_count
        FROM {schema_name}.{table_name}
    """)

    params = {}
    conditions = []
    # Step2: Add WHERE conditions if filters exist
    if year is not None:
        conditions.append("year = :year")
        params["year"] = year
    if location_name is not None:
        conditions.append("location_name = :location_name")
        params["location_name"] = location_name

    if conditions:
        query = text(f"{query} WHERE " + " AND ".join(conditions))

    # Step3: Add GROUP BY and ORDER BY clauses
    query = text(f"{query} GROUP BY location_name, year, month, day ORDER BY year, month, day")

    # Step4: Execute query and fetch results
    with db_manager.get_connection() as connection:
        result = connection.execute(query, params)
        rows = result.fetchall()
        columns = result.keys()

    # Step5: Convert results to DataFrame
    df = pd.DataFrame(rows, columns=columns)
    print(f"Queried {len(df)} aggregated daily weather records")
    return df