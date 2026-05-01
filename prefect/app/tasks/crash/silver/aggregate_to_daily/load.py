"""Load tasks for inserting daily crash data to Silver layer."""

from typing import Any

import pandas as pd
from sqlalchemy import text

from prefect import task
from prefect.tasks import task_input_hash

from config.settings import settings
from utils.database import db_manager


@task(
    name="create_daily_crashes_table",
    log_prints=True,
    tags=["load", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def create_daily_crashes_table() -> str:
    """
    Create silver.daily_crashes table if not exists.

    Returns:
        Table name
    """
    table_name = "daily_crashes"
    schema_name = "silver"

    # Step 1: Create Silver schema if not exists
    create_schema_sql = text("CREATE SCHEMA IF NOT EXISTS silver")

    # Step 2: Create daily_crashes table with unique constraint
    create_table_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL,
            state_name TEXT NOT NULL,
            total_crashes INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, month, day, state_name)
        )
    """)

    # Step 3: Execute DDL statements
    with db_manager.get_connection() as connection:
        connection.execute(create_schema_sql)
        connection.execute(create_table_sql)

    print(f"Created/verified table silver.{table_name}")
    return table_name


@task(
    name="upsert_to_daily_crashes",
    log_prints=True,
    tags=["load", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
async def upsert_to_daily_crashes(daily_data: pd.DataFrame) -> int:
    """
    Upsert daily crash data to Silver layer with deduplication.

    Uses ON CONFLICT DO UPDATE for deduplication based on (year, month, day, state_name).

    Args:
        daily_data: DataFrame with aggregated daily crash data

    Returns:
        Number of records upserted
    """
    table_name = "daily_crashes"
    schema_name = "silver"

    # Step1: Define columns for INSERT
    columns = ["year", "month", "day", "state_name", "total_crashes"]
    columns_sql = ", ".join(columns)
    placeholders = ", ".join([f":{col}" for col in columns])
    updates = "total_crashes = EXCLUDED.total_crashes"

    # Step2: Build upsert SQL query
    upsert_sql = text(f"""
        INSERT INTO {schema_name}.{table_name} ({columns_sql})
        VALUES ({placeholders})
        ON CONFLICT (year, month, day, state_name) DO UPDATE SET
        {updates}
    """)

    # Step3: Convert DataFrame to records
    records = (
        daily_data.fillna(value=None).replace({pd.NA: None}).to_dict(orient="records")
    )

    # Step4: Execute upsert
    with db_manager.get_connection() as connection:
        connection.execute(upsert_sql, records)

    # Step5: Log result and return count
    print(f"Upserted {len(daily_data)} records to silver.{table_name}")
    return len(daily_data)

