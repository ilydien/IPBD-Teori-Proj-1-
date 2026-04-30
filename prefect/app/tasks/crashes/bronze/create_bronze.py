"""Tasks for creating Bronze layer tables."""

import sys
from pathlib import Path

sys.path.insert(0, "/prefect")

from sqlalchemy import text

from prefect import task

from settings import settings
from config.database import db_manager


@task(
    name="create_bronze_table",
    log_prints=True,
    tags=["bronze", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def create_bronze_table() -> str:
    """
    Create bronze.fars_crashes table if not exists.
    Includes schema creation.

    Returns:
        Table name
    """
    table_name = "fars_crashes"
    schema_name = "bronze"

    create_schema_sql = text("CREATE SCHEMA IF NOT EXISTS bronze")

    create_table_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL,
            state_code INTEGER NOT NULL,
            state_name TEXT,
            count INTEGER NOT NULL,
            message TEXT,
            results JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, state_code)
        )
    """)

    create_index_sql = text(f"""
        CREATE INDEX IF NOT EXISTS idx_bronze_fars_crashes_state_year
            ON {schema_name}.{table_name} (state_code, year)
    """)

    with db_manager.get_connection() as connection:
        connection.execute(create_schema_sql)
        connection.execute(create_table_sql)
        connection.execute(create_index_sql)

    print(f"Created/verified table {schema_name}.{table_name}")
    return table_name
