from typing import Any
from prefect import task

import pandas as pd
from sqlalchemy.dialects.postgresql import JSONB

from settings import settings
from config.database import db_manager


@task(
    name="insert_raw_to_database",
    log_prints=True,
    tags=["load", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
async def insert_raw_to_database(
    tabular_data: pd.DataFrame,
    schema_name: str = "bronze",
    table_name: str = "fars_crashes",
) -> int:
    """
    Hybrid approach: Try Prefect Block first, fallback to .env.
    """
    dtype = {"results": JSONB}
    db_manager.create_bronze_fars_crashes_table()
    try:
        from prefect_sqlalchemy import SqlAlchemyConnector

        database_block = SqlAlchemyConnector.load(settings.PREFECT_BLOCK_NAME)

        with database_block.get_connection(begin=True) as connection:
            _ = tabular_data.to_sql(
                name=table_name,
                schema=schema_name,
                con=connection.engine,
                if_exists="append",
                index=False,
                dtype=dtype,
            )
        print("Used Prefect block (production mode)")

    except (ImportError, ValueError, KeyError) as e:
        print(f"Prefect block not found ({str(e)}), falling back to .env")

        if not settings.validate_db_settings():
            raise ValueError("Neither Prefect block nor .env configuration found")

        with db_manager.get_connection() as connection:
            _ = tabular_data.to_sql(
                name=table_name,
                schema=schema_name,
                con=connection,
                if_exists="append",
                index=False,
                dtype=dtype,
            )
        print("Used .env configuration (local mode)")

    total_count = db_manager.get_table_count(schema_name, table_name)
    print(f"Total rows in table: {total_count}")

    return len(tabular_data)
