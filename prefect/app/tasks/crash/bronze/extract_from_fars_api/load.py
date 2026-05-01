from typing import Any
from prefect import task

import json
import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB

from config.settings import settings
from utils.database import db_manager


@task(
    name="insert_raw_to_database",
    log_prints=True,
    tags=["load", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def insert_raw_to_database(
    tabular_data: pd.DataFrame,
    schema_name: str = "bronze",
    table_name: str = "fars_crashes",
) -> int:
    """
    Hybrid approach: Try Prefect Block first, fallback to .env.
    """
    dtype = {"results": JSONB}

    # Step 1: Try Prefect Block first, fallback to .env
    try:
        from prefect_sqlalchemy import SqlAlchemyConnector

        database_block = SqlAlchemyConnector.load(settings.PREFECT_BLOCK_NAME)

        # Step 2: Insert each row using Prefect block connection
        with database_block.get_connection(begin=True) as connection:
            for idx, row in tabular_data.iterrows():
                upsert_sql = text(
                    """
                    INSERT INTO {schema}.{table} (year, state_code, state_name, count, results)
                    VALUES (:year, :state_code, :state_name, :count, :results)
                    ON CONFLICT (year, state_code) DO UPDATE SET
                        state_name = EXCLUDED.state_name,
                        count = EXCLUDED.count,
                        results = EXCLUDED.results,
                        created_at = CURRENT_TIMESTAMP
                """.format(schema=schema_name, table=table_name)
                )

                results_val = row["results"]
                if isinstance(results_val, str):
                    results_val = results_val
                else:
                    results_val = (
                        json.dumps(row["results"])
                        if not isinstance(row["results"], str)
                        else row["results"]
                    )

                connection.execute(
                    upsert_sql,
                    {
                        "year": int(row["year"]),
                        "state_code": int(row["state_code"]),
                        "state_name": row["state_name"],
                        "count": int(row["count"]),
                        "results": results_val,
                    },
                )

        print("Used Prefect block (production mode)")

    except (ImportError, ValueError, KeyError) as e:
        print(f"Prefect block not found ({str(e)}), falling back to .env")

        if not settings.validate_db_settings():
            raise ValueError("Neither Prefect block nor .env configuration found")

        # Step 3: Fallback to .env configuration
        with db_manager.get_connection() as connection:
            for idx, row in tabular_data.iterrows():
                upsert_sql = text(
                    """
                    INSERT INTO {schema}.{table} (year, state_code, state_name, count, results)
                    VALUES (:year, :state_code, :state_name, :count, :results)
                    ON CONFLICT (year, state_code) DO UPDATE SET
                        state_name = EXCLUDED.state_name,
                        count = EXCLUDED.count,
                        results = EXCLUDED.results,
                        created_at = CURRENT_TIMESTAMP
                """.format(schema=schema_name, table=table_name)
                )

                results_val = row["results"]
                if isinstance(results_val, str):
                    results_val = results_val
                else:
                    results_val = json.dumps(results_val)

                connection.execute(
                    upsert_sql,
                    {
                        "year": int(row["year"]),
                        "state_code": int(row["state_code"]),
                        "state_name": row["state_name"],
                        "count": int(row["count"]),
                        "results": results_val,
                    },
                )
        print("Used .env configuration (local mode)")

    # Step 4: Log total count and return number of inserted rows
    total_count = db_manager.get_table_count(schema_name, table_name)
    print(f"Total rows in table: {total_count}")

    return len(tabular_data)
