"""Load tasks for inserting flattened crash data to Silver layer."""

from typing import Any

import pandas as pd
from sqlalchemy import text

from prefect import task
from prefect.tasks import task_input_hash

from settings import settings
from config.database import db_manager


def get_columns_sql(df: pd.DataFrame) -> str:
    """Generate column definitions for CREATE TABLE."""
    columns = ["id SERIAL PRIMARY KEY"]
    integer_cols = {
        "st_case",
        "state_code",
        "year",
        "caseyear",
        "count",
        "COUNTY",
        "CITY",
        "HOUR",
        "MINUTE",
        "FATALS",
        "PERMVIT",
        "PERNOTMVIT",
        "VE_TOTAL",
        "VE_FORMS",
        "DAY",
        "MONTH",
        "NHS",
        "PEDS",
        "CF1",
        "CF2",
        "CF3",
        "DRUNK_DR",
        "FUNC_SYS",
        "HARM_EV",
        "ROUTE",
        "SP_JUR",
        "ARR_HOUR",
        "ARR_MIN",
        "RUR_URB",
        "TYP_INT",
        "WEATHER",
        "LGT_COND",
        "PERSONS",
        "PVH_INVL",
        "RD_OWNER",
        "REL_ROAD",
        "RELJCT1",
        "RELJCT2",
        "WRK_ZONE",
        "MAN_COLL",
        "NOT_HOUR",
        "NOT_MIN",
        "DAY_WEEK",
    }
    numeric_cols = {
        "LATITUDE",
        "LONGITUD",
        "latitude",
        "longitude",
        "MILEPT",
        "LATITUDENAME",
        "LONGITUDENAME",
    }
    for col in df.columns:
        col_upper = col.upper()
        if col in integer_cols or col_upper in integer_cols:
            columns.append(f"{col} INTEGER")
        elif col in numeric_cols or col_upper in numeric_cols:
            columns.append(f"{col} NUMERIC")
        else:
            columns.append(f"{col} TEXT")
    return ", ".join(columns)


def get_columns_list(df: pd.DataFrame) -> list[str]:
    """Get list of column names for INSERT."""
    return [col for col in df.columns if col != "id"]


@task(
    name="upsert_to_silver",
    log_prints=True,
    tags=["load", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
async def upsert_to_silver(tabular_data: pd.DataFrame) -> int:
    """
    Upsert flattened crash data to Silver layer with deduplication.

    Uses ON CONFLICT DO UPDATE for deduplication based on (st_case, caseyear, state).

    Args:
        tabular_data: DataFrame with flattened crash data

    Returns:
        Number of records upserted
    """
    table_name = "parsed_crashes_array"
    schema_name = "silver"

    columns = get_columns_list(tabular_data)

    columns_sql = ", ".join(columns)
    placeholders = ", ".join([f":{col}" for col in columns])
    updates = ", ".join(
        [
            f"{col} = EXCLUDED.{col}"
            for col in columns
            if col not in ("id", "created_at")
        ]
    )

    upsert_sql = text(f"""
        INSERT INTO {schema_name}.{table_name} ({columns_sql})
        VALUES ({placeholders})
        ON CONFLICT (st_case, caseyear, state_code) DO UPDATE SET
        {updates}
    """)

    records = (
        tabular_data.fillna(value=None).replace({pd.NA: None}).to_dict(orient="records")
    )

    with db_manager.get_connection() as connection:
        connection.execute(upsert_sql, records)

    print(f"Upserted {len(tabular_data)} records to silver.{table_name}")
    return len(tabular_data)
