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
    column_mapping = {
        "state": "state_code",
        "statename": "state_name",
    }
    valid_columns = {
        "st_case",
        "caseyear",
        "state_code",
        "state_name",
        "county",
        "city",
        "day",
        "month",
        "year",
        "hour",
        "minute",
        "day_week",
        "day_weekname",
        "func_sys",
        "func_sysname",
        "harm_ev",
        "harm_evname",
        "hosp_hr",
        "hosp_hrname",
        "hosp_mn",
        "hosp_mnname",
        "route",
        "routename",
        "sp_jur",
        "sp_jurname",
        "arr_min",
        "arr_minname",
        "rur_urb",
        "rur_urbname",
        "typ_int",
        "typ_intname",
        "weather",
        "weathername",
        "lgt_cond",
        "lgt_condname",
        "fatals",
        "permvit",
        "pernotmvit",
        "ve_total",
        "ve_forms",
        "persons",
        "pvh_invl",
        "latitude",
        "longitud",
        "milept",
        "mileptname",
        "tway_id",
        "tway_id2",
        "rd_owner",
        "rd_ownername",
        "rel_road",
        "rel_roadname",
        "reljct1",
        "reljct1name",
        "reljct2",
        "reljct2name",
        "wrk_zone",
        "wrk_zonename",
        "man_coll",
        "man_collname",
        "not_hour",
        "not_hourname",
        "not_min",
        "not_minname",
        "arr_hour",
        "arr_hourname",
        "sch_bus",
        "sch_busname",
        "road_fnc",
        "road_fncname",
        "cityname",
        "countyname",
        "hourname",
        "monthname",
        "minutename",
        "weather1",
        "weather1name",
        "weather2",
        "weather2name",
        "drunk_dr",
        "nhs",
        "nhsname",
        "cf1",
        "cf1name",
        "cf2",
        "cf2name",
        "cf3",
        "cf3name",
        "peds",
        "rail",
        "railname",
        "dayname",
    }
    filtered_cols = []
    for col in df.columns:
        if col in column_mapping:
            filtered_cols.append(column_mapping[col])
        elif col in valid_columns:
            filtered_cols.append(col)
    return filtered_cols


@task(
    name="upsert_to_silver",
    log_prints=True,
    tags=["load", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def upsert_to_silver(tabular_data: pd.DataFrame) -> int:
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

    column_mapping = {
        "state": "state_code",
        "statename": "state_name",
    }
    valid_columns = {
        "st_case",
        "caseyear",
        "state_code",
        "state_name",
        "county",
        "city",
        "day",
        "month",
        "year",
        "hour",
        "minute",
        "day_week",
        "day_weekname",
        "func_sys",
        "func_sysname",
        "harm_ev",
        "harm_evname",
        "hosp_hr",
        "hosp_hrname",
        "hosp_mn",
        "hosp_mnname",
        "route",
        "routename",
        "sp_jur",
        "sp_jurname",
        "arr_min",
        "arr_minname",
        "rur_urb",
        "rur_urbname",
        "typ_int",
        "typ_intname",
        "weather",
        "weathername",
        "lgt_cond",
        "lgt_condname",
        "fatals",
        "permvit",
        "pernotmvit",
        "ve_total",
        "ve_forms",
        "persons",
        "pvh_invl",
        "latitude",
        "longitud",
        "milept",
        "mileptname",
        "tway_id",
        "tway_id2",
        "rd_owner",
        "rd_ownername",
        "rel_road",
        "rel_roadname",
        "reljct1",
        "reljct1name",
        "reljct2",
        "reljct2name",
        "wrk_zone",
        "wrk_zonename",
        "man_coll",
        "man_collname",
        "not_hour",
        "not_hourname",
        "not_min",
        "not_minname",
        "arr_hour",
        "arr_hourname",
        "sch_bus",
        "sch_busname",
        "road_fnc",
        "road_fncname",
        "cityname",
        "countyname",
        "hourname",
        "monthname",
        "minutename",
        "weather1",
        "weather1name",
        "weather2",
        "weather2name",
        "drunk_dr",
        "nhs",
        "nhsname",
        "cf1",
        "cf1name",
        "cf2",
        "cf2name",
        "cf3",
        "cf3name",
        "peds",
        "rail",
        "railname",
        "dayname",
    }

    for old_col, new_col in column_mapping.items():
        if old_col in tabular_data.columns and new_col not in tabular_data.columns:
            tabular_data.rename(columns={old_col: new_col}, inplace=True)

    columns = [col for col in tabular_data.columns if col in valid_columns]

    columns_sql = ", ".join(columns)
    placeholders = ", ".join([f":{col}" for col in columns])
    updates = ", ".join(
        [
            f"{col} = EXCLUDED.{col}"
            for col in columns
            if col not in ("id", "created_at")
        ]
    )

    tabular_data = tabular_data[columns]

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
