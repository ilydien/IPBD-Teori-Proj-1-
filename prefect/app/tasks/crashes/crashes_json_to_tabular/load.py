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


@task(
    name="create_silver_parsed_crashes_table",
    log_prints=True,
    tags=["silver", "database"],
    retries=2,
    retry_delay_seconds=[5, 10],
)
def create_silver_parsed_crashes_table() -> str:
    """
    Create silver.parsed_crashes_array table if not exists.
    
    Args:
        (None)
    Returns:
        Table name
    """
    table_name = "parsed_crashes_array"
    schema_name = "silver"

    # Step 1: Create Silver schema if not exists
    create_schema_sql = text("CREATE SCHEMA IF NOT EXISTS silver")

    # Step 2: Create Silver table with all crash fields
    create_table_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            st_case INTEGER,
            caseyear INTEGER,
            state_code INTEGER,
            state_name TEXT,
            county INTEGER,
            city INTEGER,
            day INTEGER,
            month INTEGER,
            year INTEGER,
            hour INTEGER,
            minute INTEGER,
            day_week INTEGER,
            day_weekname TEXT,
            func_sys INTEGER,
            func_sysname TEXT,
            harm_ev INTEGER,
            harm_evname TEXT,
            hosp_hr INTEGER,
            hosp_hrname TEXT,
            hosp_mn INTEGER,
            hosp_mnname TEXT,
            route INTEGER,
            routename TEXT,
            sp_jur INTEGER,
            sp_jurname TEXT,
            arr_min INTEGER,
            arr_minname TEXT,
            rur_urb INTEGER,
            rur_urbname TEXT,
            typ_int INTEGER,
            typ_intname TEXT,
            weather INTEGER,
            weathername TEXT,
            lgt_cond INTEGER,
            lgt_condname TEXT,
            fatals INTEGER,
            permvit INTEGER,
            pernotmvit INTEGER,
            ve_total INTEGER,
            ve_forms INTEGER,
            persons INTEGER,
            pvh_invl INTEGER,
            latitude NUMERIC,
            longitud NUMERIC,
            milept NUMERIC,
            mileptname TEXT,
            tway_id TEXT,
            tway_id2 TEXT,
            rd_owner INTEGER,
            rd_ownername TEXT,
            rel_road INTEGER,
            rel_roadname TEXT,
            reljct1 INTEGER,
            reljct1name TEXT,
            reljct2 INTEGER,
            reljct2name TEXT,
            wrk_zone INTEGER,
            wrk_zonename TEXT,
            man_coll INTEGER,
            man_collname TEXT,
            not_hour INTEGER,
            not_hourname TEXT,
            not_min INTEGER,
            not_minname TEXT,
            arr_hour INTEGER,
            arr_hourname TEXT,
            sch_bus INTEGER,
            sch_busname TEXT,
            road_fnc INTEGER,
            road_fncname TEXT,
            cityname TEXT,
            countyname TEXT,
            hourname TEXT,
            monthname TEXT,
            minutename TEXT,
            weather1 INTEGER,
            weather1name TEXT,
            weather2 INTEGER,
            weather2name TEXT,
            drunk_dr INTEGER,
            nhs INTEGER,
            nhsname TEXT,
            cf1 INTEGER,
            cf1name TEXT,
            cf2 INTEGER,
            cf2name TEXT,
            cf3 INTEGER,
            cf3name TEXT,
            peds INTEGER,
            rail TEXT,
            railname TEXT,
            dayname INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(st_case, caseyear, state_code)
        )
    """)

    # Step 3: Create indexes for performance
    create_index_sql = text(f"""
        CREATE INDEX IF NOT EXISTS idx_silver_parsed_crashes_state_year
            ON {schema_name}.{table_name} (state_code, caseyear);
        CREATE INDEX IF NOT EXISTS idx_silver_parsed_crashes_date
            ON {schema_name}.{table_name} (year, month, day);
    """)

    # Step 4: Execute all DDL statements
    with db_manager.get_connection() as connection:
        connection.execute(create_schema_sql)
        connection.execute(create_table_sql)
        connection.execute(create_index_sql)

    print(f"Created/verified table {schema_name}.{table_name}")
    return table_name


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

    # Step 1: Define column mapping and valid columns
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

    # Step 2: Rename columns if needed
    for old_col, new_col in column_mapping.items():
        if old_col in tabular_data.columns and new_col not in tabular_data.columns:
            tabular_data.rename(columns={old_col: new_col}, inplace=True)

    # Step 3: Filter to valid columns only
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

    # Step 4: Build upsert SQL query
    upsert_sql = text(f"""
        INSERT INTO {schema_name}.{table_name} ({columns_sql})
        VALUES ({placeholders})
        ON CONFLICT (st_case, caseyear, state_code) DO UPDATE SET
        {updates}
    """)

    # Step 5: Convert DataFrame to records and execute upsert
    records = (
        tabular_data.fillna(value=None).replace({pd.NA: None}).to_dict(orient="records")
    )

    with db_manager.get_connection() as connection:
        connection.execute(upsert_sql, records)

    # Step 6: Log result and return count
    print(f"Upserted {len(tabular_data)} records to silver.{table_name}")
    return len(tabular_data)
