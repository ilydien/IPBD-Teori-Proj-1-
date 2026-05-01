"""Flow for extracting crashes from API to Bronze layer."""

import sys
from pathlib import Path

sys.path.insert(0, "/prefect")

from prefect import flow
from tasks.crash.bronze.extract_from_fars_api.extract import fetch_crash_incidents
from tasks.crash.bronze.extract_from_fars_api.transform import unwrap_api_response
from tasks.crash.bronze.extract_from_fars_api.load import insert_raw_to_database


def execute_tasks(state_code: int, year: int, format_data: str = "json") -> int:
    # Step 1: Ensure Bronze table exists
    from tasks.crash.bronze.create_bronze import create_bronze_table
    create_bronze_table()

    # Step 2: Check if data already exists in bronze layer
    from utils.helpers import check_data_exists

    if check_data_exists(
        schema_name="bronze",
        table_name="fars_crashes",
        year=year,
        state_code=state_code,
    ):
        print(f"Skipping: Data already exists for state {state_code}, year {year}")
        return 0

    # Step 3: Fetch crash data from FARS API
    api_response = fetch_crash_incidents(
        stateCode=state_code,
        year=year,
        formatData=format_data,
    )

    # Step 4: Unwrap API response to tabular format
    params = {
        "year": year,
        "state": state_code,
    }

    tabular_data = unwrap_api_response(
        api_response=api_response,
        params=params,
    )

    # Step 5: Insert tabular data to Bronze layer
    inserted_count = insert_raw_to_database(
        tabular_data=tabular_data,
        schema_name="bronze",
        table_name="fars_crashes",
    )

    return inserted_count


@flow(name="extract_crash_data_to_bronze_range", log_prints=True)
def extract_crash_data_to_bronze_range(
    state_code: int, start_year: int, end_year: int, format_data: str = "json"
) -> int:
    """
    Extract crash data from FARS API and load to Bronze layer.
    """
    total_inserted_count = 0
    for year in range(start_year, end_year + 1):
        inserted_count = execute_tasks(state_code, year, format_data)
        total_inserted_count += inserted_count

    return total_inserted_count


@flow(name="extract_crash_data_to_bronze", log_prints=True)
def extract_crash_data_to_bronze(
    state_code: int, year: int, format_data: str = "json"
) -> int:
    """
    Extract crash data from FARS API and load to Bronze layer.
    """
    inserted_count = execute_tasks(state_code, year, format_data)

    return inserted_count
