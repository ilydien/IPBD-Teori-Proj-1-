"""Flow for extracting crashes from API to Bronze layer."""

from prefect import flow
from tasks.crashes.extract_from_api.extract import fetch_crash_incidents
from tasks.crashes.extract_from_api.transform import unwrap_api_response
from tasks.crashes.extract_from_api.load import insert_raw_to_database


@flow(name="extract_crash_data_to_bronze", log_prints=True)
def extract_crash_data_to_bronze(
    state_code: int, start_year: int, end_year: int, format_data: str = "json"
) -> int:
    """
    Extract crash data from FARS API and load to Bronze layer.
    """
    api_response = fetch_crash_incidents(
        stateCode=state_code,
        startYear=start_year,
        endYear=end_year,
        formatData=format_data,
    )

    params = {
        "FromYear": start_year,
        "ToYear": end_year,
        "state": state_code,
    }

    tabular_data = unwrap_api_response(
        api_response=api_response,
        params=params,
    )

    inserted_count = insert_raw_to_database(
        tabular_data=tabular_data,
        schema_name="bronze",
        table_name="fars_crashes",
    )

    return inserted_count
