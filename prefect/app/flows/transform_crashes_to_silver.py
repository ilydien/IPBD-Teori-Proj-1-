"""Flow for transforming Bronze crash data to Silver layer."""

from prefect import flow

from tasks.crashes.crashes_json_to_tabular.extract import select_from_bronze_crashes
from tasks.crashes.crashes_json_to_tabular.transform import flatten_crashes_json
from tasks.crashes.crashes_json_to_tabular.load import upsert_to_silver
from config.database import db_manager
from sqlalchemy import text


@flow(name="transform_crashes_to_silver", log_prints=True)
def transform_crashes_to_silver(
    start_year: int | None = None,
    end_year: int | None = None,
    state_code: int | list[int] | None = None,
) -> int:
    """
    Transform crash data from Bronze to Silver layer.
    """
    # Create table from schema file

    print("Created silver.parsed_crashes_array table")

    bronze_df = select_from_bronze_crashes(
        start_year=start_year,
        end_year=end_year,
        state_code=state_code,
    )

    flattened_df = flatten_crashes_json(bronze_df)

    upserted_count = upsert_to_silver(flattened_df)

    return upserted_count
