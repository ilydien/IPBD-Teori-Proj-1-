"""Flow for transforming Bronze crash data to Silver layer."""

from prefect import flow

from tasks.crashes.crashes_json_to_tabular.extract import select_from_bronze_crashes
from tasks.crashes.crashes_json_to_tabular.transform import flatten_crashes_json
from tasks.crashes.crashes_json_to_tabular.load import upsert_to_silver
from config.database import db_manager


def execute_tasks(
    state_code: int | None = None,
    year: int | None = None,
) -> int:
    bronze_df = select_from_bronze_crashes(
        year=year,
        state_code=state_code,
    )

    flattened_df = flatten_crashes_json(bronze_df)

    upserted_count = upsert_to_silver(flattened_df)
    return upserted_count


@flow(name="transform_crashes_to_silver", log_prints=True)
def transform_crashes_to_silver(
    state_code: int | None = None,
    year: int | None = None,
) -> int:
    """
    Transform crash data from Bronze to Silver layer.
    """
    upserted_count = execute_tasks(state_code, year)

    return upserted_count


@flow(name="transform_crashes_to_silver_range", log_prints=True)
def transform_crashes_to_silver_range(
    state_code: int | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> int:
    """
    Transform crash data from Bronze to Silver layer.
    """
    total_upserted_count = 0

    for year in range(start_year, end_year + 1):
        upserted_count = execute_tasks(state_code, year)
        total_upserted_count += upserted_count

    return total_upserted_count
