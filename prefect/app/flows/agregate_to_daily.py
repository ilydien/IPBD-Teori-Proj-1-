"""Flow for aggregating crash data to daily level."""

from prefect import flow

from tasks.crashes.agregate_to_daily.extract import select_from_silver_crashes
from tasks.crashes.agregate_to_daily.transform import agregate_to_daily
from tasks.crashes.agregate_to_daily.load import (
    upsert_to_daily_crashes,
)


def execute_tasks(
    state_code: int | None = None,
    year: int | None = None,
) -> int:
    # Step 1: Ensure daily crashes table exists
    from tasks.crashes.agregate_to_daily.load import create_daily_crashes_table

    create_daily_crashes_table()

    # Step 2: Select crash data from Silver layer
    crashes_df = select_from_silver_crashes(year=year, state_code=state_code)

    # Step 3: Aggregate crash data to daily level
    daily_df = agregate_to_daily(crashes_df)

    # Step 4: Upsert aggregated data to daily crashes table
    upserted_count = upsert_to_daily_crashes(daily_df)

    return upserted_count


@flow(name="agregate_crashes_to_daily", log_prints=True)
def agregate_crashes_to_daily(
    state_code: int | None = None,
    year: int | None = None,
) -> int:
    """
    Aggregate crash data to daily level.

    Args:
        year: Filter by year (optional)
        state_code: Filter by state (optional)

    Returns:
        Number of records upserted
    """
    upserted_count = execute_tasks(state_code, year)

    return upserted_count


@flow(name="agregate_crashes_to_daily_range", log_prints=True)
def agregate_crashes_to_daily_range(
    state_code: int | list[int] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> int:
    """
    Aggregate crash data to daily level.

    Args:
        start_year: Filter by year (optional)
        end_year: Filter by year (optional)
        state_code: Filter by state (optional)

    Returns:
        Number of records upserted
    """
    total_upserted_count = 0
    for year in range(start_year, end_year + 1):
        total_upserted_count += execute_tasks(state_code, year)

        return total_upserted_count
