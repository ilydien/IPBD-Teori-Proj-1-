"""Flow for building Gold layer with crash and weather data joins."""

import sys
from pathlib import Path

sys.path.insert(0, "/prefect")

from prefect import flow

from tasks.crashes.gold.create_gold import create_gold_table, populate_gold_table, get_gold_count


@flow(name="build_gold_layer", log_prints=True)
def build_gold_layer(
    year: int | None = None,
    state_name: str | None = None,
) -> dict:
    """
    Build Gold layer by joining Silver crash and weather data.

    Args:
        year: Filter by year (optional)
        state_name: Filter by state (optional)

    Returns:
        Dict with result summary
    """
    # Step 1: Create Gold table
    create_gold_table()

    # Step 2: Populate Gold table
    upserted = populate_gold_table(
        year=year,
        state_name=state_name,
    )

    # Step 3: Get total count
    total_count = get_gold_count()

    return {
        "upserted": upserted,
        "total_records": total_count,
    }


@flow(name="build_gold_layer_full", log_prints=True)
def build_gold_layer_full() -> dict:
    """
    Build Gold layer for all data (no filters).

    Returns:
        Dict with result summary
    """
    return build_gold_layer(year=None, state_name=None)


if __name__ == "__main__":
    build_gold_layer.serve(name="build-gold-layer")
