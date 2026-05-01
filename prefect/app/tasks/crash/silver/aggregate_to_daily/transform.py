"""Transform tasks for aggregating crash data to daily level."""

from typing import Any

import pandas as pd

from prefect import task


@task(
    name="agregate_to_daily",
    log_prints=True,
    tags=["transform"],
)
def agregate_to_daily(crashes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate crash data to daily level.
    
    Groups by year, month, day, state_name and counts total crashes.
    
    Args:
        crashes_df: DataFrame from silver with flattened crash data
    
    Returns:
        DataFrame aggregated by day/month/year/state_name
    """
    # Step1: Return empty DataFrame if input is empty
    if crashes_df.empty:
        return pd.DataFrame(
            columns=["year", "month", "day", "state_name", "total_crashes"]
        )
    
    # Step2: Validate required columns exist
    required_cols = ["year", "month", "day", "state_name"]
    for col in required_cols:
        if col not in crashes_df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    # Step3: Group by date and state, count crashes
    aggregated = (
        crashes_df.groupby(["year", "month", "day", "state_name"])
        .agg(total_crashes=("st_case", "count"))
        .reset_index()
    )
    
    aggregated = aggregated.rename(columns={"state_name": "state_name"})
    
    # Step4: Return aggregated DataFrame
    print(f"Aggregated {len(crashes_df)} crashes to {len(aggregated)} daily records")
    return aggregated

