"""Transform tasks for flattening crash JSON to tabular format."""

import json
from typing import Any

import pandas as pd

from prefect import task


@task(
    name="flatten_crashes_json",
    log_prints=True,
    tags=["transform"],
)
def flatten_crashes_json(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten crash JSON array to tabular format for Silver layer.

    Takes bronze.fars_crashes data with 'results' JSONB column and flattens
    each crash record into individual columns.

    Args:
        bronze_df: DataFrame from bronze with columns: start_year, end_year, state_code, state_name, count, message, results

    Returns:
        DataFrame with all crash fields flattened (one row per crash record)
    """
    all_crashes = []

    for _, row in bronze_df.iterrows():
        start_year = row["start_year"]
        end_year = row["end_year"]
        state_code = row["state_code"]
        state_name = row["state_name"]

        results_json = row["results"]
        if results_json is None:
            crashes = []
        elif isinstance(results_json, str):
            crashes = json.loads(results_json)
        elif isinstance(results_json, (list, dict)):
            crashes = results_json if isinstance(results_json, list) else [results_json]
        else:
            crashes = []

        for crash in crashes:
            flattened = {
                "state_code": state_code,
                "state_name": state_name,
            }
            for key, value in crash.items():
                normalized_key = key.lower()
                if value == "" or value is None:
                    flattened[normalized_key] = None
                elif isinstance(value, str) and value.isdigit():
                    flattened[normalized_key] = int(value)
                else:
                    try:
                        flattened[normalized_key] = int(value)
                    except (ValueError, TypeError):
                        try:
                            flattened[normalized_key] = float(value)
                        except (ValueError, TypeError):
                            flattened[normalized_key] = value

            all_crashes.append(flattened)

    df = pd.DataFrame(all_crashes)
    print(f"Flattened {len(df)} crash records")
    return df
