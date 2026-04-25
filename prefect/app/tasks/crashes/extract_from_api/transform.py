from typing import Any
from prefect import task

import pandas as pd


@task(
    name="unwrap_api_response",
    log_prints=True,
    tags=["transform"],
)
def unwrap_api_response(
    api_response: dict[str, Any], params: dict[str, Any]
) -> pd.DataFrame:
    """Extract and flatten API response for Bronze layer."""
    results_array = api_response.get("Results", [[]])[0]

    return pd.DataFrame.from_dict(
        {
            "start_year": [params["FromYear"]],
            "end_year": [params["ToYear"]],
            "state_code": [params["state"]],
            "state_name": [results_array[0].get("STATENAME") if results_array else None],
            "count": [api_response.get("Count", 0)],
            "message": [api_response.get("Message", "")],
            "results": [results_array],  # Pass as list, pandas/to_sql handles JSONB
        }
    )
