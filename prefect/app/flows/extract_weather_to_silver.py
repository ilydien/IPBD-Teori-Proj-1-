"""Flow for extracting weather data from API to Silver layer."""

import sys
import time
from pathlib import Path
sys.path.insert(0, "/prefect")

from prefect import flow

from tasks.weather.extract_from_api.extract import fetch_weather_from_api
from tasks.weather.extract_from_api.load import insert_weather_to_silver, create_weather_table
from utils.locations import locations

start_year_test = 2012
end_year_test = 2015


@flow(name="extract_weather_to_silver", log_prints=True)
def extract_weather_to_silver(
    start_year: int = start_year_test,
    end_year: int = end_year_test,
) -> int:
    """
    Extract weather data from Open-Meteo API to Silver layer.
    
    Args:
        start_year: Start year (default: 2012)
        end_year: End year (default: 2015)
    
    Returns:
        Number of records inserted
    """
    # Step 1: Create weather table if not exists
    create_weather_table()

    # Step 2: Prepare year batches for API calls
    year_batches = []
    for year in range(start_year, end_year + 1):
        year_batches.append((f"{year}-01-01", f"{year}-12-31"))

    all_records = []
    # Step 3: Fetch weather data from Open-Meteo API
    for i, (start_date, end_date) in enumerate(year_batches):
        year = int(start_date[:4])
        for j, loc in enumerate(locations):
            # Check if data already exists in silver layer
            from utils.helpers import check_data_exists

            if check_data_exists(
                schema_name="silver",
                table_name="daily_weather",
                year=year,
                location_name=loc["name"],
            ):
                print(f"⏭️  Skipping: Data already exists for {loc['name']}, year {year}")
                continue

            records = fetch_weather_from_api(loc, start_date, end_date)
            all_records.extend(records)
            # Add delay between calls to avoid rate limiting (skip after last call)
            if i < len(year_batches) - 1 or j < len(locations) - 1:
                time.sleep(2)  # 2 second delay

    # Step 4: Insert all weather records to Silver layer
    total_inserted = insert_weather_to_silver(all_records)

    return total_inserted


if __name__ == "__main__":
    extract_weather_to_silver.serve(name="extract-weather-to-silver")