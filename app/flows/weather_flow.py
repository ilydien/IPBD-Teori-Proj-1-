import sys
from pathlib import Path

BASE_DIR = Path("D:/!!COLEGE SAUCE/Semester 4/IPBD/IPBD-Teori(2)")
sys.path.insert(0, str(BASE_DIR / "prefect_app" / "app"))

import requests
from datetime import datetime, timedelta
from prefect import flow, task
from sqlalchemy import create_engine, text
from settings import settings


LOCATIONS = [
    {"name": "Alabama", "lat": 32.806671, "lon": -86.791130},
    {"name": "Philadelphia", "lat": 39.9526, "lon": -75.1652},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
]

POOL_SIZE = 5


def get_db_connection():
    connection_string = settings.get_db_connection_string()
    return create_engine(connection_string, pool_size=POOL_SIZE, max_overflow=POOL_SIZE)


def get_date_range(days_back=30):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=days_back)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


@task
def fetch_weather_for_location(loc, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": loc["lat"],
        "longitude": loc["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "auto",
    }

    print(f"Fetching data for {loc['name']}...")
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    records = []
    daily = data["daily"]
    for i in range(len(daily["time"])):
        records.append(
            {
                "location_name": loc["name"],
                "latitude": loc["lat"],
                "longitude": loc["lon"],
                "date": daily["time"][i],
                "temperature_2m_max": daily["temperature_2m_max"][i],
                "temperature_2m_min": daily["temperature_2m_min"][i],
                "precipitation_sum": daily["precipitation_sum"][i],
            }
        )

    return records


@task
def insert_weather_data(all_records):
    if not all_records:
        print("No records to insert")
        return 0

    engine = get_db_connection()

    query = text("""
        INSERT INTO silver.daily_weather 
        (location_name, latitude, longitude, date, temperature_2m_max, temperature_2m_min, precipitation_sum)
        VALUES (:location_name, :latitude, :longitude, :date, :temperature_2m_max, :temperature_2m_min, :precipitation_sum)
        ON CONFLICT (location_name, date) DO UPDATE SET
            temperature_2m_max = EXCLUDED.temperature_2m_max,
            temperature_2m_min = EXCLUDED.temperature_2m_min,
            precipitation_sum = EXCLUDED.precipitation_sum
    """)

    with engine.begin() as connection:
        for r in all_records:
            connection.execute(query, r)

    engine.dispose()

    print(f"Inserted {len(all_records)} records")
    return len(all_records)


@flow(name="fetch-weather-data", log_prints=True)
def main_flow():
    YEAR_BATCHES = [
        ("2012-01-01", "2012-12-31"),
        ("2013-01-01", "2013-12-31"),
        ("2014-01-01", "2014-12-31"),
        ("2015-01-01", "2015-12-31"),
    ]

    all_records = []
    for start_date, end_date in YEAR_BATCHES:
        records = fetch_weather_for_location.map(LOCATIONS, start_date, end_date)
        all_records.extend([item for sublist in records for item in sublist])

    count = insert_weather_data(all_records)

    return count


if __name__ == "__main__":
    main_flow.serve(name="weather-deployment")
