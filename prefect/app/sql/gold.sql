-- GOLD LAYER (Business-Level Joins & Aggregations)

CREATE SCHEMA IF NOT EXISTS gold;

-- Table: Daily Crashes and Weather
CREATE TABLE IF NOT EXISTS gold.daily_crashes_weather (
    id SERIAL PRIMARY KEY,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    state TEXT NOT NULL,
    longitude DECIMAL(11, 8),
    latitude DECIMAL(10, 8),
    temperature_2m_max DECIMAL(5, 2),
    temperature_2m_min DECIMAL(5, 2),
    precipitation_sum DECIMAL(6, 2),
    temperature_2m_avg DECIMAL(5, 2),
    total_crashes INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month, day, state)
);

CREATE INDEX IF NOT EXISTS idx_gold_daily_crashes_weather_date_state
    ON gold.daily_crashes_weather (year, month, day, state);

-- Table: Weather-Crash Correlation Summary
CREATE TABLE IF NOT EXISTS gold.weather_crash_correlation (
    id SERIAL PRIMARY KEY,
    weather_name TEXT NOT NULL,
    total_crashes INTEGER NOT NULL,
    total_days INTEGER NOT NULL,
    avg_crashes_per_day DECIMAL(5, 2) NOT NULL,
    avg_temp_max DECIMAL(5, 2),
    avg_temp_min DECIMAL(5, 2),
    avg_precipitation DECIMAL(6, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(weather_name)
);

CREATE INDEX IF NOT EXISTS idx_gold_correlation_weather
    ON gold.weather_crash_correlation (weather_name);
