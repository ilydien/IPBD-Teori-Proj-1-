-- Silver Schema for Medallion Architecture
-- Weather data from Open-Meteo API (cleaned/processed)

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.daily_weather (
    id SERIAL PRIMARY KEY,
    location_name VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    date DATE,
    temperature_2m_max DECIMAL(5, 2),
    temperature_2m_min DECIMAL(5, 2),
    precipitation_sum DECIMAL(6, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_name, date)
);