-- ============================================================
-- GOLD LAYER (Business-Level Joins & Aggregations)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS gold;

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
