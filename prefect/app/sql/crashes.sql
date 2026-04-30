-- ============================================================
-- BRONZE LAYER (Raw Data)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;
DROP SCHEMA  bronze;

SE
DROP TABLE IF EXISTS bronze.fars_crashes
CREATE TABLE IF NOT EXISTS bronze.fars_crashes (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    state_code INTEGER NOT NULL,
    state_name TEXT,
    count INTEGER NOT NULL,
    message TEXT,
    results JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, state_code)
);

CREATE INDEX IF NOT EXISTS idx_bronze_fars_crashes_state_year 
    ON bronze.fars_crashes (state_code, start_year, end_year);

-- ============================================================
-- SILVER LAYER (Cleaned/Deduplicated Data)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- Table: Crash Case
CREATE TABLE IF NOT EXISTS silver.parsed_crashes_array (
    id SERIAL PRIMARY KEY,
    st_case INTEGER,
    caseyear INTEGER,
    state_code INTEGER,
    state_name TEXT,
    county INTEGER,
    city INTEGER,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    hour INTEGER,
    minute INTEGER,
    day_week INTEGER,
    day_weekname TEXT,
    func_sys INTEGER,
    func_sysname TEXT,
    harm_ev INTEGER,
    harm_evname TEXT,
    hosp_hr INTEGER,
    hosp_hrname TEXT,
    hosp_mn INTEGER,
    hosp_mnname TEXT,
    route INTEGER,
    routename TEXT,
    sp_jur INTEGER,
    sp_jurname TEXT,
    arr_min INTEGER,
    arr_minname TEXT,
    rur_urb INTEGER,
    rur_urbname TEXT,
    typ_int INTEGER,
    typ_intname TEXT,
    weather INTEGER,
    weathername TEXT,
    lgt_cond INTEGER,
    lgt_condname TEXT,
    fatals INTEGER,
    permvit INTEGER,
    pernotmvit INTEGER,
    ve_total INTEGER,
    ve_forms INTEGER,
    persons INTEGER,
    pvh_invl INTEGER,
    latitude NUMERIC,
    longitud NUMERIC,
    milept NUMERIC,
    mileptname TEXT,
    tway_id TEXT,
    tway_id2 TEXT,
    rd_owner INTEGER,
    rd_ownername TEXT,
    rel_road INTEGER,
    rel_roadname TEXT,
    reljct1 INTEGER,
    reljct1name TEXT,
    reljct2 INTEGER,
    reljct2name TEXT,
    wrk_zone INTEGER,
    wrk_zonename TEXT,
    man_coll INTEGER,
    man_collname TEXT,
    not_hour INTEGER,
    not_hourname TEXT,
    not_min INTEGER,
    not_minname TEXT,
    arr_hour INTEGER,
    arr_hourname TEXT,
    sch_bus INTEGER,
    sch_busname TEXT,
    road_fnc INTEGER,
    road_fncname TEXT,
    cityname TEXT,
    countyname TEXT,
    hourname TEXT,
    monthname TEXT,
    minutename TEXT,
    weather1 INTEGER,
    weather1name TEXT,
    weather2 INTEGER,
    weather2name TEXT,
    drunk_dr INTEGER,
    nhs INTEGER,
    nhsname TEXT,
    cf1 INTEGER,
    cf1name TEXT,
    cf2 INTEGER,
    cf2name TEXT,
    cf3 INTEGER,
    cf3name TEXT,
    peds INTEGER,
    rail TEXT,
    railname TEXT,
    dayname INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(st_case, caseyear, state_code)
);

CREATE INDEX IF NOT EXISTS idx_silver_parsed_crashes_state_year
    ON silver.parsed_crashes_array (state_code, caseyear);
CREATE INDEX IF NOT EXISTS idx_silver_parsed_crashes_date
    ON silver.parsed_crashes_array (year, month, day);

-- Table: Daily crash counts
CREATE TABLE IF NOT EXISTS silver.daily_crashes (
    id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    state_name TEXT NOT NULL,
    total_crashes INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, month, day, state_name)
);

CREATE INDEX IF NOT EXISTS idx_silver_daily_crashes_date_state
    ON silver.daily_crashes (year, month, day, state_name);
