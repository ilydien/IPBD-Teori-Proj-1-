# IPBD Teori - ETL Pipeline: Analisis Kecelakaan & Cuaca

Pipeline ETL berbasis **Prefect** untuk menganalisis korelasi antara data kecelakaan lalu lintas (FARS) dan kondisi cuaca (Open-Meteo) menggunakan arsitektur **Medallion (Bronze → Silver → Gold)**.

---

## **📋 Daftar Isi**

- [Arsitektur](#arsitektur)
- [Sumber Data](#sumber-data)
- [Pipeline ETL](#pipeline-etl)
- [Struktur Direktori](#struktur-direktori)
- [Prerequisites](#prerequisites)
- [Cara Menjalankan](#cara-menjalankan)
- [Konfigurasi](#konfigurasi)
- [Monitoring](#monitoring)
- [Teknik Kunci](#teknik-kunci)

---

## **🏗️ Arsitektur**

Pipeline menggunakan pendekatan **Medallion Architecture**:

```
Bronze (Raw) → Silver (Cleaned) → Gold (Analytics)
```

- **Bronze**: Data mentah dari API (JSON)
- **Silver**: Data yang dibersihkan dan ditransformasi (Tabular)
- **Gold**: Data gabungan siap analitik bisnis

---

## **📊 Sumber Data**

| Sumber Data | API | Data yang Diambil |
|-------------|-----|-------------------|
| **Kecelakaan** | [FARS API](https://crashviewer.nhtsa.dot.gov/crashviewer/CrashAPI/FARSData/GetFARSData) | Data kecelakaan lalu lintas AS (50 negara bagian, 2012-2015) |
| **Cuaca** | [Open-Meteo Archive API](https://archive-api.open-meteo.com/v1/archive) | Suhu maksimal, minimal, dan curah hujan harian |

---

## **⚙️ Pipeline ETL**

### **1. ETL Data Kecelakaan (FARS)**

#### **Ekstraksi (Extract)**
- **Flow**: `flows/crash_pipeline/flow_01_extract_fars_to_bronze.py`
- **Task**: `fetch_crash_incidents()` dari `tasks/crash/bronze/extract_from_fars_api/extract.py`
- **Parameter**: `state_code` (1-56), `year`, `format` (json)
- **Teknik**: Caching dengan `task_input_hash` (1 jam), retry otomatis 3x

#### **Bronze Layer**
- **Tabel**: `bronze.fars_crashes`
- **Kolom**: year, state_code, state_name, count, results (JSONB)
- **Load**: `insert_raw_to_database()` dengan `ON CONFLICT DO UPDATE` untuk deduplikasi

#### **Silver Layer - Parsing JSON**
- **Flow**: `flows/crash_pipeline/flow_02_parse_crash_cases_to_silver.py`
- **Task Transform**: `flatten_crashes_json()` - Flattening JSON menjadi 50+ kolom tabular
- **Tabel**: `silver.parsed_crashes_array`
- **Kolom**: st_case, fatals, weather, hour, location, dll.

#### **Silver Layer - Agregasi Harian**
- **Flow**: `flows/crash_pipeline/flow_03_aggregate_daily.py`
- **Task Transform**: `agregate_to_daily()` - Group by (year, month, day, state_name)
- **Tabel**: `silver.daily_crashes`

---

### **2. ETL Data Cuaca (Open-Meteo)**

#### **Ekstraksi (Extract)**
- **Flow**: `flows/weather_pipeline/flow_01_extract_openmeteo_to_silver.py`
- **Task**: `fetch_weather_from_api()` dari `tasks/weather/silver/extract_from_openmeteo/extract.py`
- **Data**: temperature_2m_max, temperature_2m_min, precipitation_sum
- **Lokasi**: 50 negara bagian AS dengan koordinat dari `utils/locations.py`

#### **Silver Layer**
- **Tabel**: `silver.daily_weather`
- **Kolom**: location_name, date, year, month, day, temperature, precipitation
- **Load**: `insert_weather_to_silver()` dengan unique constraint (location_name, date)

---

### **3. Gold Layer (Analitik)**

#### **Joined Dataset**
- **Flow**: `flows/gold_pipeline/build_gold_layer.py`
- **Tabel**: `gold.daily_crashes_weather`
- **Proses**: LEFT JOIN antara `silver.daily_weather` + `silver.daily_crashes`
- **Kolom Tambahan**: temperature_2m_avg, total_crashes

#### **Analisis Korelasi**
- **Tabel**: `gold.weather_crash_correlation`
- **Task**: `calculate_weather_crash_correlation()`
- **Metrik**:
  - avg_crashes_per_day per kategori cuaca (weathername)
  - avg_temp_max, avg_temp_min, avg_precipitation
  - total_crashes & total_days

---

## **📁 Struktur Direktori**

```
IPBD-Teori(2)/
├── compose.yaml                    # Docker Compose configuration
├── .env                            # Environment variables
├── prefect/
│   ├── app/
│   │   ├── flows/                  # Prefect flows
│   │   │   ├── crash_pipeline/     # Flow kecelakaan (Bronze→Silver→Daily)
│   │   │   │   ├── flow_01_extract_fars_to_bronze.py
│   │   │   │   ├── flow_02_parse_crash_cases_to_silver.py
│   │   │   │   ├── flow_03_aggregate_daily.py
│   │   │   │   ├── run_all_states_pipeline.py  # Parallel processing
│   │   │   │   └── run_single_state_pipeline.py
│   │   │   ├── weather_pipeline/   # Flow cuaca
│   │   │   │   ├── flow_01_extract_openmeteo_to_silver.py
│   │   │   │   └── flow_02_aggregate_weather_daily.py
│   │   │   ├── gold_pipeline/      # Flow Gold layer
│   │   │   │   ├── build_daily_crashes_weather.py
│   │   │   │   ├── build_gold_layer.py
│   │   │   │   └── build_weather_crash_correlation.py
│   │   │   └── flow_run_full_pipeline.py  # Master orchestrator
│   │   ├── tasks/                  # Prefect tasks
│   │   │   ├── crash/             # Tasks untuk data kecelakaan
│   │   │   │   ├── bronze/
│   │   │   │   ├── silver/
│   │   │   │   └── gold/
│   │   │   └── weather/           # Tasks untuk data cuaca
│   │   ├── sql/                   # DDL scripts
│   │   │   ├── crashes.sql
│   │   │   ├── weather.sql
│   │   │   └── gold.sql
│   │   ├── utils/                 # Utilities
│   │   │   ├── locations.py      # 50 negara bagian AS
│   │   │   ├── helpers.py        # Shared functions
│   │   │   └── database.py       # Database manager
│   │   └── config/
│   │       └── settings.py        # Configuration
│   └── Containerfile              # Docker image definition
├── lakehouse/                      # PostgreSQL data directory
└── metabase/                      # Metabase data directory
```

---

## **🔧 Prerequisites**

- **Docker** & **Docker Compose**
- **Python 3.8+** (untuk pengembangan lokal)
- **PostgreSQL** (didalam container)
- **Prefect 2.x**
- **API Access**: FARS API & Open-Meteo API

---

## **🚀 Cara Menjalankan**

### **Step 1: Start Containers**

```bash
docker compose up -d
```

Cek status:
```bash
docker compose ps
```

---

### **Step 2: Akses Prefect UI**

Buka browser: **http://127.0.0.1:4200**

---

### **Step 3: Jalankan Pipeline**

#### **Opsi A: Menggunakan UI (Recommended)**
1. Buka **Deployments** di Prefect UI
2. Pilih deployment (misal: "run-full-pipeline")
3. Klik **Run** → Configure parameters:
   - `start_year`: 2012
   - `end_year`: 2015
   - `states`: null (untuk semua negara bagian)
4. Submit

#### **Opsi B: Via CLI (Inside Container)**
```bash
docker compose exec prefect-server bash
```

Kemudian jalankan:
```bash
# Full pipeline (Crash + Weather → Gold)
python -m flows.flow_run_full_pipeline

# Atau per-modul:
python -m flows.crash_pipeline.run_all_states_pipeline
python -m flows.weather_pipeline.flow_01_extract_openmeteo_to_silver
python -m flows.gold_pipeline.build_gold_layer
```

---

### **Step 4: Visualisasi dengan Metabase**

Buka **http://127.0.0.1:3000** untuk analitik dan dashboard.

---

## **⚙️ Konfigurasi**

### **Environment Variables (.env)**

```env
HOST_IP=127.0.0.1

# PostgreSQL Lakehouse (ETL Target)
POSTGRES_LAKEHOUSE_USER=postgres
POSTGRES_LAKEHOUSE_PASSWORD=postgres
POSTGRES_LAKEHOUSE_DB=postgres
POSTGRES_LAKEHOUSE_HOST=postgres-lakehouse

# Metabase
METABASE_USER=metabase
METABASE_PASSWORD=mysecretpassword
METABASE_DB=metabasedb
```

### **Settings (config/settings.py)**

| Parameter | Default | Keterangan |
|-----------|---------|-------------|
| `API_TIMEOUT` | 30 detik | Timeout API calls |
| `API_MAX_RETRIES` | 3x | Maksimum retry |
| `API_RETRY_DELAYS` | [2, 5, 10] | Delay antar retry |
| `DB_POOL_SIZE` | 5 | Connection pool size |
| `DB_MAX_OVERFLOW` | 10 | Max overflow connections |

---

## **📊 Monitoring**

### **Cek Logs Container**
```bash
docker compose logs -f prefect-worker   # Lihat eksekusi flow
docker compose logs -f prefect-server  # Lihat API calls
```

### **Cek Database Lakehouse**
```bash
docker compose exec postgres-lakehouse psql -U postgres -d postgres
```

```sql
-- Cek data hasil ETL
SELECT COUNT(*) FROM bronze.fars_crashes;
SELECT COUNT(*) FROM silver.parsed_crashes_array;
SELECT COUNT(*) FROM silver.daily_crashes;
SELECT COUNT(*) FROM gold.daily_crashes_weather;
SELECT * FROM gold.weather_crash_correlation ORDER BY avg_crashes_per_day DESC;
```

---

## **🔑 Teknik Kunci**

| Teknik | Implementasi |
|--------|--------------|
| **Deduplikasi** | `ON CONFLICT DO UPDATE` (UPSERT) |
| **Caching** | Prefect `task_input_hash` (1 jam) |
| **Retry Logic** | 2-3x retry dengan exponential backoff |
| **Rate Limiting** | `time.sleep(1-2)` antar API calls |
| **Checkpoint** | Pengecekan data existing (`check_data_exists()`) |
| **Indexing** | Index pada kolom (state_code, year, date) |
| **JSONB** | PostgreSQL JSONB untuk data mentah di Bronze |
| **Parallelism** | `ConcurrentTaskRunner` untuk multiple states/years |

---

## **📈 Alur Eksekusi Lengkap**

1. **Crash Pipeline** (paralel per state):
   ```
   FARS API → Bronze (JSON) → Silver (Tabular) → Daily Aggregation
   ```

2. **Weather Pipeline** (paralel):
   ```
   Open-Meteo API → Silver (Daily) → Aggregation
   ```

3. **Gold Pipeline** (setelah Crash & Weather selesai):
   ```
   JOIN Silver tables → gold.daily_crashes_weather
   GROUP BY weathername → gold.weather_crash_correlation
   ```

4. **Full Pipeline** (`run_full_pipeline`):
   - ConcurrentTaskRunner menjalankan Crash & Weather secara paralel
   - Gold dibuild setelah kedua pipeline selesai

---

## **⏱️ Estimasi Waktu**

Pipeline penuh (50 states × 4 years = 200 kombinasi):
- **Tanpa paralelisme**: ~3-4 jam (dengan rate limiting 1-2 detik)
- **Dengan paralelisme**: ~1-2 jam (menggunakan ConcurrentTaskRunner)

---

## **📝 Catatan Penting**

- Pipeline dirancang untuk memproses data tahun **2012-2015**
- Menggunakan **50 negara bagian AS** dengan kode FARS (1-56, beberapa kode dilewati)
- **Volume data**: ~200 kombinasi (state × year)
- **API Dependency**: Pastikan FARS API & Open-Meteo API dapat diakses dari container
- **Deduplication**: UPSERT mencegah duplikasi jika flow dijalankan ulang

---

## **🤝 Kontribusi**

Pipeline ini dibuat untuk tugas **IPBD (Implementasi dan Pengelolaan Basis Data) - Semester 4**.

---

## **📚 Referensi**

- [Prefect Documentation](https://docs.prefect.io/)
- [FARS API Documentation](https://crashviewer.nhtsa.dot.gov/CrashAPI)
- [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
- [Medallion Architecture](https://docs.databricks.com/en/lakehouse/medallion.html)

---

**© 2026 - IPBD Teori Semester 4**
