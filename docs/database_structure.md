# Database structure

DaVaSus stores raw sensor data in a single SQLite file using a star schema:
small dimension tables hold entity identity, large fact tables hold
time-series measurements, and every fact row carries a `file_id`
foreign key for full provenance back to the original CSV.

The schema is defined in code — `src/davasus/schema.py` is the single
source of truth.

## Why SQLite?

The raw 2024 dataset is ~15 GB across two CSV files (~61 M rows in the
merged file, ~29 K rows in the weather export). Single-user, read-heavy
analytical workloads. SQLite gives us indexed time-range queries,
standard date/time functions, no server process, and a portable
single-file database.

## Why a star schema?

The merged-data CSV contains a denormalised join of three logically
distinct sources (eShepherd collar, smaXtec rumen bolus, ATB weather
station). De-denormalising on ingest gives us:

- One animal/collar/file identity is stored once, not millions of times.
- Each sensor stream becomes a focused fact table with an obvious
  query pattern.
- Weather is ingested from its authoritative raw export at full
  resolution rather than the truncated subset present in the merged
  file.

## Dimension tables

### animals

| Column | Type | Description |
|---|---|---|
| `animal_id` | INTEGER PRIMARY KEY | Animal identifier from the merged CSV (`animal_id` column). Becomes the SQLite rowid alias for fastest lookup. |
| `sex` | TEXT | `f` / `m` once enriched (TBD — not in current data). |
| `origin_country` | TEXT | `PL` / `DE` once enriched (TBD). |
| `calving_first` | TEXT | First calving date once enriched (TBD). |
| `has_bolus` | INTEGER | 1 if the animal has any non-NULL smaXtec row, 0 otherwise. Set during post-ingest validation. |

### neckbands

| Column | Type | Description |
|---|---|---|
| `neckband_id` | TEXT PRIMARY KEY | eShepherd device identifier (e.g. `n0596764580`). |

### source_files

| Column | Type | Description |
|---|---|---|
| `file_id` | INTEGER PRIMARY KEY AUTOINCREMENT | Surrogate key. |
| `filename` | TEXT | Basename of the source CSV. |
| `folder` | TEXT | Containing folder, or `(standalone)` for top-level files. |

Unique constraint on `(filename, folder)`.

## Fact tables

Every fact table includes `file_id INTEGER NOT NULL REFERENCES source_files(file_id)`.

### eshepherd

eShepherd virtual-fence collar measurements: GPS, IMU activity bins,
fence distance, and stimulus counters. One row per collar per minute
of data in the merged CSV.

| Column | Type | Description |
|---|---|---|
| `neckband_id` | TEXT NOT NULL | FK to `neckbands`. |
| `animal_id` | INTEGER | FK to `animals`. Nullable in case a collar is unattached during a window. |
| `timestamp` | TEXT NOT NULL | ISO-8601 UTC. |
| `gnss_lat` | REAL | Latitude (degrees). |
| `gnss_lon` | REAL | Longitude (degrees). |
| `odometer_km` | REAL | Cumulative distance (eShepherd-experimental). |
| `audio_stim_count` | REAL | Cumulative audio-cue counter; resets occur. |
| `pulse_stim_count` | REAL | Cumulative pulse-cue counter; resets occur. |
| `fence_dist_max` | REAL | Max distance to fence in last minute. Negative = inside the inclusion zone. The sentinel `-2147483647` is stripped at ingest. |
| `fence_dist_min` | REAL | Min distance to fence. Same sentinel handling. |
| `imu_tick_40mg`…`imu_tick_240mg` | INTEGER | Accelerometer-tick counts above 40/80/120/160/200/240 mG. 4 Hz sampling. |

**Source:** `merged_eshepherd_smaxtec_weather_data_2024.csv`

### smaxtec

smaXtec rumen-bolus derived metrics. One row per bolus per timestamp;
female animals only (males carry collars but no bolus → all bolus
columns NULL → row not written).

| Column | Type | Description |
|---|---|---|
| `animal_id` | INTEGER NOT NULL | FK to `animals`. |
| `timestamp` | TEXT NOT NULL | ISO-8601 UTC. |
| `act_index` | REAL | Activity index. |
| `temp` | REAL | Rumen temperature (°C). |
| `temp_normal_index` | REAL | Deviation from animal-specific normal temperature. |
| `heat_index` | REAL | Heat / estrus index (80–90 = brunst). |
| `calving_index` | REAL | Calving-prediction index. |
| `rum_index_x` | REAL | Rumination index (channel x). |
| `rum_index_y` | REAL | Rumination index (channel y). |
| `act` | REAL | Raw activity. |
| `temp_dec_index` | REAL | Temperature-decrease index. |
| `temp_height_index` | REAL | Temperature-height index. |
| `temp_inc_index` | REAL | Temperature-increase index. |
| `temp_without_drink_cycles` | REAL | Rumen temp with cold-drink artefacts removed (preferred for thermal analysis). |
| `water_intake` | REAL | Estimated water intake (litres). |

**Source:** `merged_eshepherd_smaxtec_weather_data_2024.csv`

### weather

ATB Potsdam weather-station readings at 15-minute resolution. A
curated subset of the raw `Weather_*.csv` columns: meteorological
variables, full radiation balance, soil heat flux, and soil moisture
averages. Sensor housekeeping (battery, panel temperature, diagnostic
counters) is intentionally omitted.

| Column | Type | Description |
|---|---|---|
| `timestamp` | TEXT PRIMARY KEY | ISO-8601 UTC. |
| `record` | INTEGER | Logger record number. |
| `air_temp_{min,avg,max,std}` | REAL | Air temperature (°C). |
| `rel_humid_{min,avg,max}` | REAL | Relative humidity (%). |
| `rain_mm_tot` | REAL | Rainfall total (mm). |
| `rain_corr_mm_tot` | REAL | Wind-corrected rainfall total (mm). |
| `bp_mbar_{min,avg,max,std}` | REAL | Barometric pressure (mbar). |
| `wind_dir_deg` | REAL | Wind direction (degrees). |
| `wind_spd_{min,avg,max,std}` | REAL | Wind speed (m/s). |
| `tdewpt_c_avg` | REAL | Dew-point temperature (°C). |
| `twetbulb_c_avg` | REAL | Wet-bulb temperature (°C). |
| `sun_hrs_tot` | REAL | Sunshine duration (h). |
| `pot_slr_rad_avg` | REAL | Potential solar radiation (W/m²; negative at night). Primary photic zeitgeber. |
| `ground_temp_{min,avg,max,std}` | REAL | Soil-surface temperature (°C). |
| `rad_swin_*` | REAL | Incoming shortwave radiation (W/m²). |
| `rad_swout_*` | REAL | Outgoing shortwave (reflected) radiation. |
| `rad_lwin_*` | REAL | Incoming longwave radiation. |
| `rad_lwout_*` | REAL | Outgoing longwave radiation. |
| `rad_swnet_*` | REAL | Net shortwave. |
| `rad_lwnet_*` | REAL | Net longwave. |
| `rad_sw_albedo_*` | REAL | Shortwave albedo. |
| `rad_net_*` | REAL | Net radiation. |
| `shf_a_avg`, `shf_b_avg` | REAL | Soil heat flux (two probes). |
| `vwc_c_avg`, `vwc_d_avg` | REAL | Volumetric water content (two probes). |

**Source:** `Weather_20240221to20241218.csv`

## Indices

Created after bulk insertion to keep ingest fast:

| Index | Table | Columns |
|---|---|---|
| `idx_eshepherd_neckband_ts` | `eshepherd` | `neckband_id, timestamp` |
| `idx_eshepherd_animal_ts` | `eshepherd` | `animal_id, timestamp` |
| `idx_smaxtec_animal_ts` | `smaxtec` | `animal_id, timestamp` |
| `idx_source_folder` | `source_files` | `folder` |

The `weather` table's `timestamp PRIMARY KEY` already provides the
needed lookup index.

## Planned additions

- `allocation_events` table once the operator log arrives.
- View hierarchy (`v_smaxtec_hourly`, `v_eshepherd_daily_track`,
  `v_analysis_zeitgeber`, …) defined in `create_views.sql`.
- Animal enrichment: sex, origin (PL/DE), calving dates, lost-collar
  episodes.
