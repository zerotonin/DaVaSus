"""Shared pytest fixtures.

Two tiny synthetic CSVs (merged + weather) are written to a temp dir so
ingestion tests run end-to-end in milliseconds.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from davasus.db import Database

# ────────────────────────────────────────────────────────────────────────
#  « Synthetic CSV content »
# ────────────────────────────────────────────────────────────────────────

# Two female animals (with bolus) on collars n0001 and n0002, one male
# animal (collar-only, all bolus columns empty) on collar n0003. One
# fence-sentinel row to verify it gets stripped.

_MERGED_HEADER = (
    "neckband_id,animal_id,timestamp,"
    "GNSS_Latitude,GNSS_Longitude,Odometer_km,"
    "Audio_Stimulus_Count,Pulse_Stimulus_Count,"
    "Distance_To_Fence_Max,Distance_To_Fence_Min,"
    "IMU_Tick_Count_40mG,IMU_Tick_Count_80mG,IMU_Tick_Count_120mG,"
    "IMU_Tick_Count_160mG,IMU_Tick_Count_200mG,IMU_Tick_Count_240mG,"
    "act_index,temp,temp_normal_index,heat_index,calving_index,"
    "rum_index_x,rum_index_y,act,"
    "temp_dec_index,temp_height_index,temp_inc_index,"
    "temp_without_drink_cycles,water_intake,"
    "AirT_C_Avg,RelHumid,Rain_corr_mm_Tot,BP_mbar_Avg,"
    "WindDir_deg,WindSpd_m_s_Avg,WindSpd_m_s_Max,WindSpd_m_s_Min,"
    "Tdewpt_C_Avg,Twetbulb_C_Avg,SunHrs_Tot,PotSlrRad_Avg,"
    "GroundT_C_Avg,Rad_SWin_Avg,Rad_SWout_Avg,THI"
)

_MERGED_ROWS: tuple[str, ...] = (
    # Female with bolus, normal fence reading
    "n0001,1001,2024-03-01 01:00:00+00:00,"
    "52.377,14.268,4.6,0.0,0.0,-12.5,-15.0,"
    "61,35,27,25,21,19,"
    "2.04,39.06,39.27,0.0,0.0,41.72,36045.0,1.33,"
    "0.0,-0.15,0.0,39.12,0.0,"
    "7.4,99.5,0.0,1011.1,124.0,1.5,3.4,0.3,7.3,7.4,0.0,-18.5,5.5,-0.4,0.0,45.4",
    # Female with bolus, fence sentinel that must be stripped
    "n0002,1002,2024-03-01 01:00:00+00:00,"
    "52.378,14.269,4.7,0.0,0.0,-2147483647.0,-2147483648.0,"
    "70,40,30,28,22,20,"
    "2.10,39.10,39.30,0.0,0.0,41.80,36050.0,1.35,"
    "0.0,-0.14,0.0,39.20,0.0,"
    "7.4,99.5,0.0,1011.1,124.0,1.5,3.4,0.3,7.3,7.4,0.0,-18.5,5.5,-0.4,0.0,45.4",
    # Male, collar only — all 13 bolus columns empty (13 commas → 13 empties)
    "n0003,1003,2024-03-01 01:00:00+00:00,"
    "52.379,14.270,4.8,0.0,0.0,-10.0,-12.0,"
    "173,162,145,123,112,101,"
    ",,,,,,,,,,,,,"
    "7.4,99.5,0.0,1011.1,124.0,1.5,3.4,0.3,7.3,7.4,0.0,-18.5,5.5,-0.4,0.0,45.4",
)

# Curated weather header — only the columns the ingestor consumes,
# in a deliberately scrambled order to verify DictReader-based mapping.

_WEATHER_HEADER = (
    "TIMESTAMP,RECORD,"
    "AirT_C_Min,AirT_C_Avg,AirT_C_Max,AirT_C_Std,"
    "RelHumid_Min,RelHumid,RelHumid_Max,"
    "Rain_mm_Tot,Rain_corr_mm_Tot,"
    "BP_mbar_Min,BP_mbar_Avg,BP_mbar_Max,BP_mbar_Std,"
    "WindDir_deg,WindSpd_m_s_Min,WindSpd_m_s_Avg,WindSpd_m_s_Max,WindSpd_m_s_Std,"
    "Tdewpt_C_Avg,Twetbulb_C_Avg,SunHrs_Tot,PotSlrRad_Avg,"
    "GroundT_C_Min,GroundT_C_Avg,GroundT_C_Max,GroundT_C_Std,"
    "Rad_SWin_Min,Rad_SWin_Avg,Rad_SWin_Max,Rad_SWin_Std,"
    "Rad_SWout_Min,Rad_SWout_Avg,Rad_SWout_Max,Rad_SWout_Std,"
    "Rad_LWin_Min,Rad_LWin_Avg,Rad_LWin_Max,Rad_LWin_Std,"
    "Rad_LWout_Min,Rad_LWout_Avg,Rad_LWout_Max,Rad_LWout_Std,"
    "Rad_SWnet_Min,Rad_SWnet_Avg,Rad_SWnet_Max,Rad_SWnet_Std,"
    "Rad_LWnet_Min,Rad_LWnet_Avg,Rad_LWnet_Max,Rad_LWnet_Std,"
    "Rad_SWalbedo_Min,Rad_SWalbedo_Avg,Rad_SWalbedo_Max,Rad_SWalbedo_Std,"
    "Rad_Net_Min,Rad_Net_Avg,Rad_Net_Max,Rad_Net_Std,"
    "SHF_A_Avg,SHF_B_Avg,VWC_C_Avg,VWC_D_Avg"
)

_WEATHER_VALUES = ",".join([
    "2024-02-21 00:00:00+00", "51498",
    "6.4", "6.5", "6.5", "0.01",
    "99.6", "99.6", "99.7",
    "0", "0",
    "1023.4", "1023.5", "1023.6", "0.05",
    "201", "0.66", "1.22", "2.24", "0.23",
    "6.4", "6.4", "0", "-20.1",
    "6.6", "6.6", "6.6", "0.005",
    "-1.2", "-0.8", "-0.5", "0.14",
    "-0.05", "0.05", "0.39", "0.06",
    "312.8", "320.1", "322.3", "2.08",
    "338.4", "339.0", "339.4", "0.27",
    "-1.25", "-0.86", "-0.61", "0.14",
    "-25.4", "-18.8", "-16.9", "1.86",
    "-0.43", "-0.07", "0.07", "0.09",
    "-26.5", "-19.7", "-17.6", "1.96",
    "-4.65", "-4.05", "0.23", "0.24",
])

_WEATHER_VALUES_2 = ",".join([
    "2024-02-21 00:15:00+00", "51499",
    "6.5", "6.5", "6.6", "0.02",
    "99.6", "99.7", "99.7",
    "0", "0",
    "1023.3", "1023.3", "1023.4", "0.04",
    "232", "0.63", "1.36", "2.61", "0.31",
    "6.5", "6.5", "0", "-20.2",
    "6.6", "6.6", "6.6", "0.004",
    "-1.6", "-0.9", "-0.5", "0.24",
    "-0.09", "0.07", "0.43", "0.08",
    "303.3", "315.1", "323.6", "6.39",
    "338.3", "339.1", "339.9", "0.44",
    "-1.67", "-0.99", "-0.51", "0.25",
    "-35.0", "-23.9", "-15.9", "6.02",
    "-0.46", "-0.09", "0.06", "0.10",
    "-36.5", "-24.9", "-16.6", "6.23",
    "-4.63", "-3.96", "0.23", "0.24",
])


# ────────────────────────────────────────────────────────────────────────
#  « Fixtures »
# ────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Return a non-existent path inside ``tmp_path`` for a fresh DB."""
    return tmp_path / "test.db"


@pytest.fixture()
def database(db_path: Path) -> Database:
    """Yield an initialised :class:`Database` and close it after the test."""
    db = Database(db_path)
    db.initialise()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture()
def merged_csv(tmp_path: Path) -> Path:
    """Write a 3-row synthetic merged CSV and return its path."""
    p = tmp_path / "merged.csv"
    p.write_text(_MERGED_HEADER + "\n" + "\n".join(_MERGED_ROWS) + "\n")
    return p


@pytest.fixture()
def weather_csv(tmp_path: Path) -> Path:
    """Write a 2-row synthetic weather CSV and return its path."""
    p = tmp_path / "weather.csv"
    p.write_text(
        _WEATHER_HEADER + "\n"
        + _WEATHER_VALUES + "\n"
        + _WEATHER_VALUES_2 + "\n"
    )
    return p
