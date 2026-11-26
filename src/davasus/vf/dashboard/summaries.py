# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.dashboard.summaries                                ║
# ║  « panel text, front-end agnostic »                              ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Turns minute i of a DashboardData into ordered label/value      ║
# ║  pairs for the info panels and the herd table. Plain strings     ║
# ║  and floats only, so Qt and a web page render the same rows.     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Human-readable rows for the dashboard panels."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from davasus.vf.dashboard.constants import OUTSIDE_LABELS
from davasus.vf.dashboard.data import (
    MISSING_CODE,
    DashboardData,
    compute_animal_cue_summary,
    compute_animal_km_today,
    compute_herd_snapshot,
)
from davasus.vf.gnss_quality import HP30_STORM_THRESHOLD

Rows = list[tuple[str, str]]
DASH: str = "–"


def describe_time(data: DashboardData, i: int) -> Rows:
    stamp = data.minutes[i]
    day = (stamp.floor("D") - data.minutes[0].floor("D")).days + 1
    herd = compute_herd_snapshot(data, i)
    return [
        ("UTC", f"{stamp:%Y-%m-%d %H:%M}"),
        ("local (CEST)", f"{stamp.tz_convert('Europe/Berlin'):%Y-%m-%d %H:%M}"),
        ("trial day", f"{day} of {(data.minutes[-1].floor('D') - data.minutes[0].floor('D')).days + 1}"),
        ("fixes / plausible", f"{herd.n_fixes} / {herd.n_plausible} of {data.n_animals}"),
        ("mean distance to centroid", _num(herd.mean_distance_m, "m", 1)),
        ("GPS sensitivity", describe_gps_sensitivity(data, i)),
    ]


def describe_event(data: DashboardData, i: int) -> Rows:
    row = data.event_index[i]
    if row < 0:
        return [("parcel event", "before first activation")]
    event = data.events.loc[row]
    elapsed = data.minutes[i] - event["start_utc"]
    rows = [
        ("eShepherd parcel", str(event["label"])),
        ("open paddocks", describe_open_paddocks(data, row)),
        ("cut-off paddock", data.paddock_letter(event["cut_off_parcel"])),
        ("new paddock", data.paddock_letter(event["new_parcel"])),
        ("started (UTC)", f"{event['start_utc']:%Y-%m-%d %H:%M}"),
        ("time since start", _format_timedelta(elapsed)),
        ("parcel size", _num(event["parcel_size_ha"], "ha", 2)),
        ("VF length", _num(event["vf_length_m"], "m", 0)),
        ("biomass", _num(event["biomass_kg_ha"], "kg/ha", 0)),
        ("manual herding", "yes" if event["manual_herding"] == 1 else "no"),
        ("water point moved", "yes" if event["water_point_relocated"] == 1 else "no"),
    ]
    if isinstance(event["comments"], str) and event["comments"].strip():
        rows.append(("comment", event["comments"].strip()))
    return rows


def describe_open_paddocks(data: DashboardData, row: int) -> str:
    """'C (3), D (4)' from the geometry; the logged parcel numbers otherwise."""
    letters = data.event_paddocks[row] if row < len(data.event_paddocks) else ()
    if not letters:
        return str(data.events.loc[row, "open_parcels"]).replace("_", " + ")
    return ", ".join(f"{k} ({data.paddock_numbers[k]})" for k in letters)


def describe_weather(data: DashboardData, i: int) -> Rows:
    w = data.weather.iloc[i]
    return [
        ("air temperature", _num(w["air_temp_avg"], "°C", 1)),
        ("relative humidity", _num(w["rel_humid_avg"], "%", 0)),
        ("THI (NRC 1971)", _num(w["thi_nrc"], "", 1)),
        ("rain (15 min)", _num(w["rain_mm_tot"], "mm", 1)),
        ("pressure", _num(w["bp_mbar_avg"], "hPa", 0)),
        ("wind", _num(w["wind_spd_avg"], "m/s", 1)),
        ("net radiation", _num(w["rad_net_avg"], "W/m²", 0)),
        ("sunshine (15 min)", _num(w["sun_hrs_tot"] * 60.0, "min", 0)),
    ]


def describe_space_weather(data: DashboardData, i: int) -> Rows:
    if not data.has_space_weather:
        return [("status", "not fetched; run davasus-vf-spaceweather")]
    s = data.space_weather.iloc[i]
    return [
        ("Hp30", _num(s["hp30"], "", 2)),
        ("ap30", _num(s["ap30"], "nT", 0)),
        ("Kp", _num(s["kp"], "", 2)),
        ("GPS sensitivity", describe_gps_sensitivity(data, i)),
        ("source", "GFZ Potsdam, CC BY 4.0"),
    ]


def describe_heifer(data: DashboardData, i: int, a: int) -> Rows:
    lom = data.loms[a]
    meta = data.heifers.loc[lom]
    code = int(data.outside[i, a])
    cues = compute_animal_cue_summary(data, i, a)
    herd = compute_herd_snapshot(data, i)
    birth = pd.Timestamp(meta["birth_date"], tz="UTC")
    age_months = (data.minutes[i] - birth).days / 30.44
    return [
        ("ear tag (LOM)", lom),
        ("neckband", data.neckbands[a]),
        ("breed / sex", f"{meta['breed']} / {meta['sex']}"),
        ("birth date", f"{birth:%Y-%m-%d}  ({age_months:.1f} months)"),
        ("GVE", _num(meta["gve"], "", 2)),
        ("status", describe_outside_code(code)),
        ("position", _latlon(data.lat[i, a], data.lon[i, a])),
        ("distance to centroid", _num(herd.distance_to_centroid[a], "m", 1)),
        ("fence distance min / max", f"{_num(data.fence_dist_min[i, a], '', 1)} / "
                                     f"{_num(data.fence_dist_max[i, a], 'm', 1)}"),
        ("IMU ticks > 40 mg", _num(data.imu_40mg[i, a], "", 0)),
        ("odometer (collar)", _num(data.odometer_km[i, a], "km", 2)),
        ("distance today", _num(compute_animal_km_today(data, i, a), "km", 2)),
        ("audio / pulses today", f"{cues.audio_day:.0f} / {cues.pulse_day:.0f}"),
        ("SR today", _num(cues.sr_day, "", 3)),
        ("audio / pulses so far", f"{cues.audio_total:.0f} / {cues.pulse_total:.0f}"),
        ("SR so far", _num(cues.sr_total, "", 3)),
    ]


HERD_TABLE_COLUMNS: tuple[str, ...] = (
    "heifer", "status", "dist. centroid (m)", "audio today", "pulses today", "SR today",
)


def build_herd_table(data: DashboardData, i: int) -> list[list[object]]:
    """One row per animal: short tag, Outside code, distance and today's cues."""
    herd = compute_herd_snapshot(data, i)
    rows: list[list[object]] = []
    for a, lom in enumerate(data.loms):
        cues = compute_animal_cue_summary(data, i, a)
        rows.append([
            lom[-4:], int(data.outside[i, a]), _round(herd.distance_to_centroid[a], 1),
            cues.audio_day, cues.pulse_day, _round(cues.sr_day, 3),
        ])
    return rows


def describe_gps_sensitivity(data: DashboardData, i: int) -> str:
    if not data.has_space_weather:
        return "unknown (no space weather)"
    if data.is_gps_reduced[i]:
        return f"REDUCED (Hp30 \u2265 {HP30_STORM_THRESHOLD:g}); omit from analyses"
    return "normal"


def describe_outside_code(code: int) -> str:
    if code == MISSING_CODE:
        return "no fix"
    return f"{code}: {OUTSIDE_LABELS.get(code, 'unknown')}"


def _num(value: float, unit: str, digits: int) -> str:
    if value is None or (isinstance(value, float | np.floating) and not math.isfinite(value)):
        return DASH
    return f"{value:.{digits}f} {unit}".rstrip()


def _round(value: float, digits: int) -> float:
    return round(float(value), digits) if np.isfinite(value) else float("nan")


def _int_or_dash(value: object) -> str:
    return DASH if pd.isna(value) else str(int(value))


def _latlon(lat: float, lon: float) -> str:
    if not (np.isfinite(lat) and np.isfinite(lon)) or (lat == 0 and lon == 0):
        return DASH
    return f"{lat:.6f} N, {lon:.6f} E"


def _format_timedelta(delta: pd.Timedelta) -> str:
    minutes = int(delta.total_seconds() // 60)
    days, rest = divmod(minutes, 1440)
    return f"{days} d {rest // 60:02d}:{rest % 60:02d}"
