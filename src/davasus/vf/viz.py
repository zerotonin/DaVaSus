# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz                                                ║
# ║  « where she walked, what she heard »                            ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Single-animal figures for M8: the 24 h trajectory on the herd   ║
# ║  background, and the cue timeline with daily SR and the file's   ║
# ║  ratio R. Each returns (figure, data) for save_triplet (D5).     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Figures for the virtual-fence analysis."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.collections import LineCollection
from matplotlib.figure import Figure

from davasus.vf.constants import MIN_DAILY_AUDIO, SCALE_BAR_M, TRAJECTORY_CMAP, VF_COLOURS
from davasus.vf.geometry import LocalProjection, is_plausible_fix
from davasus.viz import apply_style

apply_style()

# ┌────────────────────────────────────────────────────────────┐
# │ 24 h trajectory  « one animal, one UTC day »               │
# └────────────────────────────────────────────────────────────┘


def plot_daily_trajectory(
    day_fixes: pd.DataFrame,
    lom: str,
    projection: LocalProjection,
) -> tuple[Figure, pd.DataFrame]:
    """Track of ``lom`` over one day, coloured by hour, on the herd's fixes.

    Args:
        day_fixes:  All animals' fixes for the day (``load_fixes`` output).
        lom:        Ear tag of the focal animal.
        projection: Farm projection shared by all figures.

    Returns:
        Figure and the focal animal's fixes with ``x_m``, ``y_m``, ``hour``.
    """
    fixes = projection.add_xy(day_fixes)
    ok = is_plausible_fix(fixes)
    focal = fixes[fixes["lom"] == lom].sort_values("timestamp")
    if focal.empty:
        raise ValueError(f"no fixes for {lom} on this day")
    focal = focal.assign(hour=_decimal_hour(focal["timestamp"]), is_plausible=ok[focal.index])
    track = focal[focal["is_plausible"]]

    fig, ax = plt.subplots(figsize=(7.0, 6.0))
    herd = fixes[ok & (fixes["lom"] != lom)]
    ax.scatter(herd["x_m"], herd["y_m"], s=1, color=VF_COLOURS["herd"], alpha=0.15,
               linewidths=0, label="herd, same day", rasterized=True)
    line = _hour_coloured_line(track["x_m"].to_numpy(), track["y_m"].to_numpy(),
                               track["hour"].to_numpy())
    ax.add_collection(line)
    _mark_cues(ax, track)
    errors = focal[(focal["outside"] == 2) & (focal["gnss_lat"] != 0)]
    if not errors.empty:
        ax.scatter(errors["x_m"], errors["y_m"], marker="x", s=12,
                   color=VF_COLOURS["gps_error"], label=r"GPS error ($Outside = 2$)")
    ax.plot(*track[["x_m", "y_m"]].iloc[0], "o", ms=7, mfc="white", mec="black",
            label="first fix")
    ax.plot(*track[["x_m", "y_m"]].iloc[-1], "s", ms=7, mfc="black", mec="black",
            label="last fix")

    ax.set_aspect("equal")
    ax.autoscale_view()
    _strip_axes(ax)
    add_scale_bar(ax, SCALE_BAR_M)
    colourbar = fig.colorbar(line, ax=ax, fraction=0.04, pad=0.02, shrink=0.6)
    colourbar.set_label("time of day (h, UTC)")
    colourbar.set_ticks([0, 6, 12, 18, 24])
    day = focal["timestamp"].iloc[0].strftime("%Y-%m-%d")
    ax.set_title(f"{lom}  {day}  (n = {len(track)} plausible fixes)")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.02), ncols=3,
              fontsize="small", frameon=False, markerscale=1.5)
    columns = ["lom", "timestamp", "hour", "gnss_lat", "gnss_lon", "x_m", "y_m",
               "outside", "audio_stim_new", "pulse_stim_new", "is_plausible"]
    return fig, focal[columns]


def _hour_coloured_line(x: np.ndarray, y: np.ndarray, hour: np.ndarray) -> LineCollection:
    points = np.column_stack([x, y]).reshape(-1, 1, 2)
    segments = np.concatenate([points[:-1], points[1:]], axis=1)
    line = LineCollection(segments, cmap=TRAJECTORY_CMAP, linewidths=1.2)
    line.set_array(hour[:-1])
    line.set_clim(0, 24)
    return line


def _mark_cues(ax: plt.Axes, track: pd.DataFrame) -> None:
    audio = track[track["audio_stim_new"] > 0]
    pulse = track[track["pulse_stim_new"] > 0]
    ax.scatter(audio["x_m"], audio["y_m"], s=18, facecolors="none",
               edgecolors=VF_COLOURS["audio"], linewidths=1.0, label="audio cue")
    ax.scatter(pulse["x_m"], pulse["y_m"], s=30, marker="*",
               color=VF_COLOURS["pulse"], label="pulse", zorder=5)


def add_scale_bar(ax: plt.Axes, length_m: float) -> None:
    """Horizontal scale bar in the lower-right corner (axes in metres)."""
    x0, x1 = ax.get_xlim()
    y0, y1 = ax.get_ylim()
    right = x1 - 0.05 * (x1 - x0)
    bottom = y0 + 0.05 * (y1 - y0)
    ax.plot([right - length_m, right], [bottom, bottom], color="black", lw=2.5)
    ax.text(right - length_m / 2, bottom + 0.015 * (y1 - y0), f"{length_m:g} m",
            ha="center", va="bottom", fontsize="small")


def _strip_axes(ax: plt.Axes) -> None:
    ax.grid(False)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)


def _decimal_hour(timestamps: pd.Series) -> pd.Series:
    return timestamps.dt.hour + timestamps.dt.minute / 60.0 + timestamps.dt.second / 3600.0


# ┌────────────────────────────────────────────────────────────┐
# │ Cue timeline  « daily audio, pulse, SR and R »             │
# └────────────────────────────────────────────────────────────┘


def plot_stimulus_timeline(
    daily: pd.DataFrame,
    events: pd.DataFrame,
    lom: str,
) -> tuple[Figure, pd.DataFrame]:
    """Daily cues, SR and R for ``lom`` against the herd and activations.

    Args:
        daily:  ``compute_daily_stimulus_summary`` output for all animals.
        events: ``load_events`` output.
        lom:    Ear tag of the focal animal.

    Returns:
        Figure and the focal animal's daily rows.
    """
    focal = daily[daily["lom"] == lom]
    if focal.empty:
        raise ValueError(f"no daily stimulus rows for {lom}")
    fig, (ax_audio, ax_pulse, ax_ratio) = plt.subplots(
        3, 1, figsize=(9.0, 7.5), sharex=True, gridspec_kw={"height_ratios": [1, 1, 1.4]}
    )
    _plot_counts(ax_audio, focal, "audio", r"audio cues day$^{-1}$")
    _plot_counts(ax_pulse, focal, "pulse", r"pulses day$^{-1}$")
    _plot_ratios(ax_ratio, daily, focal, lom)
    for ax in (ax_audio, ax_pulse, ax_ratio):
        _draw_events(ax, events, label=ax is ax_audio)
    ax_ratio.set_xlabel("date (UTC)")
    fig.autofmt_xdate()
    total_a, total_p = focal["audio"].sum(), focal["pulse"].sum()
    fig.suptitle(f"{lom}: {total_a:.0f} audio cues, {total_p:.0f} pulses")
    fig.tight_layout()
    return fig, focal.reset_index(drop=True)


def _plot_counts(ax: plt.Axes, focal: pd.DataFrame, column: str, ylabel: str) -> None:
    ax.plot(focal["date"], focal[column], drawstyle="steps-mid",
            color=VF_COLOURS[column], lw=1.5)
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)


def _plot_ratios(ax: plt.Axes, daily: pd.DataFrame, focal: pd.DataFrame, lom: str) -> None:
    herd = compute_herd_sr_band(daily[daily["lom"] != lom])
    ax.fill_between(herd["date"], herd["q25"], herd["q75"], step="mid",
                    color=VF_COLOURS["herd"], alpha=0.25, lw=0)
    ax.plot(herd["date"], herd["median"], drawstyle="steps-mid", color=VF_COLOURS["herd"],
            lw=1.0, label=rf"SR, other heifers (median, IQR; $A \geq {MIN_DAILY_AUDIO}$)")
    ax.plot(focal["date"], focal["sr"], "o-", ms=3, color=VF_COLOURS["sr"], lw=1.5,
            label=r"SR $= (A - P)/A$")
    ax.plot(focal["date"], focal["ratio_file"], "s--", ms=3, color=VF_COLOURS["ratio_file"],
            lw=1.2, label=r"R $= A/(A + P)$ (file)")
    ax.plot(focal["date"], focal["sr_cum"], ":", color=VF_COLOURS["sr"], lw=1.5,
            label="SR, cumulative")
    ax.set_ylabel("ratio")
    ax.set_ylim(-0.05, 1.05)
    ax.legend(loc="lower right", fontsize="small", frameon=False, ncols=2)


def compute_herd_sr_band(daily: pd.DataFrame) -> pd.DataFrame:
    """Daily median and IQR of SR over animals with enough cues that day."""
    enough = daily[daily["audio"] >= MIN_DAILY_AUDIO]
    return (
        enough.groupby("date")["sr"]
        .quantile([0.25, 0.5, 0.75])
        .unstack()
        .rename(columns={0.25: "q25", 0.5: "median", 0.75: "q75"})
        .reset_index()
    )


def _draw_events(ax: plt.Axes, events: pd.DataFrame, label: bool) -> None:
    for event in events.itertuples():
        if event.manual_herding:
            ax.axvspan(event.start_utc, event.end_utc, color=VF_COLOURS["manual_herding"],
                       alpha=0.25, lw=0)
        ax.axvline(event.start_utc, color="black", lw=0.6, ls="--")
        if label and not event.is_transition:
            ax.annotate(event.open_parcels, (event.start_utc, 1.0),
                        xycoords=("data", "axes fraction"), xytext=(2, -2),
                        textcoords="offset points", va="top", fontsize="small")
