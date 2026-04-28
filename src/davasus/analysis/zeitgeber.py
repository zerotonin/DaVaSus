"""Zeitgeber-attribution analysis (M3).

Three artefacts per run:

* **Actogram heatmap per signal** — herd-median value at every
  (date, hour-of-day) cell, with sunrise / solar-noon / sunset curves
  overlaid as the photic reference.
* **Acrophase trajectory** — per-animal cosinor acrophase plotted
  against date alongside the sunrise / sunset curves; a herd-median
  trace makes the entrainment direction visible at a glance.
* **PLV-solar** — per-animal phase-locking value between the rumen
  temperature acrophase trajectory and the solar acrophase (solar noon)
  trajectory. PLV ∈ [0, 1]; values near 1 mean the cow's circadian
  peak rides the sun.

Inputs:

* The cosinor result CSV produced by :class:`CircadianAnalysis`
  (long format, one row per animal × day × signal).
* The DB connection (used for the actogram aggregations and to read
  the weather PotSlrRad signal).

Once the operator allocation log arrives, this module gains a second
PLV term (rumen vs feeding pulse train) and a variance decomposition
(``R²_solar`` vs ``R²_feeding``). For now we ship the solar arm only.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from davasus.constants import (
    WONG_BLACK,
    colour_for_category,
    colour_for_variable,
)
from davasus.extract import HourlyAggregator
from davasus.sun import extract_sun_events
from davasus.viz import save_triplet

log = logging.getLogger(__name__)

SIGNALS_FOR_ACTOGRAM: dict[str, tuple[str, str, str]] = {
    "rumen_temp":  ("rumen_temp",   "Rumen temperature (°C)",       "rumen_temp"),
    "act_index":   ("act_index",    "Bolus activity index",         "activity"),
    "rum_index":   ("rum_index_x",  "Rumination index",             "rumination"),
    "imu_activity": ("imu_activity", "Collar IMU activity (ticks)", "imu"),
}


class ZeitgeberAnalysis:
    """Generate actograms, acrophase trajectory, and PLV-solar.

    Attributes:
        connection: Read-only SQLite connection.
        figdir: Output directory.
        cosinor_fits: Long-format DataFrame from :class:`CircadianAnalysis`.
        aggregator: Bound :class:`HourlyAggregator`.
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        figdir: Path,
        cosinor_fits: pd.DataFrame,
    ) -> None:
        """Bind inputs.

        Args:
            connection: Read-only SQLite connection.
            figdir: Output directory; created if missing.
            cosinor_fits: Long-format cosinor results.
        """
        self.connection = connection
        self.figdir = Path(figdir)
        self.cosinor_fits = cosinor_fits
        self.aggregator = HourlyAggregator(connection)
        self._sun_events: pd.DataFrame | None = None

    # ── public API ──────────────────────────────────────────────────────

    def run(self) -> dict[str, pd.DataFrame]:
        """Run all three analyses.

        Returns:
            Dict with keys ``"actogram"``, ``"trajectory"``, ``"plv"``
            mapping to the long-format CSVs that back the figures.
        """
        sun = self._sun()
        actogram_data = self._build_actograms(sun)
        trajectory = self._plot_acrophase_trajectory(sun)
        plv = self._plot_plv_solar(sun)
        return {"actogram": actogram_data, "trajectory": trajectory, "plv": plv}

    # ── sun-event cache ────────────────────────────────────────────────

    def _sun(self) -> pd.DataFrame:
        """Return (and cache) the sunrise / sunset table for every date."""
        if self._sun_events is not None:
            return self._sun_events
        weather = self.aggregator.weather_with_thi()
        self._sun_events = extract_sun_events(weather)
        return self._sun_events

    # ── actogram heatmaps ───────────────────────────────────────────────

    def _build_actograms(self, sun: pd.DataFrame) -> pd.DataFrame:
        """Build a herd-median actogram for each signal.

        Args:
            sun: Sun-event table (one row per date).

        Returns:
            Long-format DataFrame stacking every (signal, date, hour)
            herd-median cell — saved alongside the figures.
        """
        rows: list[pd.DataFrame] = []
        animal_ids = sorted(self.cosinor_fits["animal_id"].astype(int).unique().tolist())
        per_signal: dict[str, list[pd.DataFrame]] = {tag: [] for tag in SIGNALS_FOR_ACTOGRAM}
        for animal_id in animal_ids:
            signals = self.aggregator.signals(int(animal_id))
            if signals.empty:
                continue
            for tag, (column, _, _) in SIGNALS_FOR_ACTOGRAM.items():
                if column not in signals:
                    continue
                per_signal[tag].append(
                    signals[["date", "hour", column]]
                    .rename(columns={column: "value"})
                )

        for tag, frames in per_signal.items():
            if not frames:
                continue
            stacked = pd.concat(frames, ignore_index=True).dropna(subset=["value"])
            if stacked.empty:
                continue
            herd = (
                stacked.groupby(["date", "hour"], as_index=False)["value"]
                .median()
            )
            herd["signal"] = tag
            rows.append(herd)
            self._plot_actogram(tag, herd, sun)
        return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

    def _plot_actogram(self, signal_tag: str, herd: pd.DataFrame, sun: pd.DataFrame) -> None:
        """Plot the heatmap for one signal with sun-event overlays.

        Args:
            signal_tag: Key of :data:`SIGNALS_FOR_ACTOGRAM`.
            herd: Long-format herd-median frame for this signal.
            sun: Sun-event table.
        """
        _, label, palette_key = SIGNALS_FOR_ACTOGRAM[signal_tag]
        pivot = herd.pivot(index="date", columns="hour", values="value").sort_index()
        # Ensure every hour 0..23 is present for plotting.
        for h in range(24):
            if h not in pivot.columns:
                pivot[h] = np.nan
        pivot = pivot[sorted(pivot.columns)]

        fig, ax = plt.subplots(figsize=(7, 9))
        im = ax.imshow(
            pivot.to_numpy(),
            aspect="auto",
            origin="upper",
            extent=[-0.5, 23.5, len(pivot) - 0.5, -0.5],
            cmap="viridis",
            interpolation="nearest",
        )
        ax.set_xlabel("Hour of day (UTC)")
        ax.set_ylabel("Date")
        ax.set_xticks(range(0, 24, 3))
        # Show one tick per month.
        dates = list(pivot.index)
        if dates:
            month_ticks = [
                i for i, d in enumerate(dates)
                if i == 0 or d.month != dates[i - 1].month
            ]
            ax.set_yticks(month_ticks)
            ax.set_yticklabels([dates[i].isoformat() for i in month_ticks])

        self._overlay_sun(ax, dates, sun)
        cb = fig.colorbar(im, ax=ax, shrink=0.6)
        cb.set_label(label)
        ax.set_title(f"Actogram (herd median) — {label}")
        ax.title.set_color(colour_for_variable(palette_key))

        save_triplet(fig, herd, self.figdir / f"actogram_{signal_tag}")
        plt.close(fig)

    def _overlay_sun(
        self,
        ax: plt.Axes,
        dates: list,
        sun: pd.DataFrame,
    ) -> None:
        """Draw sunrise / solar-noon / sunset curves on the actogram.

        Args:
            ax: Target axes.
            dates: Y-axis date labels (one per row in the heatmap).
            sun: Sun-event table.
        """
        if sun.empty or not dates:
            return
        sun_by_date = sun.set_index("date")
        idx = np.arange(len(dates))
        sunrise = np.array([sun_by_date["sunrise_h"].get(d, np.nan) for d in dates])
        sunset = np.array([sun_by_date["sunset_h"].get(d, np.nan) for d in dates])
        noon = np.array([sun_by_date["solar_noon_h"].get(d, np.nan) for d in dates])
        ax.plot(sunrise, idx, color=WONG_BLACK, lw=1.5, label="sunrise")
        ax.plot(sunset, idx, color=WONG_BLACK, lw=1.5, ls="--", label="sunset")
        ax.plot(noon, idx, color=WONG_BLACK, lw=0.8, ls=":", alpha=0.5, label="solar noon")
        ax.legend(loc="upper right", fontsize=8, framealpha=0.7)

    # ── acrophase trajectory ────────────────────────────────────────────

    def _plot_acrophase_trajectory(self, sun: pd.DataFrame) -> pd.DataFrame:
        """Plot rumen-temp acrophase vs date with sunrise / sunset overlay.

        Args:
            sun: Sun-event table.

        Returns:
            DataFrame backing the figure (long format).
        """
        rumen = self.cosinor_fits[
            (self.cosinor_fits["signal"] == "rumen_temp") & self.cosinor_fits["success"]
        ].copy()
        if rumen.empty:
            log.warning("acrophase trajectory: no rumen cosinor fits to plot")
            return rumen
        rumen["date"] = pd.to_datetime(rumen["date"]).dt.date

        fig, ax = plt.subplots(figsize=(8, 4.5))
        for _animal_id, sub in rumen.groupby("animal_id"):
            ax.plot(
                sub["date"], sub["acrophase_h"],
                color=colour_for_category("paired_line"), alpha=0.10, lw=0.6,
            )
        herd = (
            rumen.groupby("date")["acrophase_h"]
            .median()
            .reset_index()
            .sort_values("date")
        )
        ax.plot(
            herd["date"], herd["acrophase_h"],
            color=colour_for_variable("rumen_temp"), lw=2,
            label="herd median",
        )
        if not sun.empty:
            ax.plot(
                sun["date"], sun["sunrise_h"],
                color=WONG_BLACK, lw=1.2, label="sunrise",
            )
            ax.plot(
                sun["date"], sun["sunset_h"],
                color=WONG_BLACK, lw=1.2, ls="--", label="sunset",
            )
        ax.set_xlabel("Date")
        ax.set_ylabel("Rumen-temperature acrophase (h, UTC)")
        ax.set_ylim(0, 24)
        ax.set_yticks(range(0, 25, 3))
        ax.set_title("Acrophase trajectory — rumen temperature")
        ax.legend(loc="best", fontsize=9)

        save_triplet(
            fig,
            rumen[["animal_id", "date", "acrophase_h", "amplitude", "mesor"]],
            self.figdir / "acrophase_trajectory_rumen_temp",
        )
        plt.close(fig)
        return herd

    # ── PLV-solar ──────────────────────────────────────────────────────

    def _plot_plv_solar(self, sun: pd.DataFrame) -> pd.DataFrame:
        """Per-animal phase-locking value between rumen and solar acrophase.

        Args:
            sun: Sun-event table.

        Returns:
            DataFrame ``[animal_id, plv_solar, n_days]`` written
            alongside the figure.
        """
        rumen = self.cosinor_fits[
            (self.cosinor_fits["signal"] == "rumen_temp") & self.cosinor_fits["success"]
        ].copy()
        if rumen.empty or sun.empty:
            log.warning("PLV-solar: missing cosinor or sun events; skipping")
            return pd.DataFrame()
        rumen["date"] = pd.to_datetime(rumen["date"]).dt.date
        sun_indexed = sun.set_index("date")[["solar_noon_h"]]

        rows: list[dict] = []
        for animal_id, sub in rumen.groupby("animal_id"):
            sub = sub.dropna(subset=["acrophase_h"]).copy()
            sub = sub.join(sun_indexed, on="date").dropna(subset=["solar_noon_h"])
            if sub.empty:
                continue
            theta_rumen = 2.0 * np.pi * sub["acrophase_h"].to_numpy() / 24.0
            theta_solar = 2.0 * np.pi * sub["solar_noon_h"].to_numpy() / 24.0
            phase_diff = theta_rumen - theta_solar
            plv = float(np.abs(np.mean(np.exp(1j * phase_diff))))
            rows.append(
                {"animal_id": int(animal_id), "plv_solar": plv, "n_days": int(len(sub))}
            )
        plv_df = pd.DataFrame(rows).sort_values("plv_solar", ascending=False)
        if plv_df.empty:
            return plv_df

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.hist(
            plv_df["plv_solar"], bins=20,
            color=colour_for_variable("solar"), edgecolor=WONG_BLACK, alpha=0.85,
        )
        median_plv = float(plv_df["plv_solar"].median())
        ax.axvline(
            median_plv, color=colour_for_category("median"), lw=2,
            label=f"median = {median_plv:.2f}",
        )
        ax.set_xlabel("PLV (rumen-temp acrophase ↔ solar noon)")
        ax.set_ylabel("Number of animals")
        ax.set_xlim(0, 1)
        ax.set_title("Phase-locking value to solar reference (M3)")
        ax.legend(loc="best", fontsize=9)
        save_triplet(fig, plv_df, self.figdir / "plv_solar")
        plt.close(fig)
        return plv_df
