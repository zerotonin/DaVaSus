"""Per-animal-day cosinor analysis (M2) — DigiMuh-parity.

For every bolus animal × every day × four signals:

* rumen temperature (``temp_without_drink_cycles``)
* bolus activity index (``act_index``)
* rumination index (``rum_index_x``)
* collar IMU activity (sum of ``imu_tick_*`` channels)

we aggregate to hourly means and fit a single 24 h cosinor (Halberg
1959). Each per-day fit is tagged as **heat-stress** or **cool** using
the per-animal breakpoints from M4 — heat-stress = the day's maximum
Mader-Gaughan THI exceeded the animal's broken-stick breakpoint.

Outputs:

* ``cosinor_fits.csv`` — one row per ``(animal_id, date, signal)`` with
  mesor / amplitude / acrophase + ``heat_stress_day``.
* ``profile_24h_<signal>.{png,svg,csv}`` — herd-mean hourly profile,
  one trace per heat-stress status (DigiMuh-parity figure).

Reference: see ``Decisions.md`` D10–D12 for the M4 breakpoint
methodology that the heat-stress tag relies on.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from davasus.constants import colour_for_category, colour_for_variable
from davasus.cosinor import CosinorFitter, CosinorResult
from davasus.extract import HourlyAggregator
from davasus.viz import save_triplet

log = logging.getLogger(__name__)

# Mapping from result-row "signal" tag to (column-in-aggregated-frame,
# nice axis label, palette key in COLOURS["variables"]).
SIGNALS: dict[str, tuple[str, str, str]] = {
    "rumen_temp":  ("rumen_temp",   "Rumen temperature (°C)",       "rumen_temp"),
    "act_index":   ("act_index",    "Bolus activity index",         "activity"),
    "rum_index":   ("rum_index_x",  "Rumination index",             "rumination"),
    "imu_activity": ("imu_activity", "Collar IMU activity (ticks)", "imu"),
}


class CircadianAnalysis:
    """Run the full cosinor pipeline and emit the M2 outputs.

    Attributes:
        connection: Read-only SQLite connection.
        figdir: Output directory.
        breakpoints: DataFrame with the M4 broken-stick results.
        fitter: :class:`CosinorFitter` reused across rows.
        aggregator: :class:`HourlyAggregator` bound to ``connection``.
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        figdir: Path,
        breakpoints: pd.DataFrame,
        fitter: CosinorFitter | None = None,
    ) -> None:
        """Configure the analysis.

        Args:
            connection: Read-only SQLite connection.
            figdir: Output directory; created if missing.
            breakpoints: DataFrame following the
                ``broken_stick_results.csv`` schema (must contain
                ``animal_id``, ``breakpoint``, ``success``).
            fitter: Custom :class:`CosinorFitter` (optional).
        """
        self.connection = connection
        self.figdir = Path(figdir)
        self.breakpoints = breakpoints
        self.fitter = fitter or CosinorFitter()
        self.aggregator = HourlyAggregator(connection)

    # ── public API ──────────────────────────────────────────────────────

    def run(self) -> pd.DataFrame:
        """Run the analysis and write all artefacts.

        Returns:
            Long-format DataFrame of cosinor fits — one row per
            ``(animal_id, date, signal)``.
        """
        heat_index = self._heat_index()
        fits = self._fit_all(heat_index)
        self._write_fits_csv(fits)
        ok = fits[fits["success"]]
        if ok.empty:
            log.warning("no successful cosinor fits — skipping figures")
            return fits
        self._plot_24h_profiles(ok, heat_index)
        self._plot_amplitude_boxplots(ok)
        return fits

    # ── internals ──────────────────────────────────────────────────────

    def _heat_index(self) -> pd.DataFrame:
        """Heat-stress tag per ``(animal_id, date)`` from the breakpoints."""
        return self.aggregator.heat_stress_days(self.breakpoints)

    def _fit_all(self, heat_index: pd.DataFrame) -> pd.DataFrame:
        """Cosinor-fit every animal × day × signal.

        Args:
            heat_index: Output of :meth:`HourlyAggregator.heat_stress_days`.

        Returns:
            Long-format DataFrame.
        """
        animals = sorted(self.breakpoints[self.breakpoints["success"]]["animal_id"].astype(int).tolist())
        rows: list[dict] = []
        heat_lookup = self._heat_lookup(heat_index)
        for animal_id in animals:
            signals = self.aggregator.signals(animal_id)
            if signals.empty:
                continue
            rows.extend(self._fit_animal(animal_id, signals, heat_lookup))
        if not rows:
            return pd.DataFrame(
                columns=[
                    "animal_id", "date", "signal",
                    "mesor", "amplitude", "acrophase_h", "relative_amplitude",
                    "n_hours", "success", "reason", "heat_stress_day",
                ],
            )
        out = pd.DataFrame(rows).sort_values(["animal_id", "date", "signal"]).reset_index(drop=True)
        return out

    def _fit_animal(
        self,
        animal_id: int,
        signals: pd.DataFrame,
        heat_lookup: dict[tuple[int, object], bool],
    ) -> list[dict]:
        """Cosinor-fit every (date, signal) for one animal.

        Args:
            animal_id: Animal id.
            signals: Per-(date, hour) frame for this animal.
            heat_lookup: ``(animal_id, date) → heat_stress_day``.

        Returns:
            List of dicts ready to become rows in the result frame.
        """
        rows: list[dict] = []
        for (date, group) in signals.groupby("date"):
            heat = heat_lookup.get((animal_id, date))
            for signal_tag, (column, _, _) in SIGNALS.items():
                if column not in group:
                    continue
                hours = group["hour"].to_numpy()
                values = group[column].to_numpy()
                result = self.fitter.fit(hours, values)
                rows.append(self._row(animal_id, date, signal_tag, result, heat))
        return rows

    @staticmethod
    def _row(
        animal_id: int,
        date,
        signal_tag: str,
        result: CosinorResult,
        heat_stress_day: bool | None,
    ) -> dict:
        """Flatten a :class:`CosinorResult` into a result-row dict."""
        return {
            "animal_id": int(animal_id),
            "date": date,
            "signal": signal_tag,
            **asdict(result),
            "heat_stress_day": heat_stress_day,
        }

    @staticmethod
    def _heat_lookup(heat_index: pd.DataFrame) -> dict[tuple[int, object], bool]:
        """Build a fast ``(animal_id, date) → heat_stress_day`` dict.

        Args:
            heat_index: Output of :meth:`HourlyAggregator.heat_stress_days`.
        """
        if heat_index.empty:
            return {}
        return {
            (int(r.animal_id), r.date): bool(r.heat_stress_day)
            for r in heat_index.itertuples(index=False)
        }

    # ── output helpers ──────────────────────────────────────────────────

    def _write_fits_csv(self, fits: pd.DataFrame) -> None:
        """Write the long-format cosinor results to disk."""
        path = self.figdir / "cosinor_fits.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        fits.to_csv(path, index=False)
        n_ok = int(fits["success"].sum())
        log.info(
            "wrote %s — %d / %d fits successful across %d signals",
            path, n_ok, len(fits),
            fits["signal"].nunique() if not fits.empty else 0,
        )

    def _plot_24h_profiles(self, ok: pd.DataFrame, heat_index: pd.DataFrame) -> None:
        """Herd-mean hourly profile per signal, stratified cool vs heat-stress.

        Args:
            ok: Successful cosinor fits.
            heat_index: Heat-stress tags (used to slice the raw hourly
                signals so we plot the *observed* hourly mean, not
                the cosinor reconstruction).
        """
        heat_lookup = self._heat_lookup(heat_index)
        by_signal: dict[str, list[pd.DataFrame]] = {tag: [] for tag in SIGNALS}
        for animal_id in ok["animal_id"].unique():
            signals = self.aggregator.signals(int(animal_id))
            if signals.empty:
                continue
            signals = signals.copy()
            aid = int(animal_id)
            signals["heat_stress_day"] = signals["date"].map(
                lambda d, _aid=aid: heat_lookup.get((_aid, d))
            )
            for tag, (column, _, _) in SIGNALS.items():
                if column not in signals:
                    continue
                slim = signals[["hour", "heat_stress_day", column]].dropna(
                    subset=[column]
                )
                slim = slim.rename(columns={column: "value"})
                slim["animal_id"] = aid
                by_signal[tag].append(slim)

        for tag, frames in by_signal.items():
            if not frames:
                continue
            cat = pd.concat(frames, ignore_index=True)
            self._plot_one_24h_profile(tag, cat)
            self._plot_one_24h_profile_per_animal(tag, cat)

    def _plot_one_24h_profile(self, signal_tag: str, df: pd.DataFrame) -> None:
        """Plot the cool vs heat-stress hourly profile for one signal.

        Args:
            signal_tag: Key of :data:`SIGNALS`.
            df: Long-format frame with ``hour``, ``heat_stress_day``,
                ``value``.
        """
        _, label, palette_key = SIGNALS[signal_tag]
        cool = df[df["heat_stress_day"] == False]    # noqa: E712
        hot  = df[df["heat_stress_day"] == True]     # noqa: E712

        fig, ax = plt.subplots(figsize=(6.5, 4))
        plotted: list[pd.DataFrame] = []
        for sub, name, colour in (
            (cool, "cool day", colour_for_category("below_bp")),
            (hot,  "heat-stress day", colour_for_category("above_bp")),
        ):
            if sub.empty:
                continue
            agg = (
                sub.groupby("hour")["value"]
                .agg(["mean", "sem", "count"])
                .reset_index()
            )
            agg["status"] = name
            ax.plot(agg["hour"], agg["mean"], color=colour, lw=2, label=f"{name} (n={int(agg['count'].sum())})")
            ax.fill_between(
                agg["hour"], agg["mean"] - agg["sem"], agg["mean"] + agg["sem"],
                color=colour, alpha=0.2,
            )
            plotted.append(agg)

        ax.set_xlabel("Hour of day (UTC)")
        ax.set_ylabel(label)
        ax.set_xlim(0, 23)
        ax.set_xticks(range(0, 24, 3))
        ax.set_title(f"24 h profile — {label}")
        ax.legend(loc="best", fontsize=9)
        # Hint at the variable colour by tinting the title.
        ax.title.set_color(colour_for_variable(palette_key))

        if plotted:
            out = pd.concat(plotted, ignore_index=True)
        else:
            out = pd.DataFrame(columns=["hour", "mean", "sem", "count", "status"])
        save_triplet(fig, out, self.figdir / f"profile_24h_{signal_tag}")
        plt.close(fig)

    def _plot_one_24h_profile_per_animal(
        self, signal_tag: str, df: pd.DataFrame
    ) -> None:
        """Hierarchical 24 h profile — mean of per-animal means.

        For each animal × heat-status × hour, take that animal's mean
        of the signal. Then plot the mean and SEM **across animals**
        per hour. ``N`` reported is the number of contributing animals,
        not observations — this is the right inferential N because
        animals are the experimental unit.

        Args:
            signal_tag: Key of :data:`SIGNALS`.
            df: Long-format frame with ``animal_id``, ``hour``,
                ``heat_stress_day``, ``value``.
        """
        _, label, palette_key = SIGNALS[signal_tag]
        per_animal = (
            df.groupby(["animal_id", "heat_stress_day", "hour"], as_index=False)
            ["value"].mean()
        )
        cool = per_animal[per_animal["heat_stress_day"] == False]   # noqa: E712
        hot  = per_animal[per_animal["heat_stress_day"] == True]    # noqa: E712

        fig, ax = plt.subplots(figsize=(6.5, 4))
        plotted: list[pd.DataFrame] = []
        for sub, name, colour in (
            (cool, "cool day",        colour_for_category("below_bp")),
            (hot,  "heat-stress day", colour_for_category("above_bp")),
        ):
            if sub.empty:
                continue
            agg = (
                sub.groupby("hour")["value"]
                .agg(["mean", "std", "count"])
                .reset_index()
            )
            agg["sem"] = agg["std"] / agg["count"].pow(0.5)
            n_animals = int(sub["animal_id"].nunique())
            agg["status"] = name
            agg["n_animals"] = n_animals
            ax.plot(
                agg["hour"], agg["mean"],
                color=colour, lw=2,
                label=f"{name} (n={n_animals} animals)",
            )
            ax.fill_between(
                agg["hour"], agg["mean"] - agg["sem"], agg["mean"] + agg["sem"],
                color=colour, alpha=0.25,
            )
            plotted.append(agg)

        ax.set_xlabel("Hour of day (UTC)")
        ax.set_ylabel(label)
        ax.set_xlim(0, 23)
        ax.set_xticks(range(0, 24, 3))
        ax.set_title(f"24 h profile (per-animal means ± SEM) — {label}")
        ax.legend(loc="best", fontsize=9)
        ax.title.set_color(colour_for_variable(palette_key))

        if plotted:
            out = pd.concat(plotted, ignore_index=True)
        else:
            out = pd.DataFrame(
                columns=["hour", "mean", "std", "count", "sem", "status", "n_animals"],
            )
        save_triplet(fig, out, self.figdir / f"profile_24h_{signal_tag}_per_animal")
        plt.close(fig)

    def _plot_amplitude_boxplots(self, ok: pd.DataFrame) -> None:
        """Per-signal amplitude boxplot, cool vs heat-stress, herd-wide.

        Args:
            ok: Successful cosinor fits.
        """
        for signal_tag in SIGNALS:
            sub = ok[ok["signal"] == signal_tag].dropna(subset=["amplitude"])
            if sub.empty:
                continue
            fig, ax = plt.subplots(figsize=(5, 4))
            data = [
                sub.loc[sub["heat_stress_day"] == False, "amplitude"].to_numpy(),  # noqa: E712
                sub.loc[sub["heat_stress_day"] == True,  "amplitude"].to_numpy(),  # noqa: E712
            ]
            box = ax.boxplot(
                data, vert=True, widths=0.4,
                patch_artist=True, labels=["cool", "heat-stress"],
            )
            for patch, key in zip(box["boxes"], ("below_bp", "above_bp"), strict=False):
                patch.set_facecolor(colour_for_category(key))
                patch.set_alpha(0.6)
            ax.set_ylabel("Cosinor amplitude")
            ax.set_title(f"Daily rhythm amplitude — {SIGNALS[signal_tag][1]}")
            save_triplet(
                fig,
                sub[["animal_id", "date", "amplitude", "heat_stress_day"]],
                self.figdir / f"amplitude_boxplot_{signal_tag}",
            )
            plt.close(fig)
