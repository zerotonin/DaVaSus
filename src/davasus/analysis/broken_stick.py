"""Per-animal broken-stick heat-stress thresholds (M4).

Joins each bolus animal's rumen-temperature time series to the weather
station record, fits a continuous two-segment piecewise linear model of
``temp_without_drink_cycles`` versus THI, and writes:

* ``broken_stick_results.csv`` — one row per animal with breakpoint,
  slopes, fit diagnostics, and a success flag.
* ``breakpoint_distribution.{png,svg,csv}`` — boxplot of all successful
  breakpoints across the herd.
* ``example_fit_<animal_id>.{png,svg,csv}`` × 3 — the best, median,
  and worst R² fits as illustrative examples.

THI defaults to the wind- and solar-adjusted Mader–Gaughan variant per
``Decisions.md`` D10. Pass ``thi_mode='nrc'`` to fall back to the NRC
(1971) base form for sensitivity comparison.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import asdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from davasus.constants import (
    THI_REFERENCE,
    colour_for_category,
    colour_for_variable,
)
from davasus.extract import RumenWeatherExtractor
from davasus.fitting import BrokenStickRegressor, BrokenStickResult
from davasus.viz import save_triplet

log = logging.getLogger(__name__)

THI_MODES = ("nrc", "mader")


class BrokenStickAnalysis:
    """Orchestrate the broken-stick analysis end-to-end.

    Attributes:
        connection: Read-only SQLite connection.
        figdir: Output directory; created if missing.
        thi_mode: ``"mader"`` (default) or ``"nrc"``.
        regressor: :class:`BrokenStickRegressor` reused across animals.
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        figdir: Path,
        thi_mode: str = "mader",
        regressor: BrokenStickRegressor | None = None,
    ) -> None:
        """Configure the analysis.

        Args:
            connection: Read-only SQLite connection.
            figdir: Output directory for the CSV + figure triplets.
            thi_mode: Which THI variant to regress against
                (``"mader"`` or ``"nrc"``).
            regressor: Custom :class:`BrokenStickRegressor` (optional).

        Raises:
            ValueError: If ``thi_mode`` is not in :data:`THI_MODES`.
        """
        if thi_mode not in THI_MODES:
            raise ValueError(f"thi_mode must be one of {THI_MODES}, got {thi_mode!r}")
        self.connection = connection
        self.figdir = Path(figdir)
        self.thi_mode = thi_mode
        self.regressor = regressor or BrokenStickRegressor()
        self._extractor = RumenWeatherExtractor(connection)

    # ── public API ──────────────────────────────────────────────────────

    def run(self) -> pd.DataFrame:
        """Run the analysis and write all artefacts.

        Returns:
            DataFrame indexed by ``animal_id`` with one row per fitted
            animal (including failed fits, marked ``success=False``).
        """
        results = self._fit_all()
        self._write_results_csv(results)
        successful = results[results["success"]]
        if successful.empty:
            log.warning("no successful broken-stick fits — skipping figures")
            return results
        self._plot_breakpoint_distribution(successful)
        self._plot_example_fits(successful)
        return results

    # ── per-animal fitting ──────────────────────────────────────────────

    def _fit_all(self) -> pd.DataFrame:
        """Fit every bolus animal and return the result table.

        Returns:
            DataFrame with one row per animal, sorted by ``animal_id``.
        """
        thi_col = f"thi_{self.thi_mode}"
        rows: list[dict] = []
        for animal_id in self._extractor.iter_bolus_animals():
            df = self._extractor.extract(animal_id)
            if df.empty:
                rows.append(self._row_for_failed(animal_id, 0, "no rows after extract"))
                continue
            x = df[thi_col].to_numpy()
            y = df["body_temp"].to_numpy()
            result = self.regressor.fit(x, y)
            rows.append(self._row_from_result(animal_id, result))
        out = pd.DataFrame(rows)
        out["thi_mode"] = self.thi_mode
        return out.sort_values("animal_id").reset_index(drop=True)

    @staticmethod
    def _row_from_result(animal_id: int, result: BrokenStickResult) -> dict:
        """Adapt a :class:`BrokenStickResult` into a flat row dict."""
        row = {"animal_id": int(animal_id), **asdict(result)}
        return row

    @staticmethod
    def _row_for_failed(animal_id: int, n_points: int, reason: str) -> dict:
        """Row for an animal that couldn't be fit at all."""
        nan = float("nan")
        return {
            "animal_id": int(animal_id),
            "breakpoint": nan, "intercept": nan,
            "slope_below": nan, "slope_above": nan,
            "rmse": nan, "r2": nan,
            "n_points": int(n_points),
            "success": False, "reason": reason,
        }

    # ── output helpers ──────────────────────────────────────────────────

    def _write_results_csv(self, results: pd.DataFrame) -> None:
        """Write the per-animal results CSV.

        Args:
            results: DataFrame returned by :meth:`_fit_all`.
        """
        path = self.figdir / "broken_stick_results.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        results.to_csv(path, index=False)
        n_ok = int(results["success"].sum())
        log.info(
            "wrote %s — %d / %d fits successful (thi_mode=%s)",
            path, n_ok, len(results), self.thi_mode,
        )

    def _plot_breakpoint_distribution(self, ok: pd.DataFrame) -> None:
        """Boxplot of successful breakpoints across the herd.

        Args:
            ok: Subset of results with ``success=True``.
        """
        fig, ax = plt.subplots(figsize=(5, 4))
        ax.boxplot(ok["breakpoint"], vert=True, widths=0.4)
        ax.scatter(
            np.random.normal(1, 0.04, size=len(ok)),
            ok["breakpoint"],
            alpha=0.4, s=14, color=colour_for_category("scatter"),
        )
        ax.axhline(
            THI_REFERENCE,
            color=colour_for_category("reference"),
            ls="--", lw=1, alpha=0.7,
            label=f"Hoffmann 2020 ref. ({THI_REFERENCE:.1f})",
        )
        ax.set_xticks([1])
        ax.set_xticklabels([f"n = {len(ok)}"])
        ax.set_ylabel(f"Breakpoint THI ({self.thi_mode.upper()})")
        ax.set_title("Per-animal heat-stress breakpoint")
        ax.legend(loc="best", fontsize=9)
        save_triplet(fig, ok[["animal_id", "breakpoint"]], self.figdir / "breakpoint_distribution")
        plt.close(fig)

    def _plot_example_fits(self, ok: pd.DataFrame) -> None:
        """Plot best, median, worst R² fits as worked examples.

        Args:
            ok: Subset of results with ``success=True``.
        """
        ranked = ok.sort_values("r2", ascending=False).reset_index(drop=True)
        if len(ranked) >= 3:
            picks = {
                "best":   ranked.iloc[0],
                "median": ranked.iloc[len(ranked) // 2],
                "worst":  ranked.iloc[-1],
            }
        else:
            picks = {f"rank_{i}": row for i, row in ranked.iterrows()}
        for label, row in picks.items():
            self._plot_one_fit(int(row["animal_id"]), row, label)

    def _plot_one_fit(self, animal_id: int, row: pd.Series, label: str) -> None:
        """Plot the data and the fitted line for one animal.

        Args:
            animal_id: Animal id.
            row: Row from the results DataFrame for this animal.
            label: Tag appended to the output filename (``best``,
                ``median``, ``worst``).
        """
        thi_col = f"thi_{self.thi_mode}"
        df = self._extractor.extract(animal_id)
        df = df[[thi_col, "body_temp"]].dropna()
        df = df.rename(columns={thi_col: "thi"})
        if df.empty:
            log.warning("animal %d has no rows for example plot", animal_id)
            return

        bp = float(row["breakpoint"])
        a = float(row["intercept"])
        b1 = float(row["slope_below"])
        b2 = float(row["slope_above"])
        x_line = np.linspace(df["thi"].min(), df["thi"].max(), 200)
        y_line = a + b1 * x_line + (b2 - b1) * np.maximum(0.0, x_line - bp)

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.scatter(
            df["thi"], df["body_temp"],
            s=4, alpha=0.15,
            color=colour_for_variable("thi_" + self.thi_mode),
        )
        ax.plot(
            x_line, y_line,
            color=colour_for_category("fit_line"),
            lw=2, label="fit",
        )
        ax.axvline(
            bp,
            color=colour_for_category("fit_line"),
            lw=1, ls="--", alpha=0.6, label=f"bp = {bp:.2f}",
        )
        ax.set_xlabel(f"THI ({self.thi_mode.upper()})")
        ax.set_ylabel("Rumen temperature (°C, no drink cycles)")
        ax.set_title(
            f"animal {animal_id} — {label} fit (R² = {row['r2']:.3f}, "
            f"n = {int(row['n_points'])})"
        )
        ax.legend(loc="best")

        save_triplet(
            fig, df.assign(animal_id=animal_id),
            self.figdir / f"example_fit_{label}_animal_{animal_id}",
        )
        plt.close(fig)
