"""Continuous two-segment piecewise linear regression.

Used for the per-animal heat-stress threshold analysis (M4): rumen
temperature ($y$) versus THI ($x$). Below the breakpoint the slope is
flat (thermoneutral zone); above it, rumen temperature rises linearly
with environmental load.

The model is

.. math::
    y = a + \\beta_1 x + (\\beta_2 - \\beta_1) \\max(0, x - \\mathrm{bp})

with the biological constraint :math:`\\beta_2 > \\beta_1 \\wedge
\\beta_2 > 0`. Breakpoint search is a 200-point grid followed by a
bounded scalar minimisation around the best grid point.

Reference: Muggeo, V. M. R. (2003). Estimating regression models with
unknown break-points. *Statistics in Medicine* 22, 3055–3071.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from collections.abc import Sequence

log = logging.getLogger(__name__)

DEFAULT_GRID_POINTS = 200
DEFAULT_MARGIN_FRAC = 0.05
DEFAULT_MIN_POINTS = 50


@dataclass
class BrokenStickResult:
    """Outcome of a single broken-stick fit.

    Attributes:
        breakpoint: THI value at the change-point.
        intercept: Intercept ``a``.
        slope_below: Slope below the breakpoint (``b1``, expected ≈ 0).
        slope_above: Slope above the breakpoint (``b2``, expected > 0).
        rmse: Root-mean-square residual of the fit.
        r2: Coefficient of determination.
        n_points: Number of (x, y) pairs after NaN removal.
        success: True if the fit met the biological constraint.
        reason: Human-readable explanation when ``success`` is False.
    """

    breakpoint: float
    intercept: float
    slope_below: float
    slope_above: float
    rmse: float
    r2: float
    n_points: int
    success: bool
    reason: str = ""


class BrokenStickRegressor:
    """Fit a continuous two-segment piecewise linear model.

    Attributes:
        grid_points: Number of breakpoint candidates in the initial grid.
        margin_frac: Fraction of the x-range to exclude from each end of
            the grid (avoids degenerate breakpoints at the data edges).
        min_points: Minimum sample size required to attempt a fit.
        result: Populated by :meth:`fit`; ``None`` before fitting.
    """

    def __init__(
        self,
        grid_points: int = DEFAULT_GRID_POINTS,
        margin_frac: float = DEFAULT_MARGIN_FRAC,
        min_points: int = DEFAULT_MIN_POINTS,
    ) -> None:
        """Configure the search resolution.

        Args:
            grid_points: Initial grid resolution for the breakpoint scan.
            margin_frac: Edge fraction to exclude from the grid bounds.
            min_points: Minimum non-NaN sample size to proceed.
        """
        self.grid_points = grid_points
        self.margin_frac = margin_frac
        self.min_points = min_points
        self.result: BrokenStickResult | None = None

    # ── public API ──────────────────────────────────────────────────────

    def fit(self, x: Sequence[float], y: Sequence[float]) -> BrokenStickResult:
        """Fit the broken-stick model to ``(x, y)``.

        Args:
            x: Predictor values (THI). Will be coerced to ``float``.
            y: Response values (rumen temperature). Aligned with ``x``.

        Returns:
            A :class:`BrokenStickResult`. ``success=False`` is returned
            (rather than raising) when the data fails preconditions or
            the biological constraint is violated.
        """
        xa, ya = self._clean(x, y)
        if xa.size < self.min_points:
            self.result = self._failed(xa.size, "too few points")
            return self.result

        bp_lo, bp_hi = self._grid_bounds(xa)
        if bp_lo >= bp_hi:
            self.result = self._failed(xa.size, "degenerate THI range")
            return self.result

        bp_best, sse_best, params_best = self._grid_search(xa, ya, bp_lo, bp_hi)
        bp_refined, params_refined, sse_refined = self._refine(
            xa, ya, bp_best, sse_best, params_best, bp_lo, bp_hi
        )
        a, b1, b2 = params_refined

        if not (b2 > b1 and b2 > 0.0):
            self.result = self._failed(
                xa.size, f"biological constraint failed (b1={b1:.3g}, b2={b2:.3g})"
            )
            return self.result

        rmse, r2 = self._goodness_of_fit(xa, ya, bp_refined, a, b1, b2, sse_refined)
        self.result = BrokenStickResult(
            breakpoint=float(bp_refined),
            intercept=float(a),
            slope_below=float(b1),
            slope_above=float(b2),
            rmse=float(rmse),
            r2=float(r2),
            n_points=int(xa.size),
            success=True,
        )
        return self.result

    def predict(self, x: Sequence[float]) -> np.ndarray:
        """Predict ``y`` from ``x`` using the fitted parameters.

        Args:
            x: Input THI values.

        Returns:
            Predicted rumen-temperature values.

        Raises:
            RuntimeError: If :meth:`fit` has not been called yet.
        """
        if self.result is None:
            raise RuntimeError("call fit() before predict()")
        r = self.result
        xa = np.asarray(x, dtype=float)
        return r.intercept + r.slope_below * xa + (r.slope_above - r.slope_below) * np.maximum(
            0.0, xa - r.breakpoint
        )

    # ── core math ───────────────────────────────────────────────────────

    @staticmethod
    def _clean(x: Sequence[float], y: Sequence[float]) -> tuple[np.ndarray, np.ndarray]:
        """Drop NaN pairs and return as ``float64`` arrays.

        Args:
            x: Predictor.
            y: Response.

        Returns:
            ``(x_clean, y_clean)`` of equal length.
        """
        xa = np.asarray(x, dtype=float)
        ya = np.asarray(y, dtype=float)
        mask = np.isfinite(xa) & np.isfinite(ya)
        return xa[mask], ya[mask]

    def _grid_bounds(self, xa: np.ndarray) -> tuple[float, float]:
        """Compute the inclusive grid bounds for the breakpoint search.

        Args:
            xa: Cleaned predictor array.

        Returns:
            ``(low, high)`` in the same units as ``xa``.
        """
        x_min = float(np.min(xa))
        x_max = float(np.max(xa))
        margin = self.margin_frac * (x_max - x_min)
        return x_min + margin, x_max - margin

    @staticmethod
    def _fit_for_bp(
        xa: np.ndarray, ya: np.ndarray, bp: float
    ) -> tuple[tuple[float, float, float], float]:
        """OLS fit of the model with a fixed breakpoint.

        Args:
            xa: Predictor (cleaned).
            ya: Response (cleaned).
            bp: Candidate breakpoint.

        Returns:
            ``((a, b1, b2), sse)`` where ``b2 = b1 + delta``.
        """
        design = np.column_stack(
            (np.ones_like(xa), xa, np.maximum(0.0, xa - bp))
        )
        coeffs, _, _, _ = np.linalg.lstsq(design, ya, rcond=None)
        a, b1, delta = (float(c) for c in coeffs)
        b2 = b1 + delta
        residuals = ya - (a + b1 * xa + delta * np.maximum(0.0, xa - bp))
        sse = float(np.sum(residuals * residuals))
        return (a, b1, b2), sse

    def _grid_search(
        self, xa: np.ndarray, ya: np.ndarray, bp_lo: float, bp_hi: float
    ) -> tuple[float, float, tuple[float, float, float]]:
        """Scan ``grid_points`` candidate breakpoints, return the best.

        Args:
            xa: Predictor.
            ya: Response.
            bp_lo: Lower grid bound.
            bp_hi: Upper grid bound.

        Returns:
            ``(bp_best, sse_best, (a, b1, b2))``.
        """
        candidates = np.linspace(bp_lo, bp_hi, self.grid_points)
        best_bp = float(candidates[0])
        best_sse = float("inf")
        best_params: tuple[float, float, float] = (0.0, 0.0, 0.0)
        for bp in candidates:
            params, sse = self._fit_for_bp(xa, ya, float(bp))
            if sse < best_sse:
                best_sse = sse
                best_bp = float(bp)
                best_params = params
        return best_bp, best_sse, best_params

    def _refine(
        self,
        xa: np.ndarray,
        ya: np.ndarray,
        bp_seed: float,
        sse_seed: float,
        params_seed: tuple[float, float, float],
        bp_lo: float,
        bp_hi: float,
    ) -> tuple[float, tuple[float, float, float], float]:
        """Refine the breakpoint with a bounded scalar minimiser.

        Falls back to the grid result if scipy is unavailable or the
        minimiser fails to improve the SSE.

        Args:
            xa: Predictor.
            ya: Response.
            bp_seed: Best breakpoint from the grid.
            sse_seed: SSE at ``bp_seed``.
            params_seed: ``(a, b1, b2)`` at ``bp_seed``.
            bp_lo: Lower bound for the refinement.
            bp_hi: Upper bound.

        Returns:
            ``(bp_refined, (a, b1, b2), sse_refined)``.
        """
        try:
            from scipy.optimize import minimize_scalar
        except ImportError:
            return bp_seed, params_seed, sse_seed

        def _sse(bp: float) -> float:
            _, sse = self._fit_for_bp(xa, ya, float(bp))
            return sse

        # Search a tight window around the grid winner; fall back to
        # the full grid bounds if the grid spacing is wider than expected.
        step = (bp_hi - bp_lo) / max(self.grid_points - 1, 1)
        lo = max(bp_lo, bp_seed - 2 * step)
        hi = min(bp_hi, bp_seed + 2 * step)
        if not (lo < bp_seed < hi):
            return bp_seed, params_seed, sse_seed

        res = minimize_scalar(_sse, bounds=(lo, hi), method="bounded")
        if not res.success or res.fun >= sse_seed:
            return bp_seed, params_seed, sse_seed
        params, sse = self._fit_for_bp(xa, ya, float(res.x))
        return float(res.x), params, sse

    @staticmethod
    def _goodness_of_fit(
        xa: np.ndarray,
        ya: np.ndarray,
        bp: float,
        a: float,
        b1: float,
        b2: float,
        sse: float,
    ) -> tuple[float, float]:
        """Return ``(rmse, r2)`` for the fitted model.

        Args:
            xa: Predictor (cleaned).
            ya: Response (cleaned).
            bp: Fitted breakpoint.
            a: Fitted intercept.
            b1: Fitted slope below.
            b2: Fitted slope above.
            sse: Residual sum-of-squares at the fitted parameters.

        Returns:
            ``(rmse, r2)``.
        """
        n = xa.size
        rmse = float(np.sqrt(sse / n))
        ss_total = float(np.sum((ya - np.mean(ya)) ** 2))
        r2 = 1.0 - sse / ss_total if ss_total > 0 else float("nan")
        return rmse, r2

    @staticmethod
    def _failed(n_points: int, reason: str) -> BrokenStickResult:
        """Build a ``success=False`` result with NaN parameters.

        Args:
            n_points: Sample size after cleaning.
            reason: Human-readable explanation for the failure.

        Returns:
            A failed :class:`BrokenStickResult`.
        """
        nan = float("nan")
        return BrokenStickResult(
            breakpoint=nan, intercept=nan,
            slope_below=nan, slope_above=nan,
            rmse=nan, r2=nan,
            n_points=int(n_points),
            success=False, reason=reason,
        )
