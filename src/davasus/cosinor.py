"""Single-harmonic cosinor analysis (Halberg 1959).

Fits the model

.. math::
    y(t) = M + A \\cos\\!\\Big(\\frac{2\\pi t}{T} - \\phi\\Big) + \\varepsilon

with the period :math:`T` fixed (default 24 h). Solution is closed-form
via the discrete Fourier transform at the chosen frequency — no
iterative optimisation needed.

Output parameters (per :class:`CosinorResult`):

* **mesor** :math:`M` — rhythm-adjusted midline. *Not* the simple
  arithmetic mean unless samples are uniformly distributed across the
  cycle; for hourly aggregates over a full day the two coincide.
* **amplitude** :math:`A` — half the peak-to-trough swing.
* **acrophase_h** — the hour-of-day (0 ≤ h < 24) at which the cosine
  peaks.
* **relative_amplitude** — :math:`A / M`, useful for cross-signal
  comparison.

Reference:
    Halberg, F. (1959). *Physiologic 24-hour periodicity: general and
    procedural considerations with reference to the adrenal cycle.* Z.
    Vitam. Horm. Fermentforsch. 10, 225–296.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from collections.abc import Sequence

log = logging.getLogger(__name__)

DEFAULT_PERIOD_H = 24.0
DEFAULT_MIN_HOURS = 6


@dataclass
class CosinorResult:
    """Outcome of a single cosinor fit.

    Attributes:
        mesor: Rhythm-adjusted midline (same units as the signal).
        amplitude: Half of peak-to-trough range.
        acrophase_h: Hour-of-day of the peak, in ``[0, 24)``.
        relative_amplitude: ``amplitude / mesor`` (NaN if mesor = 0).
        n_hours: Number of distinct hours in the input.
        success: True if the fit met the minimum-coverage requirement.
        reason: Explanation when ``success`` is False.
    """

    mesor: float
    amplitude: float
    acrophase_h: float
    relative_amplitude: float
    n_hours: int
    success: bool
    reason: str = ""


class CosinorFitter:
    """Fit a fixed-period cosine to hour-of-day samples.

    Attributes:
        period_h: Cycle length in hours (24 for circadian).
        min_hours: Minimum distinct hours required to attempt a fit.
        result: Populated by :meth:`fit`; ``None`` before fitting.
    """

    def __init__(
        self,
        period_h: float = DEFAULT_PERIOD_H,
        min_hours: int = DEFAULT_MIN_HOURS,
    ) -> None:
        """Configure the fit.

        Args:
            period_h: Period in hours. Default 24.
            min_hours: Minimum distinct hours; below this returns failure.
        """
        self.period_h = period_h
        self.min_hours = min_hours
        self.result: CosinorResult | None = None

    # ── public API ──────────────────────────────────────────────────────

    def fit(self, hours: Sequence[float], values: Sequence[float]) -> CosinorResult:
        """Fit the cosinor model.

        Args:
            hours: Hour-of-day for each sample, in ``[0, 24)``.
            values: Aligned signal values.

        Returns:
            A :class:`CosinorResult`. ``success=False`` is returned
            (rather than raising) when there are too few distinct hours.
        """
        h, v = self._clean(hours, values)
        n_distinct = int(np.unique(np.floor(h)).size)
        if n_distinct < self.min_hours:
            self.result = self._failed(n_distinct, "too few distinct hours")
            return self.result

        omega = 2.0 * np.pi / self.period_h
        mesor = float(np.mean(v))
        cos_comp = float(np.mean(v * np.cos(omega * h)))
        sin_comp = float(np.mean(v * np.sin(omega * h)))
        amplitude = 2.0 * float(np.hypot(cos_comp, sin_comp))
        acrophase_h = float(np.atan2(sin_comp, cos_comp) * self.period_h / (2.0 * np.pi))
        if acrophase_h < 0.0:
            acrophase_h += self.period_h
        rel_amp = amplitude / mesor if mesor != 0.0 else float("nan")

        self.result = CosinorResult(
            mesor=mesor,
            amplitude=amplitude,
            acrophase_h=acrophase_h,
            relative_amplitude=rel_amp,
            n_hours=n_distinct,
            success=True,
        )
        return self.result

    def predict(self, hours: Sequence[float]) -> np.ndarray:
        """Predict ``y`` from ``hours`` using the fitted parameters.

        Args:
            hours: Hour-of-day points to evaluate at.

        Returns:
            Predicted signal values.

        Raises:
            RuntimeError: If :meth:`fit` has not been called yet.
        """
        if self.result is None:
            raise RuntimeError("call fit() before predict()")
        r = self.result
        h = np.asarray(hours, dtype=float)
        omega = 2.0 * np.pi / self.period_h
        return r.mesor + r.amplitude * np.cos(omega * h - 2.0 * np.pi * r.acrophase_h / self.period_h)

    # ── internals ───────────────────────────────────────────────────────

    @staticmethod
    def _clean(
        hours: Sequence[float], values: Sequence[float]
    ) -> tuple[np.ndarray, np.ndarray]:
        """Drop NaN pairs and coerce to ``float64``.

        Args:
            hours: Predictor values.
            values: Response values.

        Returns:
            ``(h, v)`` arrays of equal length.
        """
        h = np.asarray(hours, dtype=float)
        v = np.asarray(values, dtype=float)
        mask = np.isfinite(h) & np.isfinite(v)
        return h[mask], v[mask]

    @staticmethod
    def _failed(n_hours: int, reason: str) -> CosinorResult:
        """Build a ``success=False`` result with NaN parameters.

        Args:
            n_hours: Distinct hours observed.
            reason: Human-readable failure explanation.
        """
        nan = float("nan")
        return CosinorResult(
            mesor=nan, amplitude=nan, acrophase_h=nan,
            relative_amplitude=nan, n_hours=int(n_hours),
            success=False, reason=reason,
        )
