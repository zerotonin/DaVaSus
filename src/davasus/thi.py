"""Temperature-Humidity Index helpers.

Two formulae:

* :func:`compute_thi_nrc` — NRC (1971) base THI from temperature and
  relative humidity.
* :func:`compute_thi_mader` — Mader–Gaughan adjustment that adds a
  wind-cooling and a solar-loading term on top of the NRC base.

Inputs are vectorised: scalars, numpy arrays, and pandas Series all
work. NaN propagation follows numpy semantics — any NaN in the inputs
yields NaN in the output for that element.

References:
    NRC (1971) *A guide to environmental research on animals.* National
        Academy of Sciences, Washington DC.
    Mader, T. L., Davis, M. S., Brown-Brandl, T. (2006) Environmental
        factors influencing heat stress in feedlot cattle.
        J. Anim. Sci. 84(3), 712–719.
    Gaughan, J. B., Mader, T. L., Holt, S. M., Lisle, A. (2008) A new
        heat load index for feedlot cattle. J. Anim. Sci. 86(1), 226–234.
"""

from __future__ import annotations

from typing import TypeVar

import numpy as np

ArrayLike = TypeVar("ArrayLike")


def compute_thi_nrc(temp_c: ArrayLike, rel_humid: ArrayLike) -> ArrayLike:
    """Compute the NRC (1971) Temperature-Humidity Index.

    Args:
        temp_c: Air temperature in °C. Scalar, numpy array, or pandas Series.
        rel_humid: Relative humidity as percent (0–100), aligned with ``temp_c``.

    Returns:
        THI in the same shape as the inputs.
    """
    t = np.asarray(temp_c, dtype=float)
    rh = np.asarray(rel_humid, dtype=float)
    t_f = 1.8 * t + 32.0
    return t_f - (0.55 - 0.0055 * rh) * (1.8 * t - 26.8)


def compute_thi_mader(
    temp_c: ArrayLike,
    rel_humid: ArrayLike,
    wind_m_s: ArrayLike,
    solar_w_m2: ArrayLike,
) -> ArrayLike:
    """Compute the wind- and solar-adjusted THI (Mader 2006 / Gaughan 2008).

    Args:
        temp_c: Air temperature in °C.
        rel_humid: Relative humidity (0–100).
        wind_m_s: Wind speed in m/s.
        solar_w_m2: Incoming shortwave radiation in W/m².

    Returns:
        Adjusted THI, same shape as the inputs.
    """
    base = compute_thi_nrc(temp_c, rel_humid)
    u = np.asarray(wind_m_s, dtype=float)
    rsw = np.asarray(solar_w_m2, dtype=float)
    return 4.51 + base - 1.992 * u + 0.0068 * rsw
