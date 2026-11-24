# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.gnss_quality                                       ║
# ║  « is it the cow or the ionosphere »                             ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Handover 4.9: GNSS-error proxies per 30 min (lost-collar        ║
# ║  jitter, lying-animal noise floor, impossible positions, fix     ║
# ║  gaps, common-mode fence approaches) joined to GFZ Hp30 / Kp     ║
# ║  and DLR ROTI, so behavioural scores can be rerun storm-free.    ║
# ╚══════════════════════════════════════════════════════════════════╝
"""GNSS quality proxies and space-weather covariates (handover 4.9)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

PROXY_INTERVAL_MIN: int = 30
MAX_PLAUSIBLE_SPEED_M_PER_MIN: float = 100.0
HP30_STORM_THRESHOLD: float = 6.0
PROXY_STORM_PERCENTILE: float = 95.0

# NOAA-scale storms in the study years; verify every window against Hp30.
CANDIDATE_STORMS_UTC: tuple[tuple[str, str], ...] = (
    ("2024-05-10 18:00", "2024-05-12 00:00"),
    ("2024-06-27 00:00", "2024-06-29 00:00"),
    ("2024-08-11 00:00", "2024-08-13 00:00"),
    ("2024-09-16 00:00", "2024-09-18 00:00"),
    ("2024-10-10 00:00", "2024-10-11 00:00"),
    ("2025-06-01 00:00", "2025-06-02 00:00"),
)

# ┌────────────────────────────────────────────────────────────┐
# │ Error proxies  « per 30 min, from the collars alone »      │
# └────────────────────────────────────────────────────────────┘


class GnssErrorProxyBuilder:
    """Per-interval GNSS-error proxies from the collar fixes."""

    def build(self, fixes: pd.DataFrame) -> pd.DataFrame:
        """One row per interval: jitter, noise floor, gap and jump rates."""
        raise NotImplementedError

    def flag_degraded_intervals(self, proxies: pd.DataFrame) -> pd.Series:
        """True where any proxy exceeds its percentile threshold."""
        raise NotImplementedError


# ┌────────────────────────────────────────────────────────────┐
# │ Space weather  « GFZ Hp30 / Kp, DLR ROTI, cached »         │
# └────────────────────────────────────────────────────────────┘


class SpaceWeatherFetcher:
    """Download and cache external space-weather indices under data_root."""

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir

    def fetch_hp30(self, start_utc: str, end_utc: str) -> pd.DataFrame:
        """Hp30 and ap30 from GFZ Potsdam (CC BY 4.0, acknowledge GFZ)."""
        raise NotImplementedError
