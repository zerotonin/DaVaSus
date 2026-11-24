# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.latency                                            ║
# ║  « how fast does the herd find the new parcel »                  ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Q1 and Q2: per-animal duration of the Outside == 3 search       ║
# ║  phase after each activation (right-censored), herd t50.         ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Crossing latency and herd occupancy (handover 4.2)."""

from __future__ import annotations

import pandas as pd


class CrossingLatencyEstimator:
    """Per-animal search-phase latency per activation event."""

    def estimate(self, fixes: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
        """One row per animal and event: latency_min, is_censored."""
        raise NotImplementedError

    def herd_occupancy(self, latencies: pd.DataFrame) -> pd.DataFrame:
        """Fraction of animals out of the cut-off parcel over time, and t50."""
        raise NotImplementedError
