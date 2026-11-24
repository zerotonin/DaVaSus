# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.exploration                                        ║
# ║  « who tests the fence, who stays close »                        ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Q3: stimulus clusters per hour in the pre-fence zone, approach  ║
# ║  rate, breakouts and sociability outside search phases.          ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Fence-testing and sociability scores (handover 4.4)."""

from __future__ import annotations

import pandas as pd


class FenceTestingScorer:
    """Per-animal fence-testing, breakout and sociability scores."""

    def score(self, fixes: pd.DataFrame) -> pd.DataFrame:
        """One row per animal."""
        raise NotImplementedError
