# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.leadership                                         ║
# ║  « who goes first, and who follows whom »                        ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Q3: normalised exit ranks with Kendall's W, the follower        ║
# ║  network, and front-back position on the travel axis.            ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Leadership measures (handover 4.3 and 4.8)."""

from __future__ import annotations

import pandas as pd


class EntryOrderAnalyser:
    """Normalised exit rank per animal and event; Kendall's W."""

    def ranks(self, latencies: pd.DataFrame) -> pd.DataFrame:
        """r = (rank - 1) / (n - 1), ties averaged."""
        raise NotImplementedError

    def kendall_w(self, ranks: pd.DataFrame, n_permutations: int) -> tuple[float, float]:
        """W and its permutation p-value (IDs shuffled within events)."""
        raise NotImplementedError


class FollowerNetworkBuilder:
    """Directed i -> j edges when j crosses within the window after i."""

    def build(self, latencies: pd.DataFrame, window_min: int) -> pd.DataFrame:
        """Edge list with counts."""
        raise NotImplementedError


class HerdPositionAnalyser:
    """Front-back and lateral position on the herd travel vector."""

    def positions(self, fixes: pd.DataFrame) -> pd.DataFrame:
        """Per animal and minute: projection, lateral offset, NN distance."""
        raise NotImplementedError
