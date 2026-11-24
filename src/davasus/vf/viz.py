# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz                                                ║
# ║  « figures for M8, triple output »                               ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Every figure writes PNG, SVG with editable text and CSV into    ║
# ║  figures/08_vf_leadership/ (D5).                                 ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Figures for the virtual-fence analysis."""

from __future__ import annotations

import pandas as pd


class VFFigureMaker:
    """Plots for latency, leadership and learning."""

    def plot_learning_curves(self, curves: pd.DataFrame) -> None:
        """Per-animal SR against exposure, with fitted curves."""
        raise NotImplementedError
