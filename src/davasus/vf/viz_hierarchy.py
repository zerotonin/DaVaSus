# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz_hierarchy                                      ║
# ║  « what the fence costs each rank »                              ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Left: rank against pulses and audio cues over the trial, one    ║
# ║  stem per heifer. Right: pulses per audio cue against the        ║
# ║  exploration rank, the part of the hierarchy that drives the     ║
# ║  cue counts.                                                     ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Figure for the combined hierarchy and its cue totals."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from scipy.stats import spearmanr

from davasus.vf.constants import WONG_BLACK, WONG_BLUE, WONG_GREY, WONG_SKY
from davasus.vf.hierarchy import TIERS
from davasus.vf.viz_pull_anchor import format_p
from davasus.viz import apply_style

apply_style()

FIGSIZE_IN: tuple[float, float] = (17.0, 8.0)
VIEW_ELEVATION_DEG: float = 22.0
VIEW_AZIMUTH_DEG: float = -60.0
TIER_COLOURS: dict[str, str] = dict(zip(TIERS, (WONG_BLUE, WONG_SKY, WONG_GREY, WONG_BLACK), strict=True))
LABEL_LIFT: float = 0.03


def plot_hierarchy_cues(hierarchy: pd.DataFrame) -> tuple[Figure, pd.DataFrame]:
    """3D stems (rank, pulses, audio) and the pulse-per-audio ratio by exploration rank."""
    fig = plt.figure(figsize=FIGSIZE_IN, constrained_layout=True)
    _plot_stems(fig.add_subplot(1, 2, 1, projection="3d"), hierarchy)
    _plot_ratio(fig.add_subplot(1, 2, 2), hierarchy)
    return fig, hierarchy


def _plot_stems(ax, hierarchy: pd.DataFrame) -> None:
    colours = hierarchy["tier"].map(TIER_COLOURS)
    x, y, z = hierarchy["rank"], hierarchy["pulse_total"], hierarchy["audio_total"]
    for xi, yi, zi, colour in zip(x, y, z, colours, strict=True):
        ax.plot([xi, xi], [yi, yi], [0, zi], color=colour, linewidth=1.0, alpha=0.7)
    ax.scatter(x, y, 0, color=colours, s=12, alpha=0.35, depthshade=False)
    ax.scatter(x, y, z, color=colours, s=55, edgecolor="black", linewidth=0.5, depthshade=False)
    for xi, yi, zi, lom in zip(x, y, z, hierarchy["lom"], strict=True):
        ax.text(xi, yi, zi + LABEL_LIFT * z.max(), lom[-4:], fontsize=7, ha="center")
    ax.set_xlabel("hierarchy rank (1 = leads most)")
    ax.set_ylabel("electric pulses (sum)")
    ax.set_zlabel("audio cues (sum)")
    ax.set_xlim(0.5, len(hierarchy) + 0.5)
    ax.set_ylim(0, None)
    ax.set_zlim(0, None)
    ax.view_init(elev=VIEW_ELEVATION_DEG, azim=VIEW_AZIMUTH_DEG)
    ax.legend(handles=[Line2D([], [], marker="o", linestyle="", color=colour, markeredgecolor="black", label=tier)
                       for tier, colour in TIER_COLOURS.items()], loc="upper left", frameon=False, fontsize="small")
    rho_audio, p_audio = spearmanr(x, z)
    rho_pulse, p_pulse = spearmanr(x, y)
    ax.set_title(rf"A  rank vs audio $\rho$ = {rho_audio:.2f} ({format_p(p_audio)}); "
                 rf"vs pulses $\rho$ = {rho_pulse:.2f} ({format_p(p_pulse)})", loc="left", fontsize="medium")


def _plot_ratio(ax, hierarchy: pd.DataFrame) -> None:
    colours = hierarchy["tier"].map(TIER_COLOURS)
    ax.scatter(hierarchy["exploration_rank"], hierarchy["pulse_per_audio"], s=20 + hierarchy["audio_total"] / 25.0,
               color=colours, edgecolor="black", linewidth=0.5, alpha=0.85)
    for _, row in hierarchy.iterrows():
        ax.annotate(row["lom"][-4:], (row["exploration_rank"], row["pulse_per_audio"]), xytext=(0, 8),
                    textcoords="offset points", ha="center", fontsize="x-small")
    rho, p = spearmanr(hierarchy["exploration_rank"], hierarchy["pulse_per_audio"], nan_policy="omit")
    ax.set_xlabel("exploration rank (mean of fence, new paddock, retreat; 1 = explores most)")
    ax.set_ylabel("pulses per audio cue over the trial")
    ax.set_ylim(0, None)
    ax.set_title(rf"B  Spearman $\rho$ = {rho:.2f} ({format_p(p)}); marker area: audio cues", loc="left",
                 fontsize="medium")
