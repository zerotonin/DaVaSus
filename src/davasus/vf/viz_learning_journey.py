# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz_learning_journey                               ║
# ║  « two axes and a social route »                                 ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  A: how often a heifer interacts with the fence against how      ║
# ║  often it ends in a pulse. B: the pulsed share over her own      ║
# ║  interaction count. C: before and after a fence move. D: what    ║
# ║  an uncued heifer does after a herd mate's cue. E: that          ║
# ║  response against her own exposure. F: sociability against how   ║
# ║  often she is the first to be cued.                              ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Six-panel figure for the learning journey of each heifer."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.ticker import NullFormatter
from scipy.stats import spearmanr

from davasus.vf.constants import (
    APPROACH_FROM_M,
    CONTEXT_RANGE_M,
    FENCE_MOVE_WINDOW_H,
    LEARNING_BLOCK,
    SOCIAL_CUE_RANGE_M,
    SOCIAL_RESPONSE_MIN,
    VF_COLOURS,
    WONG_BLUE,
    WONG_GREY,
    WONG_ORANGE,
    WONG_SKY,
    WONG_VERMILLION,
)
from davasus.vf.learning_journey import STRATEGIES, LearningJourneyResult
from davasus.vf.viz_pull_anchor import format_p
from davasus.viz import apply_style

apply_style()

FIGSIZE_IN: tuple[float, float] = (19.0, 11.0)
STRATEGY_COLOURS: dict[str, str] = dict(zip(STRATEGIES, (WONG_BLUE, WONG_ORANGE, WONG_SKY, WONG_VERMILLION),
                                            strict=True))
LABEL_OFFSET: tuple[int, int] = (0, 6)
N_LABELLED_CURVES: int = 2


def plot_learning_journey(result: LearningJourneyResult) -> tuple[Figure, pd.DataFrame]:
    """The six panels; returns the figure and the per-heifer table behind it."""
    table = result.per_heifer
    fig, axes = plt.subplots(2, 3, figsize=FIGSIZE_IN, constrained_layout=True)
    _plot_axes(axes[0, 0], table)
    _plot_curves(axes[0, 1], result.curves, table)
    _plot_paired(axes[0, 2], table, "pulsed_share_before_move", "pulsed_share_after_move",
                 (f"{FENCE_MOVE_WINDOW_H} h before", f"{FENCE_MOVE_WINDOW_H} h after"),
                 "share of interactions with a pulse", _describe(result.tests, "fence move"))
    axes[0, 2].set_title("C  Around a fence move", loc="left", fontsize="medium")
    _plot_paired(axes[1, 0], table, "retreat_quiet", "retreat_mate_cued", ("herd quiet", "mate cued"),
                 f"left the zone uncued within {SOCIAL_RESPONSE_MIN} min", _describe(result.tests, "retreat"))
    axes[1, 0].set_title(f"D  Uncued heifer in the zone, company within {SOCIAL_CUE_RANGE_M:.0f} m", loc="left",
                         fontsize="medium")
    _plot_relation(axes[1, 1], table, "interactions_per_day", "retreat_gain", "fence interactions per day",
                   "retreat after a mate's cue minus herd quiet", is_log=True)
    axes[1, 1].set_title("E  Who uses a herd mate's cue", loc="left", fontsize="medium")
    _plot_relation(axes[1, 2], table, "association", "first_cue_share",
                   "association (share of time among a mate's 3 nearest)", "share of own interactions as first cue")
    axes[1, 2].set_title("F  Sociability and being first at the fence", loc="left", fontsize="medium")
    fig.legend(handles=[Line2D([], [], marker="o", linestyle="", color=colour, markeredgecolor="black", label=name)
                        for name, colour in STRATEGY_COLOURS.items() if name in set(table["strategy"])],
               loc="outside lower center", ncols=4, frameon=False, fontsize="small")
    return fig, table


def _describe(tests: pd.DataFrame, key: str) -> str:
    row = tests[tests["test"].str.contains(key)].iloc[0]
    return f"mean shift {row['mean_shift']:+.2f}, {format_p(row['p_two_sided'])} (sign flips, n = {row['n_heifers']:.0f})"


def _colours(table: pd.DataFrame) -> pd.Series:
    return table["strategy"].map(STRATEGY_COLOURS)


def _label(ax, table: pd.DataFrame, x: str, y: str) -> None:
    for _, row in table.dropna(subset=[x, y]).iterrows():
        ax.annotate(row["lom"][-4:], (row[x], row[y]), xytext=LABEL_OFFSET, textcoords="offset points",
                    ha="center", fontsize="xx-small")


def _plot_axes(ax, table: pd.DataFrame) -> None:
    ax.scatter(table["interactions_per_day"], table["pulsed_share"], s=30 + 12 * table["tones_per_interaction"] ** 2,
               color=_colours(table), edgecolor="black", linewidth=0.5, alpha=0.85)
    _label(ax, table, "interactions_per_day", "pulsed_share")
    rho, p = spearmanr(table["interactions_per_day"], table["pulsed_share"])
    ax.set_xscale("log")
    ax.set_ylim(0, 1)
    ax.set_xlabel("fence interactions per day (risk taking)")
    ax.set_ylabel("share of interactions with a pulse (conditioning)")
    ax.set_title(rf"A  Two axes: $\rho$ = {rho:.2f} ({format_p(p)}); marker area: tones per interaction", loc="left",
                 fontsize="medium")


def _plot_curves(ax, curves: pd.DataFrame, table: pd.DataFrame) -> None:
    strategy = table.set_index("lom")["strategy"]
    top = set(table.nlargest(N_LABELLED_CURVES, "n_interactions")["lom"])
    for lom, curve in curves.groupby("lom"):
        is_top = lom in top
        ax.plot(curve["last_number"], curve["pulsed_share"], color=STRATEGY_COLOURS[strategy[lom]],
                linewidth=1.8 if is_top else 0.8, alpha=1.0 if is_top else 0.55)
        if is_top:
            ax.annotate(lom[-4:], (curve["last_number"].iloc[-1], curve["pulsed_share"].iloc[-1]), xytext=(4, 0),
                        textcoords="offset points", fontsize="x-small", va="center")
    ax.set_xscale("log")
    ax.set_ylim(0, 1)
    ax.set_xlabel(f"own interaction number (blocks of {LEARNING_BLOCK})")
    ax.set_ylabel("share of interactions with a pulse")
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.set_title("B  Pulsed share over each heifer's own interactions", loc="left", fontsize="medium")


def _plot_paired(ax, table: pd.DataFrame, before: str, after: str, labels: tuple[str, str], ylabel: str,
                 note: str, is_share: bool = True) -> None:
    pairs = table.dropna(subset=[before, after])
    for (_, row), colour in zip(pairs.iterrows(), _colours(pairs), strict=True):
        ax.plot([0, 1], [row[before], row[after]], color=colour, marker="o", markersize=4, linewidth=0.9, alpha=0.8)
    ax.set_xticks([0, 1], labels)
    ax.set_xlim(-0.3, 1.3)
    ax.set_ylim(0, 1 if is_share else None)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(note)


def _plot_relation(ax, table: pd.DataFrame, x: str, y: str, xlabel: str, ylabel: str, is_log: bool = False) -> None:
    ax.scatter(table[x], table[y], s=45, color=_colours(table), edgecolor="black", linewidth=0.5, alpha=0.85)
    _label(ax, table, x, y)
    ok = table[[x, y]].notna().all(axis=1)
    rho, p = spearmanr(table.loc[ok, x], table.loc[ok, y])
    if is_log:
        ax.set_xscale("log")
    ax.axhline(0.0, color=WONG_GREY, linewidth=0.6)
    ax.set_xlabel(rf"{xlabel}; $\rho$ = {rho:.2f} ({format_p(p)})")
    ax.set_ylabel(ylabel)
    if np.nanmin(table[y]) >= 0:
        ax.set_ylim(0, None)


def plot_fence_context(result: LearningJourneyResult) -> tuple[Figure, pd.DataFrame]:
    """Where the tone and the pulse fall, and the pulsed share by approach, line and herd context."""
    table, tests = result.per_heifer, result.tests
    fig, axes = plt.subplots(2, 3, figsize=FIGSIZE_IN, constrained_layout=True)
    _plot_distance_profile(axes[0, 0], result.distance_profile)
    panels = (
        (axes[0, 1], "pulsed_share_at_line", "pulsed_share_approach", ("already at the line", "approach"),
         "share of interactions with a pulse", "approach minus", True,
         f"B  Came from at least {APPROACH_FROM_M:.0f} m inside, or was at the tone line"),
        (axes[0, 2], "pulsed_share_same_line", "pulsed_share_new_line", ("line kept", "new line"),
         "share of interactions with a pulse", "new line minus line kept", True, "C  Tone or place: a line where none was"),
        (axes[1, 1], "centre_distance_m_tone_only", "centre_distance_m_pulsed", ("tone only", "pulsed"),
         "distance to the herd centre at onset (m)", "herd centre", False, "E  Pulsed away from the herd"),
        (axes[1, 2], "n_mates_near_tone_only", "n_mates_near_pulsed", ("tone only", "pulsed"),
         f"herd mates within {CONTEXT_RANGE_M:.0f} m at onset", "herd mates within", False,
         "F  Pulsed with fewer mates around"))
    for ax, before, after, labels, ylabel, key, is_share, title in panels:
        _plot_paired(ax, table, before, after, labels, ylabel, _describe(tests, key), is_share)
        ax.set_title(title, loc="left", fontsize="medium")
    _plot_depth(axes[1, 0], result.interactions, table)
    fig.legend(handles=[Line2D([], [], marker="o", linestyle="", color=colour, markeredgecolor="black", label=name)
                        for name, colour in STRATEGY_COLOURS.items() if name in set(table["strategy"])],
               loc="outside lower center", ncols=4, frameon=False, fontsize="small")
    return fig, result.distance_profile


def _plot_distance_profile(ax, profile: pd.DataFrame) -> None:
    edges = np.r_[profile["from_m"].to_numpy(), profile["to_m"].iloc[-1]]
    for column, colour, label in (("tone_share", VF_COLOURS["audio"], "tone"), ("pulse_share", VF_COLOURS["pulse"],
                                                                                "pulse")):
        ax.stairs(profile[column], edges, color=colour, linewidth=1.8, label=label)
    ax.axvline(0.0, color="black", linewidth=0.8)
    ax.set_ylim(0, 1)
    ax.set_xlabel("largest distance to the fence in the minute (m; negative: inside)")
    ax.set_ylabel("share of fixes")
    ax.legend(frameon=False, fontsize="small", loc="upper left")
    ax.set_title("A  The tone starts 5 m before the line", loc="left", fontsize="medium")


def _plot_depth(ax, interactions: pd.DataFrame, table: pd.DataFrame) -> None:
    strategy = interactions["lom"].map(table.set_index("lom")["strategy"])
    for name, colour in STRATEGY_COLOURS.items():
        depth = np.sort(interactions.loc[strategy == name, "depth_m"].dropna().to_numpy())
        if len(depth):
            ax.plot(depth, 1.0 - np.arange(1, len(depth) + 1) / len(depth), color=colour, linewidth=1.6)
    ax.axvline(0.0, color="black", linewidth=0.8)
    ax.set_xlim(-5.2, 5.0)
    ax.set_yscale("log")
    ax.set_xlabel("deepest point of the interaction (m from the fence line)")
    ax.set_ylabel("share of interactions that go deeper")
    ax.set_title("D  How far past the tone line", loc="left", fontsize="medium")
