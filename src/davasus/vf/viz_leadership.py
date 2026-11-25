# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz_leadership                                     ║
# ║  « leader or gradient, in six panels »                           ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  First-mover shares with both nulls, the departure gap that      ║
# ║  limits resolution, Plackett-Luce worths of the full order, and  ║
# ║  initiation attempts against first places.                       ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Figure for the Dumont leadership analysis."""

from __future__ import annotations

import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from scipy.stats import spearmanr

from davasus.vf.constants import CLEAR_LEAD_MIN, TIE_TOLERANCE_MIN, VF_COLOURS
from davasus.vf.dumont import SCENARIOS, DumontResult
from davasus.viz import apply_style

apply_style()

FIGSIZE_IN: tuple[float, float] = (15.0, 9.0)
DUMONT_TOP_SHARE: float = 0.48
SCENARIO_LABELS: dict[str, str] = {"raw": "raw departures", "lag_corrected": "collar lag removed"}
SCENARIO_COLOURS: dict[str, str] = {"raw": VF_COLOURS["leader_raw"],
                                    "lag_corrected": VF_COLOURS["leader_lag"]}
NULL_COLOURS: dict[str, str] = {"egalitarian": VF_COLOURS["null_egalitarian"],
                                "graded": VF_COLOURS["null_graded"]}
DODGE: float = 0.18
LEGEND_HEADROOM: float = 1.35


def plot_dumont_leadership(result: DumontResult) -> tuple[Figure, pd.DataFrame]:
    """Six panels; returns the figure and the per-heifer table behind it."""
    fig, axes = plt.subplots(2, 3, figsize=FIGSIZE_IN, constrained_layout=True)
    table = result.per_animal.copy()
    order = (table[table["scenario"] == "raw"].sort_values("share", ascending=False)["lom"].tolist())
    _plot_shares(axes[0, 0], table, order)
    _plot_null(axes[0, 1], result.nulls, "top_share", "top first-mover share")
    _plot_null(axes[0, 2], result.nulls, "gini", "Gini of first-mover shares")
    _plot_gaps(axes[1, 0], result)
    _plot_worths(axes[1, 1], table, order)
    _plot_attempts(axes[1, 2], table)
    for ax, letter in zip(axes.flat, "ABCDEF", strict=True):
        ax.set_title(letter, loc="left", fontweight="bold")
    n = int(result.bouts["is_group_movement"].sum())
    fig.suptitle(f"Dumont (2005) leadership: first movers in {n} spontaneous group movements, "
                 f"24 heifers, 2024", fontsize="medium")
    return fig, table


def _plot_shares(ax, table: pd.DataFrame, order: list[str]) -> None:
    x = np.arange(len(order))
    for k, scenario in enumerate(SCENARIOS):
        rows = table[table["scenario"] == scenario].set_index("lom").loc[order]
        offset = (k - 0.5) * 2 * DODGE
        ax.errorbar(x + offset, rows["share"], yerr=[rows["share"] - rows["share_lo"],
                                                     rows["share_hi"] - rows["share"]],
                    fmt="o", ms=4, lw=1, color=SCENARIO_COLOURS[scenario], label=SCENARIO_LABELS[scenario])
        is_sig = rows["q_egalitarian_bh"] < 0.05
        ax.scatter(x[is_sig] + offset, rows["share_hi"][is_sig] + 0.01, marker="*",
                   color=SCENARIO_COLOURS[scenario], s=30)
    expected = table["expected_share"].mean()
    ax.axhline(expected, color="black", ls="--", lw=0.8)
    ax.text(len(order) - 0.5, expected, " equal share", va="bottom", ha="right", fontsize="x-small")
    ax.set_xticks(x, [lom[-4:] for lom in order], rotation=90, fontsize="x-small")
    ax.set_ylabel("first-mover share (ties split)")
    ax.set_ylim(0, ax.get_ylim()[1] * LEGEND_HEADROOM)
    ax.legend(fontsize="x-small", loc="upper right", framealpha=0.9,
              title=f"* BH q < 0.05; Dumont 2005: top heifer {DUMONT_TOP_SHARE:.0%}",
              title_fontsize="x-small")


def _plot_null(ax, nulls: pd.DataFrame, statistic: str, label: str) -> None:
    rows = nulls[nulls["statistic"] == statistic].reset_index(drop=True)
    for k, row in rows.iterrows():
        y = len(rows) - 1 - k
        ax.plot([row["null_lo"], row["null_hi"]], [y, y], lw=6, solid_capstyle="butt",
                color=NULL_COLOURS[row["null"]], alpha=0.6)
        ax.plot(row["observed"], y, "D", color=SCENARIO_COLOURS[row["scenario"]], ms=7)
        ax.text(row["null_hi"], y + 0.22, f" p = {row['p']:.3f}", fontsize="x-small")
    ax.set_yticks(range(len(rows))[::-1],
                  [f"{SCENARIO_LABELS[r.scenario]}\n{r.null} null" for r in rows.itertuples()],
                  fontsize="x-small")
    ax.set_xlabel(f"{label} (bar: null 95 %, diamond: observed)")
    ax.set_ylim(-0.6, len(rows) - 0.3)


def _plot_gaps(ax, result: DumontResult) -> None:
    gaps = result.per_bout.loc[result.per_bout["scenario"] == "raw", "gap_min"].dropna()
    lag_span = (result.lags["lag_s"].max() - result.lags["lag_s"].min()) / 60.0
    ax.axvspan(0, lag_span, color=VF_COLOURS["null_graded"], alpha=0.15, label="collar lag span")
    ax.hist(gaps, bins=np.arange(0, gaps.max() + 0.25, 0.125), color=VF_COLOURS["leader_raw"])
    ax.axvline(TIE_TOLERANCE_MIN, color="black", ls="--", lw=0.8, label="tie tolerance")
    ax.axvline(CLEAR_LEAD_MIN, color="black", ls=":", lw=0.8, label="clear lead (1 fix)")
    tied = (gaps < TIE_TOLERANCE_MIN).mean()
    ax.set_xlabel("first to second departure (min)")
    ax.set_ylabel("group movements")
    ax.legend(fontsize="x-small", frameon=False, title=f"{tied:.0%} tied", title_fontsize="x-small")


def _plot_worths(ax, table: pd.DataFrame, order: list[str]) -> None:
    x = np.arange(len(order))
    for k, scenario in enumerate(SCENARIOS):
        rows = table[table["scenario"] == scenario].set_index("lom").loc[order]
        ax.errorbar(x + (k - 0.5) * 2 * DODGE, rows["pl_order"],
                    yerr=[rows["pl_order"] - rows["pl_order_lo"], rows["pl_order_hi"] - rows["pl_order"]],
                    fmt="o", ms=4, lw=1, color=SCENARIO_COLOURS[scenario])
    ax.axhline(1.0 / len(order), color="black", ls="--", lw=0.8)
    ax.set_xticks(x, [lom[-4:] for lom in order], rotation=90, fontsize="x-small")
    ax.set_ylabel("Plackett-Luce worth, full order\n(P first if all present)")


def _plot_attempts(ax, table: pd.DataFrame) -> None:
    for scenario in SCENARIOS:
        rows = table[table["scenario"] == scenario]
        with np.errstate(invalid="ignore"), warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rho, p = spearmanr(rows["group_success_rate"], rows["share"])
        ax.scatter(rows["group_success_rate"], rows["share"], s=18, color=SCENARIO_COLOURS[scenario],
                   label=rf"{SCENARIO_LABELS[scenario]}: $\rho_s$ = {rho:.2f}, p = {p:.2f}")
    ax.set_xlabel("attempts followed by half the herd (share)")
    ax.set_ylabel("first-mover share")
    ax.set_ylim(top=ax.get_ylim()[1] * LEGEND_HEADROOM)
    ax.legend(fontsize="x-small", loc="upper left", framealpha=0.9)
