# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz_leadership_situations                          ║
# ║  « shade, fence retreat and turns in six panels »                ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  First-arrival shares into the shade, when the herd goes there,  ║
# ║  first-exit shares from the fence zone, whether the others       ║
# ║  follow a cued exit, turn scores against collar lag, and         ║
# ║  agreement of all leadership measures.                           ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Figure for the shade, fence-retreat and turn analyses."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from scipy.stats import spearmanr

from davasus.vf.constants import (
    SHADE_HOT_THI,
    VF_COLOURS,
    WONG_BLUE,
    WONG_GREEN,
    WONG_ORANGE,
    WONG_PINK,
    WONG_VERMILLION,
)
from davasus.vf.leadership_situations import LeadershipSituationsResult
from davasus.vf.viz_leadership_context import plot_agreement
from davasus.vf.viz_pull_anchor import SCENARIO_COLOURS, SCENARIO_LABELS, format_p
from davasus.viz import apply_style

apply_style()

FIGSIZE_IN: tuple[float, float] = (17.0, 10.5)
DODGE: float = 0.18
LOCAL_TZ: str = "Europe/Berlin"
PATCH_COLOURS: tuple[str, ...] = (WONG_ORANGE, WONG_BLUE, WONG_GREEN, WONG_PINK)
TURN_COLOURS: dict[str, str] = {"raw": VF_COLOURS["leader_raw"], "lag_residual": WONG_VERMILLION}
TURN_LABELS: dict[str, str] = {"raw": "raw", "lag_residual": "lag trend removed"}


def plot_leadership_situations(result: LeadershipSituationsResult,
                               agreement: pd.DataFrame) -> tuple[Figure, pd.DataFrame]:
    """Six panels plus the per-heifer table behind them."""
    fig, axes = plt.subplots(2, 3, figsize=FIGSIZE_IN, constrained_layout=True)
    _plot_shares(axes[0, 0], result.shade_per_heifer, result.shade_nulls, result.shade_w,
                 "share of shade arrivals first in")
    _plot_shade_timing(axes[0, 1], result.shade_episodes)
    _plot_shares(axes[0, 2], result.retreat_per_heifer, result.retreat_nulls, result.retreat_w,
                 "share of fence approaches first out of the zone")
    _plot_follow(axes[1, 0], result)
    _plot_turns(axes[1, 1], result)
    plot_agreement(axes[1, 2], agreement)
    for ax, letter in zip(axes.flat, "ABCDEF", strict=True):
        ax.set_title(letter, loc="left", fontweight="bold")
    n_shade = int(result.shade_episodes["is_daytime"].sum())
    n_retreat = result.retreats["bout_id"].nunique()
    n_pairs = int(result.turn_test["n_pairs"].iloc[0])
    fig.suptitle(f"Leadership into the shade ({n_shade} daytime arrivals), away from the fence "
                 f"({n_retreat} approaches) and through turns ({n_pairs} pairs); 24 heifers, 2024",
                 fontsize="medium")
    return fig, result.per_heifer


def _plot_shares(ax, per_animal: pd.DataFrame, nulls: pd.DataFrame, order_w: pd.DataFrame,
                 ylabel: str) -> None:
    raw = per_animal[per_animal["scenario"] == "raw"].set_index("lom")
    order = raw.sort_values("share", ascending=False).index.tolist()
    x = np.arange(len(order))
    for k, scenario in enumerate(("raw", "lag_corrected")):
        rows = per_animal[per_animal["scenario"] == scenario].set_index("lom").loc[order]
        ax.errorbar(x + (k - 0.5) * 2 * DODGE, rows["share"],
                    yerr=[rows["share"] - rows["share_lo"], rows["share_hi"] - rows["share"]],
                    fmt="o", ms=4, lw=1, color=SCENARIO_COLOURS[scenario], label=SCENARIO_LABELS[scenario])
    ax.axhline(raw["expected_share"].mean(), color="black", lw=0.8, ls="--", label="equal chance")
    ax.set_xticks(x, [lom[-4:] for lom in order], rotation=90, fontsize="x-small")
    ax.set_ylabel(ylabel)
    lines = []
    for scenario in [s for s in ("raw", "raw_hot") if s in set(nulls["scenario"])]:
        gini = nulls[(nulls["statistic"] == "gini") & (nulls["scenario"] == scenario)].set_index("null")
        w = order_w.set_index("scenario").loc[scenario]
        label = "hot only" if scenario.endswith("_hot") else "all"
        lines.append(f"{label}: Gini {gini.loc['egalitarian', 'observed']:.2f} vs equal "
                     f"{gini.loc['egalitarian', 'null_mean']:.2f} ({format_p(gini.loc['egalitarian', 'p'])}); "
                     f"ties {w['tie_fraction']:.0%}")
    ax.text(0.98, 0.70, "\n".join(lines), transform=ax.transAxes, ha="right", va="top", fontsize="x-small")
    ax.legend(fontsize="x-small", loc="upper right", framealpha=0.9)


def _plot_shade_timing(ax, episodes: pd.DataFrame) -> None:
    local = episodes["onset_utc"].dt.tz_convert(LOCAL_TZ)
    hour = local.dt.hour + local.dt.minute / 60.0
    for colour, (patch, rows) in zip(PATCH_COLOURS, episodes.groupby("patch"), strict=False):
        index = rows.index
        ax.scatter(hour[index], rows["thi"], s=12 + rows["rise_min"], color=colour, alpha=0.7,
                   edgecolor=np.where(rows["is_daytime"], "black", "none"), label=patch)
    ax.axhline(SHADE_HOT_THI, color="black", lw=0.8, ls="--", label=f"THI {SHADE_HOT_THI:g} (heat stress)")
    ax.set_xlim(0, 24)
    ax.set_xticks(range(0, 25, 3))
    ax.set_xlabel("arrival time (local); marker size: minutes from 10 % to 50 % of herd inside")
    ax.set_ylabel("THI at arrival")
    ax.legend(fontsize="x-small", loc="lower right", framealpha=0.9, title="black edge: daytime",
              title_fontsize="x-small")


def _plot_follow(ax, result: LeadershipSituationsResult) -> None:
    retreats = result.retreats
    rng = np.random.default_rng(0)
    for k, (flag, label, colour) in enumerate(((False, "first exit\nnot cued", WONG_BLUE),
                                               (True, "first exit\ncued", WONG_ORANGE))):
        values = retreats.loc[retreats["first_exit_cued"] == flag, "follow_min"].to_numpy()
        ax.scatter(k + rng.uniform(-0.15, 0.15, len(values)), values, s=8, alpha=0.4, color=colour)
        ax.boxplot(values, positions=[k + 0.3], widths=0.12, showfliers=False)
        ax.text(k, -0.08, f"{label} (n = {len(values)})", transform=ax.get_xaxis_transform(),
                ha="center", va="top", fontsize="x-small")
    ax.set_xticks([])
    ax.set_yscale("symlog", linthresh=10)
    ax.set_ylabel("others' median exit after the first exit (min)")
    tests = result.retreat_tests.set_index("test")
    cue = tests.loc["others' follow time: cued vs uncued first exit"]
    order = tests.loc["entry order vs exit order"]
    ax.text(0.98, 0.97, f"cued vs not: {cue['difference']:+.1f} min, {format_p(cue['p_two_sided'])}\n"
            f"entry vs exit order: mean rho {order['mean_rho']:.2f}, {format_p(order['p_two_sided'])}\n"
            f"first out was first in: {retreats['first_out_was_first_in'].mean():.0%}; "
            f"its dwell median {retreats['first_exiter_dwell_min'].median():.0f} min",
            transform=ax.transAxes, ha="right", va="top", fontsize="x-small")


def _plot_turns(ax, result: LeadershipSituationsResult) -> None:
    scores = result.turn_scores
    for scenario, colour in TURN_COLOURS.items():
        rows = scores[scores["scenario"] == scenario]
        rho, p = spearmanr(rows["lag_s"], rows["turn_lead_min"], nan_policy="omit")
        ax.errorbar(rows["lag_s"], rows["turn_lead_min"],
                    yerr=[rows["turn_lead_min"] - rows["turn_lead_lo"], rows["turn_lead_hi"] - rows["turn_lead_min"]],
                    fmt="o", ms=4, lw=0.8, color=colour,
                    label=rf"{TURN_LABELS[scenario]}: $\rho_s$ with lag = {rho:.2f}, {format_p(p)}")
    lag = np.linspace(scores["lag_s"].min(), scores["lag_s"].max(), 2)
    ax.plot(lag, lag / 60.0 - np.nanmean(scores["lag_s"]) / 60.0, color="black", lw=0.8, ls=":",
            label="expected from lag alone (full correction)")
    ax.axhline(0, color="black", lw=0.8)
    test = result.turn_test.iloc[0]
    ax.set_xlabel("collar lag estimated from travel offsets (s)")
    ax.set_ylabel("turn score: mean delay of the others (min)")
    ax.legend(fontsize="x-small", loc="upper left", framealpha=0.9,
              title=f"score SD {test['sd_score']:.2f} vs time-reversed {test['null_mean']:.2f} "
                    f"({format_p(test['p'])})", title_fontsize="x-small")
