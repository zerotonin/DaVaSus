# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz_leadership_context                             ║
# ║  « switches, fences, departures in six panels »                  ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Entry order into new paddocks, first at the fence, cue rate by  ║
# ║  arrival position, activity around departures, restlessness per  ║
# ║  heifer, and agreement of all leadership measures.               ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Figure for the leadership-in-context analyses."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

from davasus.vf.constants import (
    PREDEPARTURE_BASELINE_MIN,
    PREDEPARTURE_WINDOW_MIN,
    VF_COLOURS,
    WONG_BLUE,
    WONG_GREY,
    WONG_ORANGE,
    WONG_VERMILLION,
)
from davasus.vf.leadership_context import LEADING_MEASURES, LeadershipContextResult
from davasus.vf.viz_pull_anchor import SCENARIO_COLOURS, SCENARIO_LABELS, format_p
from davasus.viz import apply_style

apply_style()

FIGSIZE_IN: tuple[float, float] = (17.0, 10.5)
DODGE: float = 0.18
PROFILE_COLOURS: dict[str, str] = {"first movers": WONG_VERMILLION, "joiners": WONG_BLUE, "stayed": WONG_GREY}
AGREEMENT_CMAP: str = "RdBu_r"
N_LABELLED_ENTRANTS: int = 3
WILSON_Z: float = 1.96


def plot_leadership_context(result: LeadershipContextResult,
                            agreement: pd.DataFrame) -> tuple[Figure, pd.DataFrame]:
    """Six panels plus the per-heifer table behind them."""
    fig, axes = plt.subplots(2, 3, figsize=FIGSIZE_IN, constrained_layout=True)
    _plot_paddock_entry(axes[0, 0], result)
    _plot_fence_share(axes[0, 1], result)
    _plot_cue_by_position(axes[0, 2], result)
    _plot_profiles(axes[1, 0], result)
    _plot_restlessness(axes[1, 1], result)
    plot_agreement(axes[1, 2], agreement)
    for ax, letter in zip(axes.flat, "ABCDEF", strict=True):
        ax.set_title(letter, loc="left", fontweight="bold")
    n_fence = int(result.fence_episodes["is_group_movement"].sum())
    n_bouts = result.predeparture["bout_id"].nunique()
    n_switches = int(result.switches["is_ranked"].sum())
    fig.suptitle(f"Leadership in context: {n_switches} paddock switches, {n_fence} fence approaches, "
                 f"{n_bouts} group movements; 24 heifers, 2024", fontsize="medium")
    return fig, result.per_heifer


def _plot_paddock_entry(ax, result: LeadershipContextResult) -> None:
    ranked = result.switches[result.switches["is_ranked"]].reset_index(drop=True)
    entries = result.paddock_entries
    for k, switch in ranked.iterrows():
        rows = entries[(entries["bout_id"] == switch.event_id) & entries["has_joined"]]
        delay = rows["departure_min"] - rows["departure_min"].min()
        jitter = np.random.default_rng(k).uniform(-DODGE, DODGE, len(rows))
        ax.scatter(k + jitter, delay, s=16, color=WONG_BLUE, alpha=0.7)
        early = rows.assign(delay=delay).nsmallest(N_LABELLED_ENTRANTS, "departure_min")
        for value, tied in early.groupby("delay"):
            ax.annotate(", ".join(lom[-4:] for lom in tied["lom"]), (k + DODGE, value),
                        xytext=(6, 0), textcoords="offset points", fontsize="xx-small", va="center")
    labels = [f"{s.label}\n-> {s.new_paddock}, +{s.first_entry_min:.0f} min" for s in ranked.itertuples()]
    ax.set_xticks(range(len(ranked)), labels, fontsize="x-small")
    ax.set_ylabel("entry after the first heifer (min)")
    raw = result.paddock_w[result.paddock_w["scenario"] == "raw"]
    if len(raw):
        ax.text(0.02, 0.97, f"Kendall W = {raw['w'].iloc[0]:.2f} (null {raw['null_mean'].iloc[0]:.2f}), "
                f"{format_p(raw['p'].iloc[0])}\nonly {len(ranked)} switches: a case study",
                transform=ax.transAxes, va="top", fontsize="x-small")
    excluded = result.switches[~result.switches["is_ranked"]]
    if len(excluded):
        ax.set_xlabel("not ranked: " + ", ".join(
            f"{s.label} ({'manual herding' if s.manual_herding else 'paddock already open'})"
            for s in excluded.itertuples()), fontsize="x-small")


def _plot_fence_share(ax, result: LeadershipContextResult) -> None:
    table = result.fence_per_heifer
    raw = table[table["scenario"] == "raw"].set_index("lom")
    order = raw.sort_values("share", ascending=False).index.tolist()
    x = np.arange(len(order))
    for k, scenario in enumerate(("raw", "lag_corrected")):
        rows = table[table["scenario"] == scenario].set_index("lom").loc[order]
        ax.errorbar(x + (k - 0.5) * 2 * DODGE, rows["share"],
                    yerr=[rows["share"] - rows["share_lo"], rows["share_hi"] - rows["share"]],
                    fmt="o", ms=4, lw=1, color=SCENARIO_COLOURS[scenario], label=SCENARIO_LABELS[scenario])
    ax.axhline(raw["expected_share"].mean(), color="black", lw=0.8, ls="--", label="equal chance")
    ax.set_xticks(x, [lom[-4:] for lom in order], rotation=90, fontsize="x-small")
    ax.set_ylabel("share of fence approaches entered first")
    gini = result.fence_nulls[(result.fence_nulls["statistic"] == "gini")
                              & (result.fence_nulls["scenario"] == "raw")].set_index("null")
    w = result.fence_w.set_index("scenario").loc["raw"]
    ax.text(0.98, 0.70, f"Gini {gini.loc['egalitarian', 'observed']:.2f} vs equal "
            f"{gini.loc['egalitarian', 'null_mean']:.2f} ({format_p(gini.loc['egalitarian', 'p'])})\n"
            f"vs graded {gini.loc['graded', 'null_mean']:.2f} ({format_p(gini.loc['graded', 'p'])})\n"
            f"entry order W = {w['w']:.2f}, {format_p(w['p'])}",
            transform=ax.transAxes, ha="right", va="top", fontsize="x-small")
    ax.legend(fontsize="x-small", loc="upper right", framealpha=0.9)


def _plot_cue_by_position(ax, result: LeadershipContextResult) -> None:
    table = result.fence_cue_by_position
    lo, hi = wilson_interval(table["cue_rate"].to_numpy(), table["n_entries"].to_numpy())
    x = np.arange(len(table))
    ax.errorbar(x, table["cue_rate"], yerr=[table["cue_rate"] - lo, hi - table["cue_rate"]],
                fmt="o-", ms=6, lw=1.2, color=VF_COLOURS["audio"])
    ax.set_xticks(x, [f"{p}\n(n = {n})" for p, n in zip(table["position_bin"], table["n_entries"], strict=True)],
                  fontsize="x-small")
    ax.set_xlabel("arrival position in the fence approach")
    ax.set_ylabel("share cued (audio or pulse) within 5 min of entry")
    tests = result.fence_cue_tests.set_index("test")
    lines = []
    for name, label in (("first vs later entrants", "first vs later"),
                        ("first vs following, within heifer", "same heifer, first vs following"),
                        ("followers after cued vs uncued leader", "followers: leader cued vs not")):
        if name in tests.index:
            row = tests.loc[name]
            lines.append(f"{label}: {row['difference']:+.2f}, {format_p(row['p_two_sided'])}")
    ax.text(0.98, 0.97, "\n".join(lines), transform=ax.transAxes, ha="right", va="top", fontsize="x-small")
    ax.set_ylim(0, max(0.6, float(np.nanmax(hi)) + 0.15))


def wilson_interval(rate: np.ndarray, n: np.ndarray, z: float = WILSON_Z) -> tuple[np.ndarray, np.ndarray]:
    """Wilson score interval for binomial proportions."""
    centre = (rate + z ** 2 / (2 * n)) / (1 + z ** 2 / n)
    half = z * np.sqrt(rate * (1 - rate) / n + z ** 2 / (4 * n ** 2)) / (1 + z ** 2 / n)
    return centre - half, centre + half


def _plot_profiles(ax, result: LeadershipContextResult) -> None:
    profiles = result.predeparture_profiles
    for group, colour in PROFILE_COLOURS.items():
        rows = profiles[profiles["group"] == group]
        ax.plot(rows["lag_min"], rows["mean_ticks"], color=colour, lw=1.5, label=group)
    for (start, stop), colour in ((PREDEPARTURE_BASELINE_MIN, WONG_GREY),
                                  (PREDEPARTURE_WINDOW_MIN, WONG_ORANGE)):
        ax.axvspan(start, stop, color=colour, alpha=0.12)
    ax.axvline(0, color="black", lw=0.8)
    ax.set_xlabel("minutes from group-movement onset (shaded: baseline, window)")
    ax.set_ylabel("240 mg ticks per still minute")
    ax.legend(fontsize="x-small", loc="upper left", framealpha=0.9)


def _plot_restlessness(ax, result: LeadershipContextResult) -> None:
    table = result.per_heifer.set_index("lom").sort_values("predeparture_rise_rank")
    x = np.arange(len(table))
    ax.errorbar(x, table["predeparture_rise_rank"],
                yerr=[table["predeparture_rise_rank"] - table["predeparture_rise_rank_lo"],
                      table["predeparture_rise_rank_hi"] - table["predeparture_rise_rank"]],
                fmt="o", ms=4, lw=1, color=VF_COLOURS["leader_raw"])
    ax.axhline(0.5, color="black", lw=0.8, ls="--")
    ax.set_xticks(x, [lom[-4:] for lom in table.index], rotation=90, fontsize="x-small")
    ax.set_ylabel("rank of activity rise (0 = most restless)")
    tests = result.predeparture_tests.set_index("test")
    rise, w = tests.loc["rise vs departure time"], tests.loc["W of rise ranks"]
    ax.text(0.02, 0.97, f"rise vs departure order: mean rho = {rise['mean_rho']:+.3f}, "
            f"{format_p(rise['p_two_sided'])}\nfirst movers' rise rank {rise['first_mover_rise_rank']:.2f} "
            f"(0.5 = none)\nconsistency W = {w['w']:.3f}, {format_p(w['p'])}",
            transform=ax.transAxes, va="top", fontsize="x-small")


def plot_agreement(ax, agreement: pd.DataFrame) -> None:
    measures = list(dict.fromkeys(agreement["measure_a"]))
    rho = agreement.pivot(index="measure_a", columns="measure_b", values="rho").loc[measures, measures]
    p = agreement.pivot(index="measure_a", columns="measure_b", values="p").loc[measures, measures]
    image = ax.imshow(rho.to_numpy(), cmap=AGREEMENT_CMAP, vmin=-1, vmax=1)
    for i in range(len(measures)):
        for j in range(len(measures)):
            if i != j:
                star = "*" if p.iloc[i, j] < 0.05 else ""
                ax.text(j, i, f"{rho.iloc[i, j]:.2f}{star}", ha="center", va="center", fontsize="x-small")
    labels = [LEADING_MEASURES[m] for m in measures]
    ax.set_xticks(range(len(measures)), labels, rotation=40, ha="right", fontsize="x-small")
    ax.set_yticks(range(len(measures)), labels, fontsize="x-small")
    ax.grid(False)
    plt.colorbar(image, ax=ax, shrink=0.8, label=r"Spearman $\rho$ across heifers (* p < 0.05)")
