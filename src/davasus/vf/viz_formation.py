# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz_formation                                      ║
# ║  « the herd's leading edge, four panels »                        ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Front-rear asymmetry raw and lag-corrected, the lag-free        ║
# ║  formation offset, the heading check, and front against rear     ║
# ║  rates, coloured by the role call.                               ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Figure for the formation (positional leadership) analysis."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

from davasus.vf.constants import FORMATION_SPEED_BANDS, ROLE_COLOURS, VF_COLOURS
from davasus.vf.formation_analysis import ROLE_ORDER, SCENARIOS, FormationResult
from davasus.viz import apply_style

apply_style()

FIGSIZE_IN: tuple[float, float] = (15.0, 9.5)
DODGE: float = 0.18
SCENARIO_LABELS: dict[str, str] = {"raw": "raw positions", "lag_corrected": "collar lag removed"}
SCENARIO_COLOURS: dict[str, str] = {"raw": VF_COLOURS["leader_raw"],
                                    "lag_corrected": VF_COLOURS["leader_lag"]}
BAND_MARKERS: tuple[str, ...] = ("o", "s")


def plot_formation(result: FormationResult) -> tuple[Figure, pd.DataFrame]:
    """Four panels; returns the figure and the per-heifer table behind it."""
    band = next(iter(FORMATION_SPEED_BANDS))
    table = result.per_heifer
    slow = table[table["band"] == band]
    order = _order_heifers(slow)
    fig, axes = plt.subplots(2, 2, figsize=FIGSIZE_IN, constrained_layout=True)
    _plot_asymmetry(axes[0, 0], slow, order)
    _plot_intercepts(axes[0, 1], slow[slow["scenario"] == "raw"], order)
    _plot_headings(axes[1, 0], result.headings)
    _plot_front_rear(axes[1, 1], slow[slow["scenario"] == "raw"])
    for ax, letter in zip(axes.flat, "ABCD", strict=True):
        ax.set_title(letter, loc="left", fontweight="bold")
    n = int(result.tests.loc[(result.tests["scenario"] == "raw") & (result.tests["band"] == band),
                             "n_minutes"].iloc[0])
    fig.suptitle(f"Leading edge of the moving herd: hull arcs in {n} minutes at {band}, 24 heifers, 2024",
                 fontsize="medium")
    return fig, table


def _order_heifers(slow: pd.DataFrame) -> list[str]:
    raw = slow[slow["scenario"] == "raw"].copy()
    raw["rank"] = raw["role"].map({role: k for k, role in enumerate(ROLE_ORDER)})
    return raw.sort_values(["rank", "asymmetry"], ascending=[True, False])["lom"].tolist()


def _label_ticks(ax, order: list[str], roles: pd.Series) -> None:
    ax.set_xticks(np.arange(len(order)), [lom[-4:] for lom in order], rotation=90, fontsize="x-small")
    for tick, lom in zip(ax.get_xticklabels(), order, strict=True):
        tick.set_color(ROLE_COLOURS[roles[lom]])
        tick.set_fontweight("bold")


def _plot_asymmetry(ax, slow: pd.DataFrame, order: list[str]) -> None:
    x = np.arange(len(order))
    for k, scenario in enumerate(SCENARIOS):
        rows = slow[slow["scenario"] == scenario].set_index("lom").loc[order]
        ax.errorbar(x + (k - 0.5) * 2 * DODGE, rows["asymmetry"],
                    yerr=[rows["asymmetry"] - rows["asymmetry_lo"], rows["asymmetry_hi"] - rows["asymmetry"]],
                    fmt="o", ms=4, lw=1, color=SCENARIO_COLOURS[scenario], label=SCENARIO_LABELS[scenario])
    ax.axhline(0, color="black", lw=0.8)
    _label_ticks(ax, order, slow.drop_duplicates("lom").set_index("lom")["role"])
    ax.set_ylabel("front-rear asymmetry\n(front - rear) / (front + rear)")
    ax.legend(fontsize="x-small", loc="upper right", framealpha=0.9,
              title="tag colour = role", title_fontsize="x-small")


def _plot_intercepts(ax, raw: pd.DataFrame, order: list[str]) -> None:
    rows = raw.set_index("lom").loc[order]
    x = np.arange(len(order))
    colours = [ROLE_COLOURS[r] for r in rows["role"]]
    ax.errorbar(x, rows["intercept_m"], yerr=[rows["intercept_m"] - rows["intercept_lo"],
                                              rows["intercept_hi"] - rows["intercept_m"]],
                fmt="none", lw=1, ecolor="black")
    ax.scatter(x, rows["intercept_m"], c=colours, s=30, zorder=3, edgecolors="black", linewidths=0.5)
    ax.axhline(0, color="black", lw=0.8)
    _label_ticks(ax, order, rows["role"])
    ax.set_ylabel("ahead of centroid at zero herd speed (m)\nfree of collar lag")
    for role in ROLE_ORDER:
        ax.scatter([], [], c=ROLE_COLOURS[role], s=30, edgecolors="black", linewidths=0.5, label=role)
    ax.legend(fontsize="x-small", loc="upper right", framealpha=0.9, ncol=2)


def _plot_headings(ax, headings: pd.DataFrame) -> None:
    pairs = headings["pair"].unique().tolist()
    for (scenario, band), rows in headings.groupby(["scenario", "band"], sort=False):
        marker = BAND_MARKERS[list(FORMATION_SPEED_BANDS).index(band)]
        offset = (SCENARIOS.index(scenario) - 0.5) * 2 * DODGE
        ax.scatter([pairs.index(p) + offset for p in rows["pair"]], rows["rho"], marker=marker,
                   color=SCENARIO_COLOURS[scenario], s=40, label=f"{SCENARIO_LABELS[scenario]}, {band}")
    ax.axhline(0, color="black", lw=0.8)
    ax.set_xticks(range(len(pairs)), [f"heading {p}" for p in pairs])
    ax.set_ylim(-1, 1.35)
    ax.set_ylabel(r"$\rho_s$ of asymmetry between headings")
    ax.legend(fontsize="x-small", loc="upper center", ncol=2, framealpha=0.9,
              title="fixed GPS offset: opposite headings negative", title_fontsize="x-small")


def _plot_front_rear(ax, raw: pd.DataFrame) -> None:
    ax.scatter(raw["rear_rate"], raw["front_rate"], c=[ROLE_COLOURS[r] for r in raw["role"]],
               s=40, edgecolors="black", linewidths=0.5)
    for row in raw.itertuples():
        ax.annotate(row.lom[-4:], (row.rear_rate, row.front_rate), fontsize="xx-small",
                    xytext=(3, 3), textcoords="offset points")
    top = float(max(raw["front_rate"].max(), raw["rear_rate"].max())) * 1.1
    ax.plot([0, top], [0, top], color="black", lw=0.8, ls="--")
    ax.set_xlim(0, top)
    ax.set_ylim(0, top)
    ax.set_aspect("equal")
    ax.set_xlabel("share of moving minutes on the rear arc")
    ax.set_ylabel("share on the front arc")
