# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz_subgroups                                      ║
# ║  « bonds, communities and the splits that test them »            ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Composite bond matrix ordered by community, the network laid    ║
# ║  out by bond strength, when the herd split, and whether bonded   ║
# ║  pairs and community members stayed on the same side.            ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Figure for the subgroup analysis."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle

from davasus.vf.constants import (
    SPLIT_DISTANCE_M,
    WONG_BLUE,
    WONG_GREY,
    WONG_ORANGE,
    WONG_VERMILLION,
)
from davasus.vf.pair_bonds import COMPOSITE, shift_to_weights
from davasus.vf.subgroups_analysis import SubgroupsResult
from davasus.vf.viz_pull_anchor import format_p
from davasus.viz import apply_style

apply_style()

FIGSIZE_IN: tuple[float, float] = (17.0, 10.5)
LOCAL_TZ: str = "Europe/Berlin"
COMMUNITY_COLOURS: tuple[str, ...] = (WONG_BLUE, WONG_ORANGE, WONG_VERMILLION, WONG_GREY)
EDGE_QUANTILE: float = 0.85
CONTROL_COLOURS: dict[str, str] = {"none": WONG_BLUE, "rank gap": WONG_VERMILLION}
CONTROL_LABELS: dict[str, str] = {"none": "plain", "rank gap": "hierarchy-rank gap removed"}


def plot_subgroups(result: SubgroupsResult, hierarchy: pd.DataFrame | None) -> tuple[Figure, pd.DataFrame]:
    """Six panels plus the pair table behind them."""
    fig, axes = plt.subplots(2, 3, figsize=FIGSIZE_IN, constrained_layout=True)
    members = result.communities[result.communities["layer"] == COMPOSITE].set_index("lom")["community"]
    rank = None if hierarchy is None else hierarchy.set_index("lom")["rank"]
    _plot_bond_matrix(axes[0, 0], result.layers[COMPOSITE], members, rank)
    _plot_network(axes[0, 1], result.layers[COMPOSITE], members, rank)
    _plot_split_times(axes[0, 2], result.events)
    _plot_pairs(axes[1, 0], result)
    _plot_bond_tests(axes[1, 1], result.bond_tests)
    _plot_community_test(axes[1, 2], result)
    for ax, letter in zip(axes.flat, "ABCDEF", strict=True):
        ax.set_title(letter, loc="left", fontweight="bold")
    summary = result.summary.iloc[0]
    quality = result.modularity.set_index("layer").loc[COMPOSITE]
    fig.suptitle(f"Subgroups: bonds from {summary['n_training_days']} split-free days, tested on "
                 f"{summary['n_splits']} splits ({summary['split_minutes']} min at {SPLIT_DISTANCE_M:g} m); "
                 f"modularity Q = {quality['modularity']:.3f} (shuffled {quality['null_mean']:.3f}, "
                 f"{format_p(quality['p'])})")
    return fig, result.pairs


def _order(members: pd.Series, rank: pd.Series | None) -> list[str]:
    table = pd.DataFrame({"community": members, "rank": 0 if rank is None else rank.reindex(members.index)})
    return list(table.sort_values(["community", "rank"]).index)


def _colour(community: str) -> str:
    return COMMUNITY_COLOURS[min(ord(community) - ord("A"), len(COMMUNITY_COLOURS) - 1)]


def _plot_bond_matrix(ax, composite: pd.DataFrame, members: pd.Series, rank: pd.Series | None) -> None:
    order = _order(members, rank)
    values = composite.loc[order, order].to_numpy()
    limit = float(np.nanquantile(np.abs(values), 0.98))
    image = ax.imshow(values, cmap="RdBu_r", vmin=-limit, vmax=limit)
    start = 0
    for community, size in members.loc[order].groupby(members.loc[order], sort=False).size().items():
        ax.add_patch(Rectangle((start - 0.5, start - 0.5), size, size, fill=False, edgecolor=_colour(community),
                               linewidth=2.5))
        start += size
    labels = [lom[-4:] for lom in order]
    ax.set_xticks(range(len(order)), labels, rotation=90, fontsize="xx-small")
    ax.set_yticks(range(len(order)), labels, fontsize="xx-small")
    ax.grid(False)
    plt.colorbar(image, ax=ax, shrink=0.8, label="composite bond (mean z of four layers)")
    ax.set_xlabel("heifers by community, then hierarchy rank")


def _plot_network(ax, composite: pd.DataFrame, members: pd.Series, rank: pd.Series | None) -> None:
    """Classical MDS of the bonds; the strongest pairs drawn as lines."""
    loms = list(composite.index)
    weights = shift_to_weights(composite.to_numpy())
    xy = _embed(weights.max() - weights)
    a, b = np.triu_indices(len(loms), 1)
    is_strong = weights[a, b] >= np.quantile(weights[a, b], EDGE_QUANTILE)
    for i, j in zip(a[is_strong], b[is_strong], strict=True):
        is_same = members[loms[i]] == members[loms[j]]
        ax.plot(xy[[i, j], 0], xy[[i, j], 1], color=_colour(members[loms[i]]) if is_same else WONG_GREY,
                linewidth=0.8, alpha=0.6, zorder=1)
    size = 120.0 if rank is None else 60.0 + 12.0 * (len(loms) - rank.reindex(loms).to_numpy())
    ax.scatter(xy[:, 0], xy[:, 1], s=size, c=[_colour(members[lom]) for lom in loms], edgecolor="black",
               linewidth=0.5, zorder=2)
    for k, lom in enumerate(loms):
        ax.annotate(lom[-4:], xy[k], xytext=(0, 9), textcoords="offset points", ha="center", fontsize="xx-small")
    for community in sorted(members.unique()):
        ax.scatter([], [], c=_colour(community), edgecolor="black", label=f"community {community}")
    ax.legend(frameon=False, fontsize="x-small", loc="best")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlabel(f"bond space (MDS); lines: strongest {100 * (1 - EDGE_QUANTILE):.0f} % of pairs; "
                  "larger = higher rank")


def _embed(distance: np.ndarray) -> np.ndarray:
    n = len(distance)
    np.fill_diagonal(distance, 0.0)
    centring = np.eye(n) - np.ones((n, n)) / n
    values, vectors = np.linalg.eigh(-0.5 * centring @ (distance ** 2) @ centring)
    return vectors[:, -2:] * np.sqrt(np.maximum(values[-2:], 0.0))


def _plot_split_times(ax, events: pd.DataFrame) -> None:
    local = pd.to_datetime(events["onset_utc"]).dt.tz_convert(LOCAL_TZ)
    smaller = events["minority"].str.count(",") + (events["minority"] != "")
    points = ax.scatter(local.dt.normalize().dt.tz_localize(None), local.dt.hour + local.dt.minute / 60.0,
                        s=10.0 + 4.0 * events["length_min"], c=smaller, cmap="viridis", alpha=0.8,
                        edgecolor="black", linewidth=0.3)
    plt.colorbar(points, ax=ax, shrink=0.8, label="heifers on the smaller side(s)")
    ax.set_ylim(0, 24)
    ax.set_yticks(range(0, 25, 4))
    ax.set_ylabel("local time of day (h)")
    ax.set_xlabel("day (marker size: length of the split)")
    ax.tick_params(axis="x", rotation=30)


def _plot_pairs(ax, result: SubgroupsResult) -> None:
    pairs = result.pairs
    for is_same, colour, label in ((False, WONG_GREY, "different communities"), (True, WONG_BLUE, "same community")):
        subset = pairs[pairs["same_community"] == is_same]
        ax.scatter(subset[COMPOSITE], subset["together_rate"], s=14, color=colour, alpha=0.7, label=label)
    test = result.bond_tests.query("layer == @COMPOSITE and control == 'none' and split_distance_m == @SPLIT_DISTANCE_M")
    ax.set_xlabel("composite bond on split-free days (z)")
    ax.set_ylabel("share of splits on the same side")
    ax.legend(frameon=False, fontsize="x-small", loc="upper left",
              title=rf"$\rho$ = {test['rho'].iloc[0]:.2f}, {format_p(test['p'].iloc[0])} (Mantel)",
              title_fontsize="x-small")


def _plot_bond_tests(ax, tests: pd.DataFrame) -> None:
    tests = tests[tests["split_distance_m"] == SPLIT_DISTANCE_M]
    layers = list(dict.fromkeys(tests["layer"]))
    for control, offset in (("none", -0.15), ("rank gap", 0.15)):
        subset = tests[tests["control"] == control].set_index("layer").reindex(layers)
        y = np.arange(len(layers)) + offset
        ax.barh(y, subset["null_hi"], height=0.25, color=WONG_GREY, alpha=0.35)
        ax.scatter(subset["rho"], y, color=CONTROL_COLOURS[control], zorder=3, label=CONTROL_LABELS[control])
        for value, p, row in zip(subset["rho"], subset["p"], y, strict=True):
            if np.isfinite(value):
                ax.annotate(format_p(p), (value, row), xytext=(6, 0), textcoords="offset points", va="center",
                            fontsize="xx-small")
    ax.axvline(0.0, color="black", linewidth=0.6)
    ax.set_yticks(range(len(layers)), layers)
    ax.invert_yaxis()
    ax.set_xlabel(r"Mantel $\rho$: bond vs same side in splits (grey: 95 % of relabelled herds)")
    ax.legend(frameon=False, fontsize="x-small", loc="lower right")


def _plot_community_test(ax, result: SubgroupsResult) -> None:
    pairs = result.pairs
    rng = np.random.default_rng(0)
    for k, (is_same, colour) in enumerate(((True, WONG_BLUE), (False, WONG_GREY))):
        values = pairs.loc[pairs["same_community"] == is_same, "together_rate"].dropna().to_numpy()
        body = ax.violinplot(values, positions=[k], showextrema=False, widths=0.7)
        for part in body["bodies"]:
            part.set_facecolor(colour)
            part.set_alpha(0.3)
            part.get_paths()[0].vertices[:, 0] = np.minimum(part.get_paths()[0].vertices[:, 0], k)
        ax.scatter(k + 0.08 + 0.25 * rng.random(len(values)), values, s=8, color=colour, alpha=0.6)
        ax.boxplot(values, positions=[k], widths=0.08, showfliers=False, medianprops={"color": "black"})
    test = result.community_tests.query("layer == @COMPOSITE and split_distance_m == @SPLIT_DISTANCE_M")
    ax.set_xticks([0, 1], ["same community", "different communities"])
    ax.set_ylabel("share of splits on the same side")
    if len(test):
        ax.set_xlabel(f"difference {test['difference'].iloc[0]:+.3f}, {format_p(test['p'].iloc[0])} "
                      "(labels shuffled over heifers)")
