# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.viz_pull_anchor                                    ║
# ║  « who pulls whom, six panels »                                  ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Lead share per heifer, the hierarchy test, the pull matrix,     ║
# ║  the GPS noise floor behind the thresholds, agreement with       ║
# ║  formation and first movers, and why success rate is not used.   ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Figure for the pull and anchor analysis."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from scipy.stats import spearmanr

from davasus.vf.constants import (
    MOVE_MIN_M,
    PULL_SWING_M,
    ROLE_COLOURS,
    VF_COLOURS,
)
from davasus.vf.pull_anchor_analysis import SCENARIOS, PullAnchorResult
from davasus.viz import apply_style

apply_style()

FIGSIZE_IN: tuple[float, float] = (16.0, 10.0)
DODGE: float = 0.18
SCENARIO_LABELS: dict[str, str] = {"raw": "raw positions", "lag_corrected": "collar lag removed"}
SCENARIO_COLOURS: dict[str, str] = {"raw": VF_COLOURS["leader_raw"],
                                    "lag_corrected": VF_COLOURS["leader_lag"]}
MATRIX_CMAP: str = "RdBu_r"
LEGEND_ROOM: float = 0.12


def plot_pull_anchor(result: PullAnchorResult,
                     comparison: pd.DataFrame | None = None) -> tuple[Figure, pd.DataFrame]:
    """Six panels; ``comparison`` (lom, role, formation_asymmetry, dumont_share) is optional."""
    table = result.per_heifer
    raw = table[table["scenario"] == "raw"].set_index("lom")
    order = raw.sort_values("lead_share", ascending=False).index.tolist()
    roles = comparison.set_index("lom")["role"] if comparison is not None else None
    fig, axes = plt.subplots(2, 3, figsize=FIGSIZE_IN, constrained_layout=True)
    _plot_lead_share(axes[0, 0], table, order, roles)
    _plot_tests(axes[0, 1], result.tests)
    _plot_matrix(axes[0, 2], result.pull_matrix.loc[order, order])
    _plot_noise(axes[1, 0], result.gps_noise)
    _plot_comparison(axes[1, 1], raw, comparison)
    _plot_mobility(axes[1, 2], raw)
    for ax, letter in zip(axes.flat, "ABCDEF", strict=True):
        ax.set_title(letter, loc="left", fontweight="bold")
    n = int(result.tests.loc[result.tests["scenario"] == "raw", "n_pulls"].iloc[0])
    fig.suptitle(f"Pulls between pairs (Strandburg-Peshkin 2015): {n} pulls, swing >= {PULL_SWING_M:g} m, "
                 "24 heifers, 2024", fontsize="medium")
    out = table.merge(comparison, on="lom", how="left") if comparison is not None else table
    return fig, out


def _plot_lead_share(ax, table: pd.DataFrame, order: list[str], roles: pd.Series | None) -> None:
    x = np.arange(len(order))
    for k, scenario in enumerate(SCENARIOS):
        rows = table[table["scenario"] == scenario].set_index("lom").loc[order]
        ax.errorbar(x + (k - 0.5) * 2 * DODGE, rows["lead_share"],
                    yerr=[rows["lead_share"] - rows["lead_share_lo"], rows["lead_share_hi"] - rows["lead_share"]],
                    fmt="o", ms=4, lw=1, color=SCENARIO_COLOURS[scenario], label=SCENARIO_LABELS[scenario])
    ax.axhline(0.5, color="black", lw=0.8, ls="--")
    ax.set_xticks(x, [lom[-4:] for lom in order], rotation=90, fontsize="x-small")
    if roles is not None:
        for tick, lom in zip(ax.get_xticklabels(), order, strict=True):
            tick.set_color(ROLE_COLOURS.get(roles.get(lom, "middle"), "black"))
            tick.set_fontweight("bold")
    ax.set_ylabel("lead share: pulls led / (led + followed)")
    ax.legend(fontsize="x-small", loc="upper right", framealpha=0.9,
              title="tag colour = formation role" if roles is not None else None, title_fontsize="x-small")


def _plot_tests(ax, tests: pd.DataFrame) -> None:
    labels = []
    for k, row in enumerate(tests.itertuples()):
        for j, (stat, lo, hi, p, name) in enumerate((
                (row.dci, row.dci_null_mean, row.dci_null_hi, row.dci_p, "DCI"),
                (row.sd_lead_share, row.sd_null_mean, row.sd_null_hi, row.sd_p, "SD lead share"))):
            y = 2 * k + j
            ax.plot([0, hi], [y, y], lw=6, color=VF_COLOURS["null_egalitarian"], alpha=0.6,
                    solid_capstyle="butt")
            ax.plot(lo, y, "|", color="black", ms=10)
            ax.plot(stat, y, "D", color=SCENARIO_COLOURS[row.scenario], ms=7)
            ax.text(stat, y + 0.25, " " + format_p(p), fontsize="x-small")
            labels.append(f"{SCENARIO_LABELS[row.scenario]}\n{name}")
    ax.set_yticks(range(len(labels)), labels, fontsize="x-small")
    ax.set_ylim(-0.6, len(labels) - 0.2)
    ax.invert_yaxis()
    ax.set_xlabel("observed (diamond) vs random pull directions (bar: 0 to 97.5 %, tick: mean)")


def _plot_matrix(ax, matrix: pd.DataFrame) -> None:
    values = matrix.to_numpy()
    total = values + values.T
    with np.errstate(invalid="ignore", divide="ignore"):
        balance = np.where(total > 0, (values - values.T) / total, np.nan)
    image = ax.imshow(balance, cmap=MATRIX_CMAP, vmin=-1, vmax=1)
    tags = [lom[-4:] for lom in matrix.index]
    ax.set_xticks(range(len(tags)), tags, rotation=90, fontsize="xx-small")
    ax.set_yticks(range(len(tags)), tags, fontsize="xx-small")
    ax.set_xlabel("partner")
    ax.set_ylabel("heifer (ordered by lead share)")
    ax.grid(False)
    plt.colorbar(image, ax=ax, shrink=0.8, label="(led - followed) / pulls in the pair")


def _plot_noise(ax, noise: pd.DataFrame) -> None:
    for measure, colour, label in (("displacement", VF_COLOURS["leader_raw"], "one still heifer: displacement"),
                                   ("pair_range", VF_COLOURS["null_graded"], "two still heifers: pair-distance range")):
        rows = noise[noise["measure"] == measure]
        ax.fill_between(rows["lag_min"], rows["median_m"], rows["p99_m"], color=colour, alpha=0.15)
        ax.plot(rows["lag_min"], rows["p95_m"], "-o", color=colour, ms=3, label=f"{label} (95 %)")
        ax.plot(rows["lag_min"], rows["p99_m"], "--", color=colour, lw=0.8)
    ax.axhline(PULL_SWING_M, color="black", lw=1, label=f"swing threshold {PULL_SWING_M:g} m")
    ax.axhline(MOVE_MIN_M, color="black", lw=1, ls=":", label=f"real move {MOVE_MIN_M:g} m")
    ax.set_xlabel("time window (min)")
    ax.set_ylabel("GPS noise (m); band: median to 99 %")
    ax.legend(fontsize="x-small", loc="lower right", framealpha=0.9)


def _plot_comparison(ax, raw: pd.DataFrame, comparison: pd.DataFrame | None) -> None:
    if comparison is None:
        ax.text(0.5, 0.5, "run davasus-vf-formation and davasus-vf-leadership\nfor this panel",
                ha="center", va="center", transform=ax.transAxes, fontsize="small")
        ax.set_axis_off()
        return
    merged = raw.join(comparison.set_index("lom"))
    for column, colour, label in (("formation_asymmetry", VF_COLOURS["leader_lag"], "formation asymmetry"),
                                  ("dumont_share", VF_COLOURS["null_graded"], "first-mover share x 5")):
        scale = 5.0 if column == "dumont_share" else 1.0
        rho, p = spearmanr(merged["lead_share"], merged[column], nan_policy="omit")
        ax.scatter(merged["lead_share"], merged[column] * scale, s=22, color=colour,
                   label=rf"{label}: $\rho_s$ = {rho:.2f}, {format_p(p)}")
    ax.axvline(0.5, color="black", lw=0.8, ls="--")
    ax.set_xlabel("lead share (raw)")
    ax.set_ylabel("formation asymmetry / first-mover share x 5")
    ax.legend(fontsize="x-small", loc="upper left", framealpha=0.9)


def _plot_mobility(ax, raw: pd.DataFrame) -> None:
    pulls = raw["n_led"] + raw["n_followed"]
    for column, colour, label in (("lead_share", VF_COLOURS["leader_raw"], "lead share"),
                                  ("pull_success_rate", VF_COLOURS["pulse"], "pull success rate")):
        rho, p = spearmanr(pulls, raw[column])
        ax.scatter(pulls, raw[column], s=22, color=colour, label=rf"{label}: $\rho_s$ = {rho:.2f}, {format_p(p)}")
    ax.axhline(0.5, color="black", lw=0.8, ls="--")
    low = float(min(raw["lead_share"].min(), raw["pull_success_rate"].min()))
    ax.set_ylim(bottom=low - LEGEND_ROOM)
    ax.set_xlabel("pulls the heifer took part in (led + followed)")
    ax.set_ylabel("share / rate")
    ax.legend(fontsize="x-small", loc="lower right", framealpha=0.9,
              title="leading is not just taking part more often", title_fontsize="x-small")


def format_p(p: float) -> str:
    return "p < 0.001" if p < 0.001 else f"p = {p:.3f}"
