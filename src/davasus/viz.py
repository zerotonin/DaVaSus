"""Figure-saving helpers and matplotlib style configuration.

Two responsibilities:

* :func:`apply_style` applies the project rcparams from
  :data:`davasus.constants.RCPARAMS` (editable text, sane font sizes,
  no top/right spines). Called automatically on import so any module
  that pulls in ``davasus.viz`` gets the project look.
* :func:`save_triplet` writes a figure as the canonical **PNG +
  editable-text SVG + CSV** triplet under one stem.

See the *Bart Coding Whitepaper* §5 for the rationale.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from davasus.constants import MPL_STYLE, RCPARAMS

log = logging.getLogger(__name__)


def apply_style() -> None:
    """Apply the DaVaSus matplotlib style and rcparams.

    Idempotent — calling more than once is harmless. Pulls
    :data:`davasus.constants.RCPARAMS` and sets the seaborn-whitegrid
    style declared in :data:`davasus.constants.MPL_STYLE`.
    """
    if MPL_STYLE in plt.style.available:
        plt.style.use(MPL_STYLE)
    matplotlib.rcParams.update(RCPARAMS)


# Apply once on import so the rcparams are in effect before any figure
# is created. Individual scripts can re-apply or override if needed.
apply_style()


def save_triplet(
    fig: Figure,
    df: pd.DataFrame | None,
    stem: str | Path,
    *,
    dpi: int = 200,
    csv_index: bool = False,
) -> dict[str, Path]:
    """Write a figure plus its underlying data as PNG + SVG + CSV.

    Args:
        fig: Matplotlib figure to save.
        df: Long-format DataFrame backing the figure, or ``None`` if the
            figure has no tabular data (e.g. a schematic). When ``None``
            an explicit log line records the omission.
        stem: Output stem (with or without an extension); siblings
            ``<stem>.png``, ``<stem>.svg``, and ``<stem>.csv`` are
            written. Parent directory is created if it does not exist.
        dpi: PNG raster resolution.
        csv_index: Whether to write the DataFrame index to the CSV.

    Returns:
        Dict mapping ``"png" | "svg" | "csv"`` → :class:`Path`. The
        ``"csv"`` key is omitted when ``df`` is ``None``.
    """
    stem_path = Path(stem).with_suffix("")
    stem_path.parent.mkdir(parents=True, exist_ok=True)

    png_path = stem_path.with_suffix(".png")
    svg_path = stem_path.with_suffix(".svg")
    fig.savefig(png_path, dpi=dpi, bbox_inches="tight")
    fig.savefig(svg_path, bbox_inches="tight")

    written: dict[str, Path] = {"png": png_path, "svg": svg_path}
    if df is None:
        log.info("save_triplet: no df given, skipped CSV for %s", stem_path)
    else:
        csv_path = stem_path.with_suffix(".csv")
        df.to_csv(csv_path, index=csv_index)
        written["csv"] = csv_path
    return written
