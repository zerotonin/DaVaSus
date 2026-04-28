"""Single source of truth for colours, figure rcparams, and shared thresholds.

Import named symbols from here rather than hard-coding hex values, font
sizes, or magic thresholds in individual analysis modules. Adding a new
sensor / variable mostly means adding one entry to :data:`COLOURS` so it
keeps the same colour everywhere it appears.

The base palette is **Wong (2011) / Okabe-Ito** — eight colours plus
black, deliberately chosen to be distinguishable for the most common
forms of colour-vision deficiency.

Reference:
    Wong, B. (2011). *Points of view: Color blindness.* Nature Methods
    8(6), 441. https://doi.org/10.1038/nmeth.1618
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────────────
#  « Wong 2011 base palette »
# ────────────────────────────────────────────────────────────────────────

WONG_BLUE       = "#0072B2"
WONG_ORANGE     = "#E69F00"
WONG_GREEN      = "#009E73"
WONG_VERMILLION = "#D55E00"
WONG_SKY        = "#56B4E9"
WONG_PINK       = "#CC79A7"
WONG_YELLOW     = "#F0E442"
WONG_GREY       = "#999999"
WONG_BLACK      = "#000000"

WONG_PALETTE: tuple[str, ...] = (
    WONG_BLUE,
    WONG_ORANGE,
    WONG_GREEN,
    WONG_VERMILLION,
    WONG_SKY,
    WONG_PINK,
    WONG_YELLOW,
    WONG_GREY,
)


# ────────────────────────────────────────────────────────────────────────
#  « Semantic colour mappings »
# ────────────────────────────────────────────────────────────────────────
#
#  One nested dict so a future ``Palette`` wrapper can iterate them
#  uniformly. The convention is: same biological signal → same colour
#  across every figure.

COLOURS: dict[str, dict[str, str]] = {
    # Per-variable colours. Reference these when plotting one signal at
    # a time (e.g. rumen-temp histogram, THI time series).
    "variables": {
        "rumen_temp":     WONG_VERMILLION,
        "temp":           WONG_VERMILLION,
        "temp_clean":     WONG_VERMILLION,  # temp_without_drink_cycles
        "thi":            WONG_BLUE,
        "thi_nrc":        WONG_SKY,
        "thi_mader":      WONG_BLUE,
        "air_temp":       WONG_ORANGE,
        "humidity":       WONG_GREEN,
        "wind":           WONG_GREY,
        "solar":          WONG_YELLOW,
        "rain":           WONG_SKY,
        "ph":             WONG_PINK,
        "activity":       WONG_ORANGE,
        "rumination":     WONG_SKY,
        "heat_index":     WONG_VERMILLION,
        "calving_index":  WONG_PINK,
        "fence_dist":     WONG_GREY,
        "gps":            WONG_GREEN,
        "water":          WONG_SKY,
        "imu":            WONG_ORANGE,
    },

    # Plot-element categories. Reuse these to keep "fit line", "scatter",
    # "median marker", etc. visually consistent across analyses.
    "categories": {
        "below_bp":       WONG_BLUE,
        "above_bp":       WONG_VERMILLION,
        "fit_line":       WONG_VERMILLION,
        "reference":      WONG_ORANGE,
        "identity":       WONG_GREY,
        "scatter":        WONG_SKY,
        "scatter_alt":    WONG_GREEN,
        "median":         WONG_VERMILLION,
        "paired_line":    WONG_GREY,
        "highlight":      WONG_PINK,
    },

    # Per-group colours for animal stratifiers. Look these up dynamically
    # so we can recolour a whole figure consistently when a covariate
    # changes (e.g. switching from sex to origin).
    "groups": {
        "sex": {
            "f":  WONG_PINK,
            "m":  WONG_BLUE,
        },
        "origin": {
            "PL": WONG_ORANGE,
            "DE": WONG_GREEN,
        },
        "thi_mode": {
            "nrc":   WONG_SKY,
            "mader": WONG_BLUE,
        },
        "season": {
            "calving":  WONG_PINK,
            "spring":   WONG_GREEN,
            "summer":   WONG_VERMILLION,
            "autumn":   WONG_ORANGE,
            "winter":   WONG_BLUE,
        },
        "fit_outcome": {
            "success":       WONG_GREEN,
            "low_n":         WONG_GREY,
            "constraint":    WONG_VERMILLION,
        },
    },
}


def colour_for_variable(name: str, default: str = WONG_GREY) -> str:
    """Return the canonical colour for a sensor / measurement variable.

    Args:
        name: Variable key (e.g. ``"rumen_temp"`` or ``"thi_mader"``).
        default: Fallback colour when ``name`` is unknown.

    Returns:
        Hex colour string.
    """
    return COLOURS["variables"].get(name, default)


def colour_for_category(name: str, default: str = WONG_GREY) -> str:
    """Return the colour for a plot-element category (e.g. ``"fit_line"``).

    Args:
        name: Category key.
        default: Fallback colour when unknown.

    Returns:
        Hex colour string.
    """
    return COLOURS["categories"].get(name, default)


def colour_for_group(dimension: str, value: object, default: str = WONG_GREY) -> str:
    """Return the colour for ``value`` along a stratifier ``dimension``.

    Args:
        dimension: Group axis (``"sex"``, ``"origin"``, ``"season"``,
            ``"thi_mode"``, ``"fit_outcome"``).
        value: Group label. Coerced to ``str`` for lookup.
        default: Fallback colour when the group or value is unknown.

    Returns:
        Hex colour string.
    """
    return COLOURS["groups"].get(dimension, {}).get(str(value), default)


# ────────────────────────────────────────────────────────────────────────
#  « Figure rcparams »
# ────────────────────────────────────────────────────────────────────────
#
#  Applied once per process via :func:`davasus.viz.apply_style`. ``svg``
#  and ``pdf`` font types keep text editable in the saved vector files;
#  see the *Bart Coding Whitepaper* §5.

RCPARAMS: dict[str, object] = {
    "svg.fonttype":      "none",
    "pdf.fonttype":      42,
    "ps.fonttype":       42,
    "font.family":       "sans-serif",
    "font.size":         11,
    "axes.titlesize":    13,
    "axes.labelsize":    12,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "figure.dpi":        150,
    "savefig.dpi":       300,
    "savefig.bbox":      "tight",
}

MPL_STYLE: str = "seaborn-v0_8-whitegrid"


# ────────────────────────────────────────────────────────────────────────
#  « Literature thresholds »
# ────────────────────────────────────────────────────────────────────────

THI_REFERENCE: float = 68.8
"""Mild heat-stress THI threshold (Hoffmann et al. 2020)."""

THI_PLAUSIBLE_RANGE: tuple[float, float] = (45.0, 80.0)
"""Default x-axis range for THI broken-stick plots."""

RUMEN_TEMP_PLAUSIBLE_RANGE: tuple[float, float] = (30.0, 43.0)
"""Physiological plausibility window for `temp` and `temp_without_drink_cycles`."""
