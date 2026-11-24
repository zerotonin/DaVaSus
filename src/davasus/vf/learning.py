# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.learning                                           ║
# ║  « audio before pulse: does the heifer learn »                   ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Q4 and Q5: success ratio SR = (A - P) / A from summed counts,   ║
# ║  never from averaged per-minute ratios. The file's               ║
# ║  Audio_Pulse_Ratio R = A / (A + P) maps to SR = 2 - 1/R.         ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Success ratio and per-animal learning curves (handover 3.4, 4.5)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from davasus.vf.constants import SR_BLOCK_SIZE

# ┌────────────────────────────────────────────────────────────┐
# │ Success ratio  « counts first, ratios last »               │
# └────────────────────────────────────────────────────────────┘


def compute_success_ratio(audio: float, pulse: float) -> float:
    """Fraction of audio cues not followed by a pulse; NaN when A = 0."""
    if audio <= 0:
        return float("nan")
    return (audio - pulse) / audio


def convert_audio_pulse_ratio_to_sr(ratio: float) -> float:
    """Map R = A / (A + P) to SR = 2 - 1/R; NaN for the codes 0 and -1."""
    if ratio <= 0:
        return float("nan")
    return 2.0 - 1.0 / ratio


def compute_audio_pulse_ratio(audio: float, pulse: float) -> float:
    """The file's ratio R = A / (A + P) from summed counts; NaN when A = 0."""
    if audio <= 0:
        return float("nan")
    return audio / (audio + pulse)


def compute_daily_stimulus_summary(fixes: pd.DataFrame) -> pd.DataFrame:
    """Per animal and UTC day: cue counts, SR and R, plus cumulative SR.

    Args:
        fixes: Columns ``lom``, ``timestamp`` (UTC), ``audio_stim_new``
               and ``pulse_stim_new`` (cues per minute).

    Returns:
        Columns ``lom``, ``date``, ``audio``, ``pulse``, ``sr``,
        ``ratio_file``, ``audio_cum``, ``pulse_cum``, ``sr_cum``.
    """
    daily = (
        fixes.assign(date=fixes["timestamp"].dt.floor("D"))
        .groupby(["lom", "date"], as_index=False)
        .agg(audio=("audio_stim_new", "sum"), pulse=("pulse_stim_new", "sum"))
    )
    daily["sr"] = _pairwise(compute_success_ratio, daily["audio"], daily["pulse"])
    daily["ratio_file"] = _pairwise(compute_audio_pulse_ratio, daily["audio"], daily["pulse"])
    daily["audio_cum"] = daily.groupby("lom")["audio"].cumsum()
    daily["pulse_cum"] = daily.groupby("lom")["pulse"].cumsum()
    daily["sr_cum"] = _pairwise(compute_success_ratio, daily["audio_cum"], daily["pulse_cum"])
    return daily


def _pairwise(func, first: pd.Series, second: pd.Series) -> list[float]:
    return [func(a, b) for a, b in zip(first, second, strict=True)]


def compute_block_success_ratios(
    exposures: pd.DataFrame,
    block_size: int = SR_BLOCK_SIZE,
) -> pd.DataFrame:
    """SR per consecutive block of exposures for one animal.

    Args:
        exposures:  One row per stimulus cluster in time order, with
                    columns ``audio`` and ``pulse`` (summed counts).
        block_size: Exposures per block; an incomplete last block is kept.

    Returns:
        Columns ``block``, ``n_exposures``, ``audio``, ``pulse``, ``sr``.
    """
    block = np.arange(len(exposures)) // block_size
    sums = exposures.groupby(block)[["audio", "pulse"]].sum()
    sums["n_exposures"] = exposures.groupby(block).size()
    sums["sr"] = [
        compute_success_ratio(a, p) for a, p in zip(sums["audio"], sums["pulse"], strict=True)
    ]
    return sums.rename_axis("block").reset_index()[["block", "n_exposures", "audio", "pulse", "sr"]]


# ┌────────────────────────────────────────────────────────────┐
# │ Learning curve  « exponential approach to SR_inf »         │
# └────────────────────────────────────────────────────────────┘


class LearningCurveFitter:
    """Fit SR(e) = SR_inf - (SR_inf - SR_0) exp(-e / tau) per animal."""

    def fit(self, blocks: pd.DataFrame) -> dict[str, float]:
        """Return ``sr_0``, ``sr_inf``, ``tau`` and ``is_fallback``."""
        raise NotImplementedError
