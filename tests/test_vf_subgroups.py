"""Herd splits, pair bonds, communities and the combined hierarchy on synthetic herds."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from davasus.vf import network_stats
from davasus.vf.herd_splits import NO_SIDE, compute_together_rate, detect_herd_splits
from davasus.vf.hierarchy import add_cue_totals, compute_combined_hierarchy
from davasus.vf.pair_bonds import (
    combine_bonds,
    compute_association,
    compute_departure_bonds,
    compute_pull_bonds,
    shift_to_weights,
)

N_ANIMALS = 24
LOMS = [f"DE{k:02d}" for k in range(N_ANIMALS)]


def _two_camp_herd(n_minutes: int = 400) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex]:
    """Heifers 0-15 and 16-23 graze as two clusters 10 m apart; the small one walks 200 m off twice."""
    rng = np.random.default_rng(0)
    x = rng.uniform(0.0, 20.0, (n_minutes, N_ANIMALS))
    y = rng.uniform(0.0, 20.0, (n_minutes, N_ANIMALS))
    x[:, 16:] += 30.0
    x[100:120, 16:] += 200.0
    x[130:150, 16:] += 200.0  # same sides, 10 min later: one event
    x[300:303, 16:] += 200.0  # too short
    return x, y, pd.date_range("2024-08-01", periods=n_minutes, freq="min", tz="UTC")


def test_splits_are_found_merged_and_sided():
    x, y, minutes = _two_camp_herd()
    splits = detect_herd_splits(x, y, minutes, LOMS, np.zeros(len(minutes), dtype=bool))
    assert splits.n_events == 1
    event = splits.events.iloc[0]
    assert (event["i0"], event["i1"], event["group_sizes"]) == (100, 150, "16+8")
    assert event["minority"].split(",") == LOMS[16:]
    together, n_splits = compute_together_rate(splits)
    assert together[0, 1] == 1.0 and together[0, 20] == 0.0 and n_splits[0, 20] == 1


def test_excluded_minutes_and_missing_heifers_have_no_side():
    x, y, minutes = _two_camp_herd()
    x[100:150, 3] = np.nan
    splits = detect_herd_splits(x, y, minutes, LOMS, np.zeros(len(minutes), dtype=bool))
    assert splits.sides[0, 3] == NO_SIDE
    excluded = np.zeros(len(minutes), dtype=bool)
    excluded[90:160] = True
    assert detect_herd_splits(x, y, minutes, LOMS, excluded).n_events == 0


def test_association_recovers_the_camps_and_communities():
    x, y, _ = _two_camp_herd()
    association = compute_association(x, y, np.ones(len(x), dtype=bool), step_min=1)
    assert association[0, 1] > association[0, 20]
    labels, modularity = network_stats.find_communities(shift_to_weights(association))
    assert len(set(labels[:16])) == 1 and len(set(labels[16:])) == 1 and labels[0] != labels[20]
    assert modularity > 0.2
    quality = network_stats.test_modularity(shift_to_weights(association), np.random.default_rng(1), 50)
    assert quality["p"] < 0.05


def test_mantel_detects_a_shared_structure_and_the_control_removes_it():
    rng = np.random.default_rng(2)
    group = np.repeat([0, 1], N_ANIMALS // 2)
    same = (group[:, None] == group[None, :]).astype(float)
    noise = rng.normal(0.0, 0.3, same.shape)
    predictor, outcome = same + noise + noise.T, same + rng.normal(0.0, 0.3, same.shape)
    outcome = (outcome + outcome.T) / 2
    plain = network_stats.test_matrix_correlation(predictor, outcome, rng, 500)
    assert plain["rho"] > 0.4 and plain["p"] < 0.01
    partial = network_stats.test_matrix_correlation(predictor, outcome, rng, 500, control=same)
    assert abs(partial["rho"]) < 0.2
    unrelated = rng.normal(size=same.shape)
    assert network_stats.test_matrix_correlation(unrelated + unrelated.T, outcome, rng, 500)["p"] > 0.01


def test_partition_separation():
    labels = np.repeat([0, 1], N_ANIMALS // 2)
    together = np.where(labels[:, None] == labels[None, :], 0.9, 0.2)
    result = network_stats.test_partition_separation(labels, together, np.random.default_rng(3), 300)
    assert result["difference"] == pytest.approx(0.7) and result["p"] < 0.01


def test_pull_and_departure_bonds():
    events = pd.DataFrame({"initiator": ["DE00"] * 12 + ["DE01"] * 3, "partner": ["DE01"] * 12 + ["DE02"] * 3,
                           "outcome": ["pull"] * 9 + ["anchor"] * 3 + ["pull"] * 3})
    pulls = compute_pull_bonds(events, LOMS[:3])
    assert pulls[0, 1] == pulls[1, 0] == pytest.approx(0.75)
    assert np.isnan(pulls[1, 2])  # too few events
    departures = pd.DataFrame([(b, lom, True, float(k)) for b in range(5) for k, lom in enumerate(LOMS[:4])],
                              columns=["bout_id", "lom", "has_joined", "departure_min"])
    bonds = compute_departure_bonds(departures, LOMS[:4])
    assert bonds[0, 1] > bonds[0, 3]
    combined = combine_bonds({"a": bonds, "b": bonds})
    assert combined[0, 1] > combined[0, 3] and np.isnan(combined[0, 0])


def test_combined_hierarchy_and_cue_totals():
    loms = LOMS[:6]
    table = pd.DataFrame({"lom": loms, "dumont_share": np.linspace(0.3, 0.05, 6),
                          "lead_share": np.linspace(0.7, 0.3, 6),
                          "fence_first_share_raw": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5],
                          "paddock_entry_rank_raw": np.linspace(0.0, 1.0, 6)})
    hierarchy = compute_combined_hierarchy(table)
    assert hierarchy["lom"].iloc[0] == "DE00" and hierarchy["rank"].tolist() == [1, 2, 3, 4, 5, 6]
    assert hierarchy.set_index("lom").loc["DE00", "movement_rank"] == 1.0
    assert hierarchy.set_index("lom").loc["DE05", "exploration_rank"] == 3.5  # first at the fence, last into the paddock
    audio = np.ones((10, 6))
    pulse = np.zeros((10, 6))
    pulse[:5, 0] = 1.0
    with_cues = add_cue_totals(hierarchy, loms, audio, pulse).set_index("lom")
    assert with_cues.loc["DE00", "pulse_per_audio"] == 0.5 and with_cues.loc["DE01", "audio_total"] == 10
