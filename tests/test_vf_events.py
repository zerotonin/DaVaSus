"""Tests for :mod:`davasus.vf.events`."""

from __future__ import annotations

import pandas as pd
import pytest

from davasus.vf.events import parse_activation_label


@pytest.mark.parametrize(
    ("raw", "open_parcels", "new", "cut_off", "is_transition"),
    [
        ("luciano Heifers 2", (2,), None, None, False),
        ("heifers 3_2", (3, 2), 3, None, False),
        ("heifers 3_4", (3, 4), 4, 2, False),
        ("heifers 4_5", (4, 5), 5, 3, False),
        ("heifers 4_5 umstellung", (4, 5), 5, 3, True),
        ("heifers 5_6", (5, 6), 6, 4, False),
    ],
)
def test_parse_activation_label(raw, open_parcels, new, cut_off, is_transition):
    label = parse_activation_label(raw)
    assert label.open_parcels == open_parcels
    assert label.new_parcel == new
    assert label.cut_off_parcel == cut_off
    assert label.is_transition is is_transition


def test_parse_activation_label_rejects_text_without_parcel():
    with pytest.raises(ValueError):
        parse_activation_label("heifers")


def test_event_boundaries_ignore_one_stray_collar():
    from davasus.vf.events import ParcelEventBuilder

    def fix(neckband, stamp, label):
        return {
            "Neckband_Number": neckband,
            "Unix_Timestamp": f"2024-08-29 {stamp}:00+00:00",
            "eShepherd parcel activation": label,
            "comments": None,
            "parcel size [ha]": 1.0,
            "total length of VF": 1.0,
            "total time per parcel [min]": 1.0,
            "water point relocation": 0.0,
            "manually moving herd": 0.0,
            "plant biomass [mm]": None,
            "plant biomass [kg/ha]": None,
            "neckband problem with GPS": None,
            "neckband not activated for new parcel": None,
        }

    rows = [fix(n, "06:00", "heifers 4_5") for n in ("a", "b", "c")]
    rows += [fix("a", "08:18", "heifers 5_6")]
    rows += [fix(n, "18:11", "heifers 4_5") for n in ("b", "c")]
    rows += [fix(n, "18:12", "heifers 5_6") for n in ("b", "c")]
    events = ParcelEventBuilder().build(pd.DataFrame(rows))
    assert events["label"].tolist() == ["heifers 4_5", "heifers 5_6"]
    assert events.loc[1, "start_utc"] == "2024-08-29T18:12:00+00:00"
    assert events.loc[0, "end_utc"] == "2024-08-29T18:11:00+00:00"
