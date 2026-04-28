"""Tests for :class:`davasus.ingest_merged.MergedIngestor`."""

from __future__ import annotations

from davasus.db import Database
from davasus.ingest_merged import MergedIngestor


def test_merged_split_eshepherd_and_smaxtec(database: Database, merged_csv):
    """All three rows land in eshepherd; only female rows in smaxtec."""
    counts = MergedIngestor(database).ingest(merged_csv)
    assert counts == {"eshepherd": 3, "smaxtec": 2}
    assert database.count_rows("eshepherd") == 3
    assert database.count_rows("smaxtec") == 2


def test_merged_dim_tables_populated(database: Database, merged_csv):
    """Three neckbands and three animals are registered."""
    MergedIngestor(database).ingest(merged_csv)
    assert database.count_rows("neckbands") == 3
    assert database.count_rows("animals") == 3


def test_merged_strips_fence_sentinel(database: Database, merged_csv):
    """The sentinel-only collar row stores NULL fence distances."""
    MergedIngestor(database).ingest(merged_csv)
    cur = database.connection.execute(
        "SELECT fence_dist_max, fence_dist_min FROM eshepherd "
        "WHERE neckband_id = 'n0002'"
    )
    row = cur.fetchone()
    assert row == (None, None)


def test_merged_skips_smaxtec_for_collar_only(database: Database, merged_csv):
    """The male / collar-only animal has no smaxtec row."""
    MergedIngestor(database).ingest(merged_csv)
    cur = database.connection.execute(
        "SELECT COUNT(*) FROM smaxtec WHERE animal_id = 1003"
    )
    (n,) = cur.fetchone()
    assert n == 0


def test_merged_imu_ticks_are_int(database: Database, merged_csv):
    """IMU tick counts are stored as INTEGER."""
    MergedIngestor(database).ingest(merged_csv)
    cur = database.connection.execute(
        "SELECT imu_tick_40mg FROM eshepherd WHERE neckband_id = 'n0001'"
    )
    (val,) = cur.fetchone()
    assert isinstance(val, int)
    assert val == 61
