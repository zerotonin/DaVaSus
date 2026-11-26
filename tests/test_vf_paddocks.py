"""Paddocks A-F from synthetic eShepherd parcel shapefiles, and their dashboard use."""

from __future__ import annotations

import sqlite3

import numpy as np
import pytest
import shapefile

from davasus.vf.geometry import LocalProjection
from davasus.vf.paddocks import build_paddock_tables, summarise_paddocks, write_paddock_tables
from davasus.vf.schema import VFSchema
from tests.test_vf_dashboard import vf_db  # noqa: F401  (fixture)
from tests.test_vf_ingest import heifer_csv  # noqa: F401  (fixture)

PROJECTION = LocalProjection(52.397, 14.277)
WGS84_PRJ = ('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],'
             'PRIMEM["Greenwich",0],UNIT["Degree",0.0174532925199433]]')
# Paddock rectangles (x0, x1, y0, y1) in metres, arranged like the farm map.
PADDOCK_BOXES = {
    "C": (0, 100, 0, 100), "B": (100, 200, 0, 100), "A": (200, 250, 0, 100),
    "D": (0, 100, 100, 200), "E": (100, 200, 100, 200), "F": (200, 300, 100, 200),
}
# Parcel outlines (counter-clockwise vertices) and the index of their VF edge.
PARCELS = {
    "luciano Heifers 2": ([(100, 0), (250, 0), (250, 100), (100, 100)], 3),
    "heifers 3_2": ([(0, 0), (200, 0), (200, 100), (0, 100)], 1),
    "heifers 3_4": ([(0, 0), (100, 0), (100, 200), (0, 200)], 1),
    "heifers 4_5 umstellung": ([(0, 0), (100, 0), (100, 100), (200, 100), (200, 200), (0, 200)], 1),
    "heifers 4_5": ([(0, 100), (200, 100), (200, 200), (0, 200)], 0),
    "heifers 5_6": ([(100, 100), (300, 100), (300, 200), (100, 200)], 3),
}
EXPECTED_OPEN = {
    "luciano Heifers 2": {"A", "B"}, "heifers 3_2": {"B", "C"}, "heifers 3_4": {"C", "D"},
    "heifers 4_5 umstellung": {"C", "D", "E"}, "heifers 4_5": {"D", "E"}, "heifers 5_6": {"E", "F"},
}


def _write_parcel(folder, label: str, corners: list[tuple[float, float]], vf_edge: int) -> None:
    """Point shapefile in shuffled file order, ids 1..n, ``VF`` on the VF edge's start."""
    writer = shapefile.Writer(str(folder / label), shapeType=shapefile.POINT)
    writer.field("id", "N", 5)
    writer.field("VF", "N", 5)
    writer.field("FF", "N", 5)
    n = len(corners)
    for k in reversed(range(n)):
        lat, lon = PROJECTION.to_degrees(np.array([corners[k][0]]), np.array([corners[k][1]]))
        writer.point(float(lon[0]), float(lat[0]))
        writer.record(k + 1, (vf_edge + 1) % n + 1 if k == vf_edge else None, None)
    writer.close()
    (folder / f"{label}.prj").write_text(WGS84_PRJ, encoding="utf-8")


@pytest.fixture
def shape_dir(tmp_path):
    for label, (corners, vf_edge) in PARCELS.items():
        _write_parcel(tmp_path, label, corners, vf_edge)
    survey = shapefile.Writer(str(tmp_path / "survey"), shapeType=shapefile.POINT)
    survey.field("Name", "C", 10)
    survey.point(14.277, 52.397)
    survey.record("1")
    survey.close()
    return tmp_path


@pytest.fixture
def tables(shape_dir):
    return build_paddock_tables(shape_dir)


def test_paddock_areas_match_the_boxes(tables):
    summary = summarise_paddocks(tables).set_index("paddock")
    for letter, (x0, x1, y0, y1) in PADDOCK_BOXES.items():
        assert summary.loc[letter, "area_ha"] == pytest.approx((x1 - x0) * (y1 - y0) / 1e4, rel=0.01)
    assert summary["parzelle"].tolist() == [1, 2, 3, 4, 5, 6]


def test_open_paddocks_per_parcel(tables):
    open_paddocks = tables["parcel_paddocks"].groupby("label")["paddock"].apply(set).to_dict()
    assert open_paddocks == EXPECTED_OPEN


def test_vertices_sorted_by_id_and_vf_edge_flagged(tables):
    outlines = tables["parcel_outlines"]
    assert outlines["label"].nunique() == len(PARCELS)
    assert (outlines.groupby("label")["is_vf_edge"].sum() == 1).all()
    first = outlines[outlines["label"] == "heifers 3_2"].sort_values("vertex")
    x, y = PROJECTION.to_metres(first["lat"], first["lon"])
    assert np.allclose(np.c_[x, y], PARCELS["heifers 3_2"][0], atol=0.01)
    assert first["is_vf_edge"].tolist() == [0, 1, 0, 0]


def test_tables_round_trip_through_sqlite(tables, tmp_path):
    connection = sqlite3.connect(tmp_path / "geo.db")
    VFSchema(connection).create_all()
    write_paddock_tables(connection, tables)
    write_paddock_tables(connection, tables)
    count = connection.execute("SELECT COUNT(DISTINCT paddock) FROM paddocks").fetchone()[0]
    assert count == len(PADDOCK_BOXES)
    connection.close()


def test_dashboard_names_open_paddocks(tables, vf_db):  # noqa: F811
    from davasus.vf.dashboard.data import load_dashboard_data
    from davasus.vf.dashboard.summaries import describe_event

    vf_db.execute("UPDATE parcel_events SET label = 'heifers 3_4', cut_off_parcel = 2")
    VFSchema(vf_db).create_all()
    write_paddock_tables(vf_db, tables)
    data = load_dashboard_data(vf_db)
    assert data.has_paddocks and data.event_paddocks[0] == ("C", "D")
    rows = dict(describe_event(data, int(np.flatnonzero(data.event_index >= 0)[0])))
    assert rows["open paddocks"] == "C (3), D (4)"
    assert rows["cut-off paddock"] == "B (2)"


def test_map_highlights_the_active_parcel(tables, vf_db):  # noqa: F811
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication

    from davasus.vf.dashboard.data import load_dashboard_data
    from davasus.vf.dashboard.qt_map import HerdMapWidget

    QApplication.instance() or QApplication([])
    vf_db.execute("UPDATE parcel_events SET label = 'heifers 5_6'")
    VFSchema(vf_db).create_all()
    write_paddock_tables(vf_db, tables)
    data = load_dashboard_data(vf_db)
    widget = HerdMapWidget(data, tiles=None)
    widget.show_parcel(0)
    assert len(widget._open_fills) == 2
    assert np.isfinite(widget._virtual_fence.getData()[0]).sum() == 2
    widget.show_parcel(-1)
    assert widget._open_fills == []


def _write_tree_geopackage(path, ring: list[tuple[float, float]]) -> None:
    """Minimal GeoPackage: one polygon layer (no envelope) and one point layer."""
    from shapely.geometry import Point, Polygon

    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE gpkg_contents (table_name TEXT, data_type TEXT);"
        "CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT);"
        'CREATE TABLE "Wald auf Parzelle 9" (fid INTEGER, geom BLOB);'
        "CREATE TABLE survey (fid INTEGER, geom BLOB);")
    header = b"GP\x00\x01" + (0).to_bytes(4, "little")
    lonlat = [(float(o[0]), float(a[0])) for a, o in
              (PROJECTION.to_degrees(np.array([x]), np.array([y])) for x, y in ring)]
    for table, shape in (("Wald auf Parzelle 9", Polygon(lonlat)), ("survey", Point(lonlat[0]))):
        connection.execute("INSERT INTO gpkg_contents VALUES (?, 'features')", (table,))
        connection.execute("INSERT INTO gpkg_geometry_columns VALUES (?, 'geom')", (table,))
        connection.execute(f'INSERT INTO "{table}" VALUES (1, ?)', (header + shape.wkb,))
    connection.commit()
    connection.close()


def test_tree_patches_from_geopackage_reach_the_map(shape_dir, vf_db):  # noqa: F811
    pytest.importorskip("PySide6")
    from PySide6.QtWidgets import QApplication

    from davasus.vf.constants import TREE_GEOPACKAGE
    from davasus.vf.dashboard.data import load_dashboard_data
    from davasus.vf.dashboard.qt_map import HerdMapWidget

    _write_tree_geopackage(shape_dir / TREE_GEOPACKAGE, [(120, 120), (160, 120), (160, 150), (120, 150)])
    tables = build_paddock_tables(shape_dir)
    trees = tables["tree_patches"]
    assert trees["patch"].unique().tolist() == ["Wald auf Parzelle 9"] and len(trees) == 4
    VFSchema(vf_db).create_all()
    write_paddock_tables(vf_db, tables)
    data = load_dashboard_data(vf_db)
    (rx, ry), = data.trees["Wald auf Parzelle 9"]
    area = 0.5 * abs(np.dot(rx, np.roll(ry, 1)) - np.dot(ry, np.roll(rx, 1)))
    shift_x, shift_y = data.projection.to_metres(*PROJECTION.to_degrees(np.array([0.0]), np.array([0.0])))
    assert area == pytest.approx(40 * 30, rel=0.01)
    assert rx.min() - shift_x[0] == pytest.approx(120, abs=0.5)
    QApplication.instance() or QApplication([])
    HerdMapWidget(data, tiles=None)
