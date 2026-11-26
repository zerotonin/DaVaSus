# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.paddocks                                           ║
# ║  « eShepherd parcels in, paddocks A-F out »                      ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Reads the eShepherd parcel shapefiles (vertex points with the   ║
# ║  virtual-fence edges marked), cuts the six fixed paddocks        ║
# ║  (Parzelle 1-6 = A-F) out of overlapping parcels and records     ║
# ║  which paddocks each parcel holds open, plus the surveyed tree   ║
# ║  patches. All of it lands in heifer_vf.db so front ends never    ║
# ║  touch a shapefile.                                              ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Parcel outlines and paddocks A-F from the eShepherd shapefiles."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import shapefile
from shapely import wkb
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from davasus.vf.constants import (
    PADDOCK_OPEN_SHARE,
    PADDOCK_RECIPES,
    PADDOCK_SLIVER_M2,
    TREE_GEOPACKAGE,
    PaddockRecipe,
)
from davasus.vf.geometry import LocalProjection

log = logging.getLogger(__name__)

GEOGRAPHIC_PRJ_TAG: str = "GEOGCS"
PARCEL_FIELDS: frozenset[str] = frozenset({"id", "VF"})


@dataclass(frozen=True)
class ParcelOutline:
    """One eShepherd parcel: vertices in boundary order, VF flag per edge.

    ``is_vf_edge[k]`` marks the edge from vertex k to vertex k + 1 (wrapping).
    """

    label: str
    lat: np.ndarray
    lon: np.ndarray
    is_vf_edge: np.ndarray


def build_paddock_tables(shape_dir: Path) -> dict[str, pd.DataFrame]:
    """Read every parcel shapefile and derive the paddocks.

    Returns:
        Frames for the tables ``parcel_outlines``, ``paddocks`` and
        ``parcel_paddocks``.
    """
    outlines = [load_parcel_outline(path) for path in sorted(shape_dir.glob("*.shp"))
                if is_parcel_shapefile(path)]
    if not outlines:
        raise FileNotFoundError(f"no parcel shapefiles in {shape_dir}")
    projection = LocalProjection(float(np.mean(np.concatenate([o.lat for o in outlines]))),
                                 float(np.mean(np.concatenate([o.lon for o in outlines]))))
    parcels = {o.label: build_polygon(o, projection) for o in outlines}
    paddocks = derive_paddocks(parcels)
    tables = {
        "parcel_outlines": outlines_to_frame(outlines),
        "paddocks": paddocks_to_frame(paddocks, projection),
        "parcel_paddocks": compute_open_paddocks(parcels, paddocks),
    }
    geopackage = shape_dir / TREE_GEOPACKAGE
    if geopackage.is_file():
        tables["tree_patches"] = load_tree_patches(geopackage)
    else:
        log.warning("no %s in %s; tree patches not updated", TREE_GEOPACKAGE, shape_dir)
    return tables


def write_paddock_tables(connection: sqlite3.Connection, tables: dict[str, pd.DataFrame]) -> None:
    """Replace the contents of the three geometry tables."""
    for name, frame in tables.items():
        connection.execute(f"DELETE FROM {name}")
        columns = ", ".join(frame.columns)
        marks = ", ".join("?" * len(frame.columns))
        connection.executemany(f"INSERT INTO {name} ({columns}) VALUES ({marks})",
                               frame.itertuples(index=False, name=None))
    connection.commit()


# ┌────────────────────────────────────────────────────────────┐
# │ Shapefiles  « vertices sorted by id, VF edges »            │
# └────────────────────────────────────────────────────────────┘


def is_parcel_shapefile(path: Path) -> bool:
    """True for eShepherd parcel exports (``id`` and ``VF`` fields), not surveys."""
    fields = {f[0] for f in shapefile.Reader(str(path), encoding="utf-8").fields[1:]}
    if PARCEL_FIELDS.issubset(fields):
        return True
    log.info("skipping %s: no %s fields", path.name, "/".join(sorted(PARCEL_FIELDS)))
    return False


def load_parcel_outline(path: Path) -> ParcelOutline:
    """Vertex points of one parcel; ``VF = j`` on vertex k marks the edge k -> j."""
    prj = path.with_suffix(".prj")
    if prj.is_file() and GEOGRAPHIC_PRJ_TAG not in prj.read_text(encoding="utf-8"):
        raise ValueError(f"{path.name}: expected geographic (lat/lon) coordinates")
    reader = shapefile.Reader(str(path), encoding="utf-8")
    records = pd.DataFrame([list(r) for r in reader.records()],
                           columns=[f[0] for f in reader.fields[1:]])
    records["lon"] = [s.points[0][0] for s in reader.shapes()]
    records["lat"] = [s.points[0][1] for s in reader.shapes()]
    records = records.sort_values("id").reset_index(drop=True)
    ids = records["id"].astype(int).to_numpy()
    following = np.roll(ids, -1)
    is_vf_edge = np.zeros(len(ids), dtype=bool)
    for k, target in enumerate(pd.to_numeric(records["VF"], errors="coerce")):
        if pd.isna(target):
            continue
        if int(target) == following[k]:
            is_vf_edge[k] = True
        else:
            log.warning("%s: VF edge %d -> %d skips the boundary order", path.stem, ids[k], int(target))
    return ParcelOutline(path.stem, records["lat"].to_numpy(float), records["lon"].to_numpy(float),
                         is_vf_edge)


def build_polygon(outline: ParcelOutline, projection: LocalProjection) -> Polygon:
    x, y = projection.to_metres(outline.lat, outline.lon)
    polygon = Polygon(np.c_[x, y])
    if not polygon.is_valid:
        raise ValueError(f"{outline.label}: vertices sorted by id do not form a valid polygon")
    return polygon


# ┌────────────────────────────────────────────────────────────┐
# │ Paddocks  « cut from overlapping parcels »                 │
# └────────────────────────────────────────────────────────────┘


def derive_paddocks(parcels: dict[str, Polygon]) -> dict[str, tuple[PaddockRecipe, BaseGeometry]]:
    """Apply ``PADDOCK_RECIPES``; slivers below ``PADDOCK_SLIVER_M2`` are dropped."""
    paddocks: dict[str, tuple[PaddockRecipe, BaseGeometry]] = {}
    for recipe in PADDOCK_RECIPES:
        missing = [label for label in recipe.intersect if label not in parcels]
        if missing:
            log.warning("paddock %s skipped: parcel(s) %s missing", recipe.letter, ", ".join(missing))
            continue
        shape = parcels[recipe.intersect[0]]
        for label in recipe.intersect[1:]:
            shape = shape.intersection(parcels[label])
        for letter in recipe.minus:
            if letter in paddocks:
                shape = shape.difference(paddocks[letter][1])
        paddocks[recipe.letter] = (recipe, drop_slivers(shape))
    return paddocks


def drop_slivers(shape: BaseGeometry) -> BaseGeometry:
    parts = [p for p in getattr(shape.buffer(0), "geoms", [shape.buffer(0)])
             if isinstance(p, Polygon) and p.area >= PADDOCK_SLIVER_M2]
    return parts[0] if len(parts) == 1 else MultiPolygon(parts)


def compute_open_paddocks(parcels: dict[str, Polygon],
                          paddocks: dict[str, tuple[PaddockRecipe, BaseGeometry]]) -> pd.DataFrame:
    """Paddocks with at least ``PADDOCK_OPEN_SHARE`` of their area inside each parcel."""
    rows = []
    for label, parcel in parcels.items():
        for letter, (_, shape) in paddocks.items():
            share = parcel.intersection(shape).area / shape.area
            if share >= PADDOCK_OPEN_SHARE:
                rows.append((label, letter, round(share, 3)))
    return pd.DataFrame(rows, columns=["label", "paddock", "share"])


# ┌────────────────────────────────────────────────────────────┐
# │ Tree patches  « polygon layers of the forest geopackage »  │
# └────────────────────────────────────────────────────────────┘


def load_tree_patches(geopackage: Path) -> pd.DataFrame:
    """Every polygon layer of the geopackage as rings in lat/lon.

    Read with sqlite3, as a GeoPackage is SQLite with geometry blobs
    (``GP`` header, optional envelope, then WKB). Point layers such as the
    RTK survey are skipped.
    """
    with closing(sqlite3.connect(f"file:{geopackage}?mode=ro", uri=True)) as connection:
        layers = connection.execute(
            "SELECT c.table_name, g.column_name FROM gpkg_contents c "
            "JOIN gpkg_geometry_columns g USING (table_name) WHERE c.data_type = 'features'").fetchall()
        rows = []
        for layer, column in layers:
            for (blob,) in connection.execute(f'SELECT "{column}" FROM "{layer}"'):
                shape = parse_geopackage_geometry(blob)
                if not isinstance(shape, Polygon | MultiPolygon):
                    break
                rows += _rings_to_rows(layer, shape)
    frame = pd.DataFrame(rows, columns=["patch", "ring", "vertex", "lat", "lon"])
    if not frame.empty and not (frame["lat"].abs().le(90).all() and frame["lon"].abs().le(180).all()):
        raise ValueError(f"{geopackage.name}: tree patches are not in lat/lon")
    return frame


def parse_geopackage_geometry(blob: bytes) -> BaseGeometry:
    """Strip the GeoPackage header (8 bytes plus envelope) and decode the WKB."""
    envelope_bytes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[(blob[3] >> 1) & 0b111]
    return wkb.loads(bytes(blob[8 + envelope_bytes:]))


def _rings_to_rows(layer: str, shape: BaseGeometry) -> list[tuple]:
    rows = []
    for ring, part in enumerate(getattr(shape, "geoms", [shape])):
        lon, lat = np.asarray(part.exterior.coords)[:-1, :2].T
        rows += [(layer, ring, k, float(a), float(o)) for k, (a, o) in enumerate(zip(lat, lon, strict=True))]
    return rows


# ┌────────────────────────────────────────────────────────────┐
# │ Tables  « rows for heifer_vf.db »                          │
# └────────────────────────────────────────────────────────────┘


def outlines_to_frame(outlines: list[ParcelOutline]) -> pd.DataFrame:
    return pd.concat([pd.DataFrame({"label": o.label, "vertex": np.arange(len(o.lat)), "lat": o.lat,
                                    "lon": o.lon, "is_vf_edge": o.is_vf_edge.astype(int)})
                      for o in outlines], ignore_index=True)


def paddocks_to_frame(paddocks: dict[str, tuple[PaddockRecipe, BaseGeometry]],
                      projection: LocalProjection) -> pd.DataFrame:
    """Exterior ring of each part, open (first vertex not repeated)."""
    rows = []
    for letter, (recipe, shape) in paddocks.items():
        for ring, part in enumerate(getattr(shape, "geoms", [shape])):
            x, y = np.asarray(part.exterior.coords)[:-1].T
            lat, lon = projection.to_degrees(x, y)
            rows += [(letter, recipe.parzelle, ring, k, float(a), float(o))
                     for k, (a, o) in enumerate(zip(lat, lon, strict=True))]
    return pd.DataFrame(rows, columns=["paddock", "parzelle", "ring", "vertex", "lat", "lon"])


def summarise_paddocks(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Area per paddock and the parcels that hold it open, for the log."""
    projection = LocalProjection(float(tables["paddocks"]["lat"].mean()),
                                 float(tables["paddocks"]["lon"].mean()))
    rows = []
    for (letter, parzelle), group in tables["paddocks"].groupby(["paddock", "parzelle"]):
        area = sum(Polygon(np.c_[projection.to_metres(ring["lat"], ring["lon"])]).area
                   for _, ring in group.groupby("ring"))
        parcels = tables["parcel_paddocks"].loc[tables["parcel_paddocks"]["paddock"] == letter, "label"]
        rows.append({"paddock": letter, "parzelle": parzelle, "area_ha": round(area / 1e4, 3),
                     "open_in": ", ".join(parcels)})
    union = unary_union([Polygon(np.c_[projection.to_metres(g["lat"], g["lon"])])
                         for _, g in tables["paddocks"].groupby(["paddock", "ring"])])
    log.info("paddocks cover %.2f ha", union.area / 1e4)
    return pd.DataFrame(rows).sort_values("parzelle")
