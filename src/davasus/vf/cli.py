# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — vf.cli                                                ║
# ║  « davasus-vf-ingest: CSV in, heifer_vf.db out »                 ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Paths default to local_paths.json; --csv and --db override.     ║
# ║  Progress via rich (ETL), plain logging with --no-progress.      ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Command-line entry points for the virtual-fence (M8) pipeline."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

from davasus.config import REPO_ROOT, LocalPathsError, load_local_paths
from davasus.vf.constants import FIGURE_SUBDIR, HEATMAP_BIN_M, MOVIE_FPS, MOVIE_FRAME_HOURS
from davasus.vf.ingest import CHUNK_ROWS, HeiferVFIngestor

TILE_CACHE_SUBDIR: str = "osm_tiles"
SPACE_WEATHER_SUBDIR: str = "space_weather"

log = logging.getLogger(__name__)


def build_ingest_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="davasus-vf-ingest",
        description="Ingest the heifer virtual-fence CSV into a fresh SQLite database.",
    )
    p.add_argument("--csv", type=Path, help="Heifer CSV (default: local_paths.json)")
    p.add_argument("--db", type=Path, help="Output database (default: <db_dir>/heifer_vf.db)")
    p.add_argument("--test-n", type=int, help="Read only the first N rows")
    p.add_argument("--chunk-rows", type=int, default=CHUNK_ROWS)
    p.add_argument("--overwrite", action="store_true", help="Delete an existing database first")
    p.add_argument("--no-progress", action="store_true", help="Log lines instead of a bar (SLURM)")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def ingest_main(argv: list[str] | None = None) -> int:
    """Run the heifer ingest; returns the process exit code."""
    args = build_ingest_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        csv_path, db_path = resolve_ingest_paths(args.csv, args.db)
    except LocalPathsError as exc:
        log.error("%s", exc)
        return 2
    if not csv_path.is_file():
        log.error("Heifer CSV not found: %s", csv_path)
        return 2
    if db_path.exists():
        if not args.overwrite:
            log.error("%s exists; pass --overwrite to rebuild it", db_path)
            return 2
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()
    connection = sqlite3.connect(db_path)
    try:
        ingestor = HeiferVFIngestor(connection, csv_path, chunk_rows=args.chunk_rows)
        counts = _run_with_progress(ingestor, args.test_n, show_bar=not args.no_progress)
    finally:
        connection.close()
    log.info("done in %.1f s -> %s", time.perf_counter() - started, db_path)
    for table, n in counts.items():
        log.info("  %-24s %d", table, n)
    return 0


def resolve_ingest_paths(csv: Path | None, db: Path | None) -> tuple[Path, Path]:
    """Explicit arguments win; anything missing comes from local_paths.json."""
    if csv is not None and db is not None:
        return csv, db
    paths = load_local_paths()
    return csv or paths.heifer_csv_path, db or paths.heifer_db_path


def _run_with_progress(
    ingestor: HeiferVFIngestor, test_n: int | None, show_bar: bool
) -> dict[str, int]:
    if not show_bar:
        return ingestor.run(
            test_n=test_n, progress=lambda n: log.info("chunk of %d rows written", n)
        )
    from rich.progress import BarColumn, MofNCompleteColumn, Progress, TimeElapsedColumn

    columns = ("[progress.description]{task.description}", BarColumn(),
               MofNCompleteColumn(), TimeElapsedColumn())
    with Progress(*columns) as bar:
        task = bar.add_task("heifer fixes", total=test_n or _count_rows(ingestor.csv_path))
        return ingestor.run(test_n=test_n, progress=lambda n: bar.advance(task, n))


def _count_rows(path: Path) -> int:
    with open(path, "rb") as fh:
        return sum(chunk.count(b"\n") for chunk in iter(lambda: fh.read(1 << 24), b"")) - 1


# ┌────────────────────────────────────────────────────────────┐
# │ Figures  « davasus-vf-figures »                            │
# └────────────────────────────────────────────────────────────┘


def build_figures_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="davasus-vf-figures",
        description="First-look M8 figures: 24 h trajectories, cue timeline, herd movie.",
    )
    p.add_argument("--db", type=Path, help="heifer_vf.db (default: local_paths.json)")
    p.add_argument("--out", type=Path, default=REPO_ROOT / "figures" / FIGURE_SUBDIR)
    p.add_argument("--lom", help="Focal ear tag (default: most audio cues)")
    p.add_argument("--dates", nargs="+",
                   help="UTC days for trajectories (default: first VF cut-off day and the day before)")
    p.add_argument("--no-movie", action="store_true")
    p.add_argument("--fps", type=int, default=MOVIE_FPS)
    p.add_argument("--bin-m", type=float, default=HEATMAP_BIN_M)
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def figures_main(argv: list[str] | None = None) -> int:
    """Write the first-look figures; returns the process exit code."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from davasus.vf.geometry import LocalProjection
    from davasus.vf.learning import compute_daily_stimulus_summary
    from davasus.vf.load import find_most_stimulated_heifer, load_events, load_fixes
    from davasus.vf.movie import compute_occupancy_frames, render_occupancy_movie, summarise_frames
    from davasus.vf.viz import plot_daily_trajectory, plot_stimulus_timeline
    from davasus.vf.viz_3d import plot_cue_landscape_3d
    from davasus.viz import save_triplet

    args = build_figures_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        db_path = args.db or load_local_paths().heifer_db_path
    except LocalPathsError as exc:
        log.error("%s", exc)
        return 2
    if not db_path.is_file():
        log.error("Database not found: %s (run davasus-vf-ingest first)", db_path)
        return 2

    connection = sqlite3.connect(db_path)
    try:
        fixes = load_fixes(connection)
        events = load_events(connection)
        lom = args.lom or find_most_stimulated_heifer(connection)
    finally:
        connection.close()
    projection = LocalProjection.from_fixes(fixes)
    log.info("focal heifer %s; projection centre %.5f N %.5f E", lom,
             projection.lat0, projection.lon0)

    for day in args.dates or default_trajectory_days(events):
        start = pd.Timestamp(day, tz="UTC")
        day_fixes = fixes[(fixes["timestamp"] >= start)
                          & (fixes["timestamp"] < start + pd.Timedelta(days=1))]
        fig, data = plot_daily_trajectory(day_fixes, lom, projection)
        written = save_triplet(fig, data, args.out / f"trajectory_24h_{lom}_{start:%Y%m%d}")
        plt.close(fig)
        log.info("wrote %s", written["png"])

    daily = compute_daily_stimulus_summary(fixes)
    fig, data = plot_stimulus_timeline(daily, events, lom)
    written = save_triplet(fig, data, args.out / f"cue_timeline_{lom}")
    plt.close(fig)
    daily.to_csv(args.out / "cue_daily_all_heifers.csv", index=False)
    log.info("wrote %s", written["png"])

    for asinh, stem in ((False, "cue_landscape_3d"), (True, "cue_landscape_3d_asinh")):
        fig, data = plot_cue_landscape_3d(daily, events, asinh=asinh)
        written = save_triplet(fig, data, args.out / stem)
        plt.close(fig)
        log.info("wrote %s", written["png"])

    if not args.no_movie:
        frames = compute_occupancy_frames(fixes, projection, args.bin_m, MOVIE_FRAME_HOURS)
        movie = args.out / "herd_occupancy_1day_per_s.mp4"
        log.info("rendering %d frames at %d fps", len(frames.starts), args.fps)
        render_occupancy_movie(frames, events, movie, fps=args.fps)
        summarise_frames(frames).to_csv(movie.with_suffix(".csv"), index=False)
        log.info("wrote %s", movie)
    return 0


def default_trajectory_days(events: pd.DataFrame) -> list[str]:
    """First virtual-fence cut-off day (not manual herding) and the day before."""
    cut_offs = events[events["cut_off_parcel"].notna() & (events["manual_herding"] == 0)]
    if cut_offs.empty:
        return [events["start_utc"].min().strftime("%Y-%m-%d")]
    day = cut_offs["start_utc"].min().floor("D")
    return [(day - pd.Timedelta(days=1)).strftime("%Y-%m-%d"), day.strftime("%Y-%m-%d")]


# ┌────────────────────────────────────────────────────────────┐
# │ Dashboard  « davasus-vf-dashboard »                        │
# └────────────────────────────────────────────────────────────┘


def build_dashboard_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="davasus-vf-dashboard",
        description="Interactive heifer dashboard: map, herd, weather and space weather.",
    )
    p.add_argument("--db", type=Path, help="heifer_vf.db (default: local_paths.json)")
    p.add_argument("--tile-cache", type=Path,
                   help="OSM tile cache (default: <data_root>/osm_tiles)")
    p.add_argument("--no-tiles", action="store_true", help="No OpenStreetMap basemap")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def dashboard_main(argv: list[str] | None = None) -> int:
    """Open the desktop dashboard; returns the Qt exit code."""
    args = build_dashboard_parser().parse_args(argv)
    _configure_logging(args.verbose)
    try:
        db_path, data_root = _resolve_db_and_root(args.db, need_root=args.tile_cache is None
                                                  and not args.no_tiles)
    except LocalPathsError as exc:
        log.error("%s", exc)
        return 2
    if not db_path.is_file():
        log.error("Database not found: %s (run davasus-vf-ingest first)", db_path)
        return 2
    tile_dir = None if args.no_tiles else (args.tile_cache or data_root / TILE_CACHE_SUBDIR)
    try:
        from davasus.vf.dashboard.app import run_dashboard
    except ImportError as exc:
        log.error("GUI dependencies missing (%s); pip install -e \".[gui]\"", exc)
        return 2
    return run_dashboard(db_path, tile_dir)


# ┌────────────────────────────────────────────────────────────┐
# │ Space weather  « davasus-vf-spaceweather »                 │
# └────────────────────────────────────────────────────────────┘


def build_spaceweather_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="davasus-vf-spaceweather",
        description="Fetch GFZ Hp30 / ap30 / Kp for the trial window into heifer_vf.db.",
    )
    p.add_argument("--db", type=Path, help="heifer_vf.db (default: local_paths.json)")
    p.add_argument("--cache", type=Path,
                   help="JSON download cache (default: <data_root>/space_weather)")
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def spaceweather_main(argv: list[str] | None = None) -> int:
    """Download (or reuse cached) indices and upsert them; returns the exit code."""
    from davasus.vf.gnss_quality import SpaceWeatherFetcher, write_space_weather

    args = build_spaceweather_parser().parse_args(argv)
    _configure_logging(args.verbose)
    try:
        db_path, data_root = _resolve_db_and_root(args.db, need_root=args.cache is None)
    except LocalPathsError as exc:
        log.error("%s", exc)
        return 2
    if not db_path.is_file():
        log.error("Database not found: %s (run davasus-vf-ingest first)", db_path)
        return 2
    connection = sqlite3.connect(db_path)
    try:
        first, last = connection.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM heifer_fixes").fetchone()
        if first is None:
            log.error("heifer_fixes is empty")
            return 2
        start = pd.Timestamp(first).floor("D").tz_localize(None)
        end = (pd.Timestamp(last).floor("D") + pd.Timedelta(days=1)).tz_localize(None)
        fetcher = SpaceWeatherFetcher(args.cache or data_root / SPACE_WEATHER_SUBDIR)
        table = fetcher.fetch_hp30(str(start), str(end))
        n = write_space_weather(connection, table)
    finally:
        connection.close()
    log.info("wrote %d space-weather rows (%s to %s); data: GFZ Potsdam, CC BY 4.0", n,
             start.date(), end.date())
    return 0


# ┌────────────────────────────────────────────────────────────┐
# │ Leadership  « davasus-vf-leadership »                      │
# └────────────────────────────────────────────────────────────┘

LEADERSHIP_SUBDIR: str = "dumont"


def build_leadership_parser() -> argparse.ArgumentParser:
    from davasus.vf.constants import BOOTSTRAP_DRAWS, PERMUTATIONS, RANDOM_SEED

    p = argparse.ArgumentParser(
        prog="davasus-vf-leadership",
        description="Dumont (2005) leadership: first movers of spontaneous group movements.",
    )
    p.add_argument("--db", type=Path, help="heifer_vf.db (default: local_paths.json)")
    p.add_argument("--out", type=Path, default=REPO_ROOT / "figures" / FIGURE_SUBDIR / LEADERSHIP_SUBDIR)
    p.add_argument("--permutations", type=int, default=PERMUTATIONS)
    p.add_argument("--bootstrap", type=int, default=BOOTSTRAP_DRAWS)
    p.add_argument("--seed", type=int, default=RANDOM_SEED)
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def leadership_main(argv: list[str] | None = None) -> int:
    """Run the Dumont analysis and write tables plus the six-panel figure."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    from davasus.vf.dashboard.data import load_dashboard_data
    from davasus.vf.dumont import run_dumont_analysis
    from davasus.vf.initiation import build_herd_tracks
    from davasus.vf.viz_leadership import plot_dumont_leadership
    from davasus.viz import save_triplet

    args = build_leadership_parser().parse_args(argv)
    _configure_logging(args.verbose)
    try:
        db_path, _ = _resolve_db_and_root(args.db, need_root=False)
    except LocalPathsError as exc:
        log.error("%s", exc)
        return 2
    if not db_path.is_file():
        log.error("Database not found: %s (run davasus-vf-ingest first)", db_path)
        return 2
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        data = load_dashboard_data(connection)
    finally:
        connection.close()
    if not data.has_space_weather:
        log.warning("no space weather in the database; reduced-GPS minutes are not excluded "
                    "(run davasus-vf-spaceweather)")
    tracks = build_herd_tracks(data.minutes, data.loms, data.x_m, data.y_m, data.events,
                               data.is_gps_reduced)
    result = run_dumont_analysis(tracks, np.random.default_rng(args.seed),
                                 args.permutations, args.bootstrap)
    args.out.mkdir(parents=True, exist_ok=True)
    for name in ("bouts", "per_bout", "lags", "nulls", "order_w", "attempts", "attempt_tests"):
        getattr(result, name).to_csv(args.out / f"{name}.csv", index=False)
    fig, table = plot_dumont_leadership(result)
    written = save_triplet(fig, table, args.out / "dumont_leadership")
    plt.close(fig)
    log.info("wrote %s and tables in %s", written["png"].name, args.out)
    return 0


# ┌────────────────────────────────────────────────────────────┐
# │ Formation  « davasus-vf-formation »                        │
# └────────────────────────────────────────────────────────────┘

FORMATION_SUBDIR: str = "formation"


def build_formation_parser() -> argparse.ArgumentParser:
    from davasus.vf.constants import BOOTSTRAP_DRAWS, PERMUTATIONS, RANDOM_SEED

    p = argparse.ArgumentParser(
        prog="davasus-vf-formation",
        description="Positional leadership: who holds the leading edge of the moving herd.",
    )
    p.add_argument("--db", type=Path, help="heifer_vf.db (default: local_paths.json)")
    p.add_argument("--out", type=Path, default=REPO_ROOT / "figures" / FIGURE_SUBDIR / FORMATION_SUBDIR)
    p.add_argument("--permutations", type=int, default=PERMUTATIONS)
    p.add_argument("--bootstrap", type=int, default=BOOTSTRAP_DRAWS)
    p.add_argument("--seed", type=int, default=RANDOM_SEED)
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def formation_main(argv: list[str] | None = None) -> int:
    """Run the formation analysis and write tables plus the four-panel figure."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    from davasus.vf.formation_analysis import run_formation_analysis
    from davasus.vf.viz_formation import plot_formation
    from davasus.viz import save_triplet

    args = build_formation_parser().parse_args(argv)
    _configure_logging(args.verbose)
    tracks = _load_herd_tracks(args.db)
    if tracks is None:
        return 2
    result = run_formation_analysis(tracks, np.random.default_rng(args.seed),
                                    args.permutations, args.bootstrap)
    args.out.mkdir(parents=True, exist_ok=True)
    for name in ("tests", "headings", "lags"):
        getattr(result, name).to_csv(args.out / f"{name}.csv", index=False)
    fig, table = plot_formation(result)
    written = save_triplet(fig, table, args.out / "formation_leading_edge")
    plt.close(fig)
    log.info("wrote %s and tables in %s", written["png"].name, args.out)
    return 0


def _load_herd_tracks(db: Path | None):
    """Gap-filled herd tracks from heifer_vf.db, or None after logging the reason."""
    from davasus.vf.dashboard.data import load_dashboard_data
    from davasus.vf.initiation import build_herd_tracks

    try:
        db_path, _ = _resolve_db_and_root(db, need_root=False)
    except LocalPathsError as exc:
        log.error("%s", exc)
        return None
    if not db_path.is_file():
        log.error("Database not found: %s (run davasus-vf-ingest first)", db_path)
        return None
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        data = load_dashboard_data(connection)
    finally:
        connection.close()
    if not data.has_space_weather:
        log.warning("no space weather in the database; reduced-GPS minutes are not excluded "
                    "(run davasus-vf-spaceweather)")
    return build_herd_tracks(data.minutes, data.loms, data.x_m, data.y_m, data.events,
                             data.is_gps_reduced)


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _resolve_db_and_root(db: Path | None, need_root: bool) -> tuple[Path, Path | None]:
    """Database path and data_root; local_paths.json only when something is missing."""
    if db is not None and not need_root:
        return db, None
    paths = load_local_paths()
    return db or paths.heifer_db_path, paths.data_root


if __name__ == "__main__":
    sys.exit(ingest_main())
