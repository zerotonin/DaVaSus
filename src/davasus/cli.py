"""Command-line entry points for DaVaSus.

Exposes ``davasus-ingest`` (:func:`ingest_main`) and ``davasus-validate``
(:func:`validate_main`).
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

from davasus.db import Database
from davasus.ingest_merged import MergedIngestor
from davasus.ingest_weather import WeatherIngestor
from davasus.validate import Validator, render_report

log = logging.getLogger("davasus")


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build the ``davasus-ingest`` argument parser.

    Returns:
        Configured :class:`argparse.ArgumentParser`.
    """
    p = argparse.ArgumentParser(
        prog="davasus-ingest",
        description="Ingest DaVaSus CSVs into a SQLite star-schema database.",
    )
    p.add_argument(
        "--merged",
        type=Path,
        required=True,
        help="Path to merged_eshepherd_smaxtec_weather_data_*.csv",
    )
    p.add_argument(
        "--weather",
        type=Path,
        required=True,
        help="Path to Weather_*.csv",
    )
    p.add_argument(
        "--db",
        type=Path,
        default=Path("cow.db"),
        help="Output SQLite path (default: cow.db).",
    )
    p.add_argument(
        "--chunk-size",
        type=int,
        default=50_000,
        help="Rows per insert batch for the merged file (default: 50000).",
    )
    p.add_argument(
        "--test-n",
        type=int,
        default=None,
        help="Cap rows read from the merged file. Useful for smoke tests.",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return p


def _configure_logging(verbose: bool) -> None:
    """Configure root logging for CLI use.

    Args:
        verbose: If ``True``, set level to ``DEBUG``; otherwise ``INFO``.
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def ingest_main(argv: list[str] | None = None) -> int:
    """Run the full ingestion pipeline.

    Args:
        argv: Optional argv list (used in tests). When ``None`` the
            real :data:`sys.argv` is consumed.

    Returns:
        Process exit code (``0`` on success).
    """
    args = _build_arg_parser().parse_args(argv)
    _configure_logging(args.verbose)

    if not args.merged.is_file():
        log.error("Merged CSV not found: %s", args.merged)
        return 2
    if not args.weather.is_file():
        log.error("Weather CSV not found: %s", args.weather)
        return 2

    started = time.perf_counter()
    with Database(args.db) as db:
        db.initialise()

        log.info("→ weather ingest")
        weather_n = WeatherIngestor(db).ingest(args.weather)

        log.info("→ merged ingest")
        merged_counts = MergedIngestor(db, chunk_size=args.chunk_size).ingest(
            args.merged, test_n=args.test_n
        )

        log.info("→ building indices")
        db.finalise()

        elapsed = time.perf_counter() - started
        log.info(
            "Done in %.1fs — weather=%d, eshepherd=%d, smaxtec=%d",
            elapsed, weather_n,
            merged_counts["eshepherd"],
            merged_counts["smaxtec"],
        )
    return 0


def _build_broken_stick_parser() -> argparse.ArgumentParser:
    """Build the ``davasus-broken-stick`` argument parser.

    Returns:
        Configured :class:`argparse.ArgumentParser`.
    """
    p = argparse.ArgumentParser(
        prog="davasus-broken-stick",
        description=(
            "Per-animal broken-stick fits of rumen temperature vs THI. "
            "Writes results CSV and figures into <figdir>."
        ),
    )
    p.add_argument("--db", type=Path, required=True, help="Path to cow.db")
    p.add_argument(
        "--figdir",
        type=Path,
        default=None,
        help="Output directory (default: <repo>/figures/04_heat).",
    )
    p.add_argument(
        "--thi-mode",
        choices=("nrc", "mader"),
        default="mader",
        help="THI variant (default: mader = wind+solar adjusted).",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return p


def broken_stick_main(argv: list[str] | None = None) -> int:
    """Run :class:`davasus.analysis.broken_stick.BrokenStickAnalysis`.

    Args:
        argv: Optional argv list (for tests).

    Returns:
        Exit code: ``0`` on success, ``2`` if the database is missing.
    """
    from davasus.analysis.broken_stick import BrokenStickAnalysis
    from davasus.extract import open_readonly

    args = _build_broken_stick_parser().parse_args(argv)
    _configure_logging(args.verbose)

    if not args.db.is_file():
        log.error("Database not found: %s", args.db)
        return 2

    figdir = args.figdir
    if figdir is None:
        repo_root = Path(__file__).resolve().parents[2]
        figdir = repo_root / "figures" / "04_heat"

    started = time.perf_counter()
    con = open_readonly(args.db)
    try:
        analysis = BrokenStickAnalysis(con, figdir=figdir, thi_mode=args.thi_mode)
        results = analysis.run()
    finally:
        con.close()

    n_ok = int(results["success"].sum())
    elapsed = time.perf_counter() - started
    log.info(
        "broken-stick done in %.1fs — %d/%d fits successful, outputs in %s",
        elapsed, n_ok, len(results), figdir,
    )
    return 0


def _build_validate_parser() -> argparse.ArgumentParser:
    """Build the ``davasus-validate`` argument parser.

    Returns:
        Configured :class:`argparse.ArgumentParser`.
    """
    p = argparse.ArgumentParser(
        prog="davasus-validate",
        description="Run plausibility checks on a DaVaSus SQLite database.",
    )
    p.add_argument("--db", type=Path, required=True, help="Path to cow.db")
    p.add_argument(
        "--json",
        type=Path,
        default=None,
        help="Optional path to write the report as JSON.",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return p


def validate_main(argv: list[str] | None = None) -> int:
    """Run the validation pipeline against an existing database.

    Args:
        argv: Optional argv list (used in tests). When ``None`` the
            real :data:`sys.argv` is consumed.

    Returns:
        Process exit code: ``0`` on a clean report, ``1`` if any orphan
        rows or out-of-range values were found, ``2`` on file errors.
    """
    args = _build_validate_parser().parse_args(argv)
    _configure_logging(args.verbose)

    if not args.db.is_file():
        log.error("Database not found: %s", args.db)
        return 2

    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    try:
        report = Validator(con, args.db).run()
    finally:
        con.close()

    print(render_report(report))

    if args.json is not None:
        args.json.write_text(report.to_json() + "\n")
        log.info("wrote JSON report → %s", args.json)

    flagged = (
        any(o > 0 for o in report.orphans.values())
        or any(r.out_of_range > 0 for r in report.ranges)
        or bool(report.warnings)
    )
    return 1 if flagged else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(ingest_main())
