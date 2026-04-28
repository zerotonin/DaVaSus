"""Command-line entry points for DaVaSus.

Currently exposes ``davasus-ingest`` (see :func:`ingest_main`).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from davasus.db import Database
from davasus.ingest_merged import MergedIngestor
from davasus.ingest_weather import WeatherIngestor

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


if __name__ == "__main__":  # pragma: no cover
    sys.exit(ingest_main())
