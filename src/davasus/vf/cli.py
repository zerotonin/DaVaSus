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

from davasus.config import LocalPathsError, load_local_paths
from davasus.vf.ingest import CHUNK_ROWS, HeiferVFIngestor

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


if __name__ == "__main__":
    sys.exit(ingest_main())
