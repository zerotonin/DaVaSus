# ╔══════════════════════════════════════════════════════════════════╗
# ║  DaVaSus — config                                                ║
# ║  « machine-specific paths live outside git »                     ║
# ╠══════════════════════════════════════════════════════════════════╣
# ║  Resolves data roots and SLURM identifiers from the gitignored   ║
# ║  local_paths.json (template: local_paths.template.json).         ║
# ║  Order: DAVASUS_<KEY> environment variable, then the active      ║
# ║  profile in local_paths.json. No silent fallback.                ║
# ╚══════════════════════════════════════════════════════════════════╝
"""Per-machine path resolution (env var > ``local_paths.json``)."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT: Path = Path(__file__).resolve().parents[2]
LOCAL_PATHS_FILE: Path = REPO_ROOT / "local_paths.json"
TEMPLATE_FILE: Path = REPO_ROOT / "local_paths.template.json"
PROFILE_ENV_VAR = "DAVASUS_PROFILE"
ENV_PREFIX = "DAVASUS_"
PATH_KEYS: tuple[str, ...] = ("data_root", "db_dir")


class LocalPathsError(RuntimeError):
    """Raised when ``local_paths.json`` is missing or incomplete."""


# ┌────────────────────────────────────────────────────────────┐
# │ Resolved configuration  « one frozen record per machine »  │
# └────────────────────────────────────────────────────────────┘


@dataclass(frozen=True)
class LocalPaths:
    """Resolved machine-specific settings for one profile."""

    profile: str
    data_root: Path
    db_dir: Path
    heifer_csv: str
    slurm_account: str | None = None
    slurm_partition: str | None = None

    @property
    def heifer_csv_path(self) -> Path:
        return self.data_root / self.heifer_csv

    @property
    def heifer_db_path(self) -> Path:
        return self.db_dir / "heifer_vf.db"


def load_local_paths(path: Path = LOCAL_PATHS_FILE, profile: str | None = None) -> LocalPaths:
    """Load one profile from ``local_paths.json``; env vars override keys.

    Raises:
        LocalPathsError: file missing, profile unknown, or a key unset.
    """
    raw = _read_json(path)
    name = profile or os.environ.get(PROFILE_ENV_VAR) or raw.get("active_profile", "local")
    profiles = raw.get("profiles", {})
    if name not in profiles:
        raise LocalPathsError(f"Profile {name!r} not in {path.name}; have {sorted(profiles)}")
    values = {**profiles[name]}
    for key in (*PATH_KEYS, "heifer_csv", "slurm_account", "slurm_partition"):
        override = os.environ.get(ENV_PREFIX + key.upper())
        if override:
            values[key] = override
    _require_filled(values, (*PATH_KEYS, "heifer_csv"), name)
    return LocalPaths(
        profile=name,
        data_root=Path(values["data_root"]).expanduser(),
        db_dir=Path(values["db_dir"]).expanduser(),
        heifer_csv=values["heifer_csv"],
        slurm_account=values.get("slurm_account"),
        slurm_partition=values.get("slurm_partition"),
    )


def _read_json(path: Path) -> dict:
    if not path.exists():
        raise LocalPathsError(
            f"{path} not found. Copy {TEMPLATE_FILE.name} to {path.name} "
            "and fill in data_root, db_dir and heifer_csv for this machine."
        )
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _require_filled(values: dict, keys: tuple[str, ...], profile: str) -> None:
    missing = [k for k in keys if not values.get(k) or str(values[k]).startswith("<")]
    if missing:
        raise LocalPathsError(f"Profile {profile!r} has unset keys: {', '.join(missing)}")


def main() -> None:
    """Print the resolved profile, or ``--export`` it as shell variables."""
    try:
        paths = load_local_paths()
    except LocalPathsError as exc:
        sys.exit(f"error: {exc}")
    for key, value in vars(paths).items():
        if "--export" in sys.argv:
            print(f"export {ENV_PREFIX}{key.upper()}='{value if value is not None else ''}'")
        else:
            print(f"{key:16s} {value}")


if __name__ == "__main__":
    main()
