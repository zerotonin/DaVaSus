"""Tests for :mod:`davasus.config`."""

from __future__ import annotations

import json

import pytest

from davasus.config import LocalPathsError, load_local_paths


def _write(tmp_path, profile_values):
    path = tmp_path / "local_paths.json"
    path.write_text(
        json.dumps({"active_profile": "local", "profiles": {"local": profile_values}}),
        encoding="utf-8",
    )
    return path


def test_missing_file_fails_loudly(tmp_path):
    with pytest.raises(LocalPathsError, match="local_paths.template.json"):
        load_local_paths(tmp_path / "local_paths.json")


def test_placeholder_values_are_rejected(tmp_path):
    path = _write(tmp_path, {"data_root": "<path>", "db_dir": "/x", "heifer_csv": "h.csv"})
    with pytest.raises(LocalPathsError, match="data_root"):
        load_local_paths(path)


def test_env_var_overrides_json(tmp_path, monkeypatch):
    path = _write(tmp_path, {"data_root": "/a", "db_dir": "/b", "heifer_csv": "h.csv"})
    monkeypatch.setenv("DAVASUS_DATA_ROOT", str(tmp_path))
    paths = load_local_paths(path)
    assert paths.data_root == tmp_path
    assert paths.heifer_csv_path == tmp_path / "h.csv"
