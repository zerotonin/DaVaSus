# DaVaSus

![Tests](https://github.com/zerotonin/DaVaSus/actions/workflows/tests.yml/badge.svg)
![Lint](https://github.com/zerotonin/DaVaSus/actions/workflows/lint.yml/badge.svg)

Pasture-based mother-cow sensor data pipeline. Sister project to
[DigiMuh](https://github.com/zerotonin/digimuh) (stall-based dairy);
DaVaSus is the pasture counterpart with smaXtec rumen boluses,
eShepherd virtual-fence collars, an ATB Potsdam weather station, and
operator-driven allocation events.

The scientific aim is to compare circadian zeitgebers between human-paced
stall life and pasture life — quantifying how much of the rumen-temperature
rhythm is locked to the sun versus to the allocation schedule.

## Status

Pre-alpha — ingestion pipeline under construction.

## Installation

```bash
git clone <url>
cd DaVaSus

# conda (recommended); installs the analysis, stats and dev extras
conda env create -f environment.yml
conda activate davasus

# or pip
pip install -e ".[analysis,stats,dev]"
```

The `stats` extra pulls [reRandomStats](https://github.com/zerotonin/reRandomStats)
(pinned tag) from GitHub.

### Machine-specific paths (do this first)

```bash
cp local_paths.template.json local_paths.json
```

Fill in `data_root` (folder with the raw CSVs), `db_dir` (where the SQLite
databases are written) and `heifer_csv` for each profile you use. The file is
gitignored. Any key can be overridden with `DAVASUS_<KEY>` (for example
`DAVASUS_DATA_ROOT`); `DAVASUS_PROFILE` picks the profile. Check the result
with `python -m davasus.config`; it fails with a clear message when the file
is missing or a placeholder is still set.

## Quick start

```bash
# Smoke test with the first 100k rows of the merged file
davasus-ingest \
    --merged /media/geuba03p/DATADRIVE1/DaVaSus/merged_eshepherd_smaxtec_weather_data_2024.csv \
    --weather /media/geuba03p/DATADRIVE1/DaVaSus/Weather_20240221to20241218.csv \
    --db cow_test.db \
    --test-n 100000

# Full ingestion
davasus-ingest \
    --merged /media/geuba03p/DATADRIVE1/DaVaSus/merged_eshepherd_smaxtec_weather_data_2024.csv \
    --weather /media/geuba03p/DATADRIVE1/DaVaSus/Weather_20240221to20241218.csv \
    --db cow.db
```

### Heifer virtual-fence study (M8)

```bash
# paths come from local_paths.json (heifer_csv, db_dir)
davasus-vf-ingest                 # writes <db_dir>/heifer_vf.db
davasus-vf-ingest --test-n 50000  # smoke test
davasus-vf-ingest --overwrite     # rebuild from the CSV
```

The heifer data go into their own database, `heifer_vf.db`, separate from
`cow.db`: a different herd, and the CSV's repeated weather, parcel log and
animal register are stored once each. Raw values are kept; only the
eShepherd no-fence sentinel becomes `NULL`. Plausibility filters are applied
at analysis time.

```bash
davasus-vf-figures                # 24 h trajectories, cue timeline, herd movie
davasus-vf-spaceweather           # GFZ Hp30 / ap30 / Kp for the trial into heifer_vf.db
davasus-vf-dashboard              # interactive desktop dashboard (needs the gui extra)
```

**Dashboard.** `pip install -e ".[gui]"` (PySide6, pyqtgraph). The map shows
every heifer at the current minute, coloured by its eShepherd `Outside`
code, with trails and the herd centroid; click a heifer or a table row to
select it. Dockable panels show the herd table, the selected heifer
(register, position, fence distance, cues and SR), the active parcel event,
weather with THI and space weather. The timeline at the bottom covers the
whole trial; click or drag in it to jump. Keys: Space plays or pauses,
Left/Right step one minute, Shift+Left/Right one hour. The dock layout is
remembered; *View → Reset layout* restores it.

The basemap uses OpenStreetMap tiles (© OpenStreetMap contributors),
downloaded once into `<data_root>/osm_tiles` and reused offline; pass
`--no-tiles` to skip them. Space-weather indices are from GFZ Potsdam
(CC BY 4.0) and are cached as JSON under `<data_root>/space_weather`.

## Repository layout

```
DaVaSus/
├── src/davasus/        # package
│   ├── schema.py       # CREATE TABLE statements (single source of truth)
│   ├── db.py           # Database connection + dim-table helpers
│   ├── ingest_weather.py
│   ├── ingest_merged.py
│   └── cli.py          # davasus-ingest entry point
├── tests/              # pytest, synthetic CSV fixtures
├── docs/               # database structure, column dictionary, pipeline
├── figures/            # generated figures (PNG + editable-text SVG + CSV); gitignored
├── scripts/            # ops scripts (run inside the repo)
└── .github/workflows/  # CI (pytest, ruff)
```

## Documentation

- `docs/database_structure.md` — full schema
- `docs/column_dictionary.md` — column-level data dictionary
- `docs/pipeline.md` — end-to-end pipeline overview

## License

MIT
