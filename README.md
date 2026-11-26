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
davasus-vf-leadership             # Dumont (2005) first-mover analysis, tables and figure
davasus-vf-formation              # leading edge of the moving herd (positional leadership)
davasus-vf-pull-anchor            # who pulls whom: pair separation-reunion events
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

**Reduced GPS sensitivity.** Half-hours with GFZ Hp30 ≥ 6 (NOAA G2 or
stronger) are flagged as reduced GPS sensitivity. Analyses get the flag per
fix from `davasus.vf.gnss_quality.annotate_reduced_gps` and omit those
fixes; the dashboard shades them pink in the timeline and shows a banner
above the map.

**Leadership (Dumont 2005).** `davasus-vf-leadership` finds spontaneous
group movements (herd centroid at 10 m/min or faster for at least 3 min and
50 m, outside the two hours after a parcel activation and outside reduced
GPS periods), times each heifer's departure along the travel direction, and
credits the first mover (ties within 0.5 min split). It reports first-mover
shares with day-block bootstrap CIs, an egalitarian null (labels shuffled
within days) and a graded null (geometric worths), Plackett-Luce worths for
first places and full departure orders, Kendall's W for the whole order,
and individual initiation attempts with their followers against a
time-shifted chance baseline. Everything is run twice: on the raw
departures and with each collar's apparent sampling lag removed, because
fixes are stamped to the minute.

**Formation (positional leadership).** `davasus-vf-formation` asks which
heifers hold the leading edge while the herd moves. For every moving minute
it takes the front and rear arcs of the herd's convex hull (extreme points
within 45° of the travel direction and its opposite) and scores each heifer
by (front − rear) / (front + rear). Collar lag shifts a heifer along her own
path in proportion to speed, so the asymmetry is computed on raw and on
lag-corrected positions, next to the along-axis offset extrapolated to zero
herd speed, which lag cannot shift. A front or rear role is called only when
all three agree. A heading check guards against fixed geographic GPS
offsets.

**Pull and anchor (Strandburg-Peshkin et al. 2015).** `davasus-vf-pull-anchor`
finds events in which a pair's distance rises and falls again by at least
22 m. The heifer that moved more while the gap opened initiated the event;
if the partner then closed the gap it is a pull, if the initiator came back
it is an anchor. The thresholds sit above the GPS noise measured on still
heifers (IMU near zero for 15 min or more; 99 % of their pair-distance range
within 20 min is about 20 m), and moved distances are corrected by the
noise floor. Each heifer's lead share, pulls led / (led + followed), cancels
general mobility; it is tested against random pull directions, run on raw
and lag-corrected positions, and at 15 and 30 m. A heifer is called leader or
follower when her lead-share CI excludes 0.5 on the same side in both
scenarios.

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
