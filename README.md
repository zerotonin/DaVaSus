# DaVaSus

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

# conda (recommended)
conda env create -f environment.yml
conda activate davasus

# or pip
pip install -e ".[dev]"
```

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
