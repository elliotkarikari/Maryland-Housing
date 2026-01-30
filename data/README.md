# Data Directory

This directory contains data files used by the Maryland Housing Atlas.

## Structure

```
data/
├── cache/           # Downloaded data cache (gitignored)
│   ├── economic_v2/ # Layer 1 LODES/ACS cache
│   ├── mobility_v2/ # Layer 2 OSM/GTFS cache
│   ├── schools/     # Layer 3 NCES cache
│   ├── housing/     # Layer 4 ACS/BPS cache
│   └── demographics/# Layer 5 IRS/ACS cache
├── raw/             # Raw downloaded files (gitignored)
└── temp/            # Temporary processing files (gitignored)
```

## Cache Behavior

- **Automatic caching**: Data is cached locally after download to avoid repeated API calls
- **Expiration**: Different data sources have different cache lifetimes:
  - OSM data: 30 days
  - GTFS feeds: 7 days
  - Census/LODES: No expiration (stable archives)
- **Location**: Cache files are stored in `~/.cache/pygris` for Census boundaries

## Data Sources

| Layer | Source | Update Frequency |
|-------|--------|------------------|
| Layer 1 | Census LODES WAC | Annual |
| Layer 2 | OpenStreetMap, GTFS | Monthly |
| Layer 3 | NCES, State DoE | Annual |
| Layer 4 | Census ACS, BPS | Annual |
| Layer 5 | IRS SOI, Census | Annual |
| Layer 6 | FEMA NFHL, EPA, CDC | Varies |

## Clearing Cache

To clear all cached data and force a fresh download:

```bash
rm -rf data/cache/
rm -rf ~/.cache/pygris
```

Note: Regenerating cache may take 30-60 minutes depending on network speed.

## Git Ignore

The following are gitignored to prevent accidental commits:

- `data/cache/**`
- `data/raw/**`
- `data/temp/**`

Only this README and `.gitkeep` files are version controlled.
