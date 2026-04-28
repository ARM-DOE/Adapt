# Adapt

[![CI](https://github.com/ARM-DOE/Adapt/actions/workflows/ci.yml/badge.svg)](https://github.com/ARM-DOE/Adapt/actions?query=workflow%3ACI)
[![Codecov](https://img.shields.io/codecov/c/github/ARM-DOE/Adapt.svg?logo=codecov)](https://codecov.io/gh/ARM-DOE/Adapt)
[![Docs](https://img.shields.io/badge/docs-users-4088b8.svg)](https://arm-doe.github.io/Adapt/)
[![Downloads](https://img.shields.io/github/downloads/ARM-DOE/Adapt/total?label=downloads)](https://github.com/ARM-DOE/Adapt/releases)
[![ARM](https://img.shields.io/badge/Sponsor-ARM-blue.svg?colorA=00c1de&colorB=00539c)](https://www.arm.gov/)

**Real-time processing for informed adaptive scanning of ARM weather radars**

`Adapt` is a framewrok for near real-time weather radar data processing for ARM operations and field campaigns. Currently, it ingests NEXRAD Level-II data, performs gridding/segmentation/analysis, and writes results for downstream visualization and scientific workflows.

## Installation (stable release)

1. Go to the [releases page](https://github.com/ARM-DOE/Adapt/releases) and download the latest `Source code (tar.gz)` or `Source code (zip)`.

2. Extract and create the environment:

```bash
# extract
tar -xzf Adapt-<version>.tar.gz
cd Adapt-<version>

# create environment and install adapt
mamba env create -f environment.yml
mamba activate adapt_env
```

## Quickstart

```bash
adapt run-nexrad --radar KLOT --base-dir ~/adapt_output
adapt dashboard --repo ~/adapt_output
```

Open the dashboard in a second terminal for live viewing.

## Documentation

- Detailed usage, configuration, outputs, and troubleshooting: `docs/USAGE.md`

## Status and compatibility

**Status: Alpha.** `Adapt` is under active development and is provided for early testing and evaluation.  
**No backward compatibility is guaranteed** for code, APIs, configuration, or generated data products (e.g., SQLite/Parquet/NetCDF). Expect breaking changes between commits and releases.  
Contribution guidelines and a roadmap will be published in a future release.

## Funding

`Adapt` is supported by the U.S. Department of Energy as part of the Atmospheric Radiation Measurement (ARM), an Office of Science User Facility.

## License

BSD license; see `LICENSE` for terms and disclaimer.
