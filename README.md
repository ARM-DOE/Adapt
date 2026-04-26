# Adapt

**Real-time processing for informed adaptive scanning of ARM weather radars**

`Adapt` is a framewrok for near real-time weather radar data processing for ARM operations and field campaigns. Currently, it ingests NEXRAD Level-II data, performs gridding/segmentation/analysis, and writes results for downstream visualization and scientific workflows.

## Quickstart

```bash
git clone https://github.com/ARM-DOE/Adapt.git
cd Adapt
mamba env create -f environment.yml
mamba activate adapt_env

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
