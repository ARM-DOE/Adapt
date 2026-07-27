# ARM Adapt

**Real-time processing for informed adaptive scanning of ARM weather radar operations and field campaigns.**

![Status](https://img.shields.io/badge/STATUS-ACTIVE%20DEVELOPMENT-orange?style=for-the-badge&logo=github)
![API](https://img.shields.io/badge/API-BREAKING%20CHANGES-red?style=for-the-badge&logo=dependabot)
![Stability](https://img.shields.io/badge/STABILITY-ALPHA-yellow?style=for-the-badge)

[![CI](https://github.com/ARM-DOE/Adapt/actions/workflows/ci.yml/badge.svg)](https://github.com/ARM-DOE/Adapt/actions/workflows/ci.yml)
[![Docs](https://github.com/ARM-DOE/Adapt/actions/workflows/docs.yml/badge.svg)](https://github.com/ARM-DOE/Adapt/actions/workflows/docs.yml)
[![codecov](https://img.shields.io/codecov/c/github/ARM-DOE/Adapt.svg?logo=codecov)](https://codecov.io/gh/ARM-DOE/Adapt)
[![CodeFactor](https://www.codefactor.io/repository/github/arm-doe/adapt/badge)](https://www.codefactor.io/repository/github/arm-doe/adapt)
[![PyPI Release](https://github.com/ARM-DOE/Adapt/actions/workflows/pypi-release.yml/badge.svg)](https://github.com/ARM-DOE/Adapt/actions/workflows/pypi-release.yml)
[![Downloads](https://static.pepy.tech/personalized-badge/arm-adapt?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pypi.org/project/arm-adapt/)
[![License](https://img.shields.io/pypi/l/arm-adapt)](https://github.com/ARM-DOE/Adapt?tab=License-1-ov-file)
[![ARM Sponsor](https://img.shields.io/badge/Sponsor-ARM-blue.svg?colorA=00c1de&colorB=00539c)](https://www.arm.gov/)

> **Note:** Adapt is under active development. We are not accepting external Pull Requests at this time.
> Contribution guidelines will be published after the stable release. Expect frequent breaking changes
> in APIs, configuration files, database schemas, and outputs.

---

## Overview

**ARM Adapt** is a configuration-driven modular framework for near real-time analysis of convective systems
designed to support adaptive sampling and study of convective storms and their life cycles. The system
implements a modular pipeline that ingests radar observations, performs gridding and segmentation to identify
convective cells, and maintains their identity through time using tracking. It further derives cell-level
properties and motion to characterize storm evolution and generate candidate targets for adaptive radar scanning.

Adapt operates in both real-time and archival modes, producing standardized data products in the form of
gridded fields, tabular summaries, and relational tracking records. Its design emphasizes reproducibility,
extensibility, and consistency, allowing new analysis methods and data sources to be integrated without
altering core workflows.

Currently it ingests NEXRAD Level-II data, performs gridding, segmentation, and analysis, and writes
results for downstream visualization and scientific workflows.

---

## Pipeline

```
AWS S3 (NEXRAD Level-II)
        │
        ▼
   Downloader
        │  raw files
        ▼
   Processor
        ├─ Ingest      (Py-ART + xarray → Cartesian grid)
        ├─ Detection   (threshold + morphology → cell labels)
        ├─ Projection  (optical flow → future cell positions)
        ├─ Analysis    (per-cell statistics)
        └─ Tracking    (stable cell identities across scans)
        │
        ▼
   Output repository  (NetCDF + Parquet + SQLite)
        │
        ▼
   Dashboard  (live Tkinter GUI)
```

---

## Installation

Create a fresh conda environment (Python 3.13) and install from PyPI:

```bash
conda create -n adapt_env python=3.13 -y
conda activate adapt_env
pip install arm-adapt
adapt --help
```

The environment must be **fresh**. Adapt's compiled dependencies (netCDF4, h5py,
opencv, Py-ART) ship both conda and pip builds of the same native libraries;
installing the pip wheels over existing conda builds mixes incompatible binaries
and fails at import with a DLL error — on Windows, `[WinError 11] An attempt was
made to load a program with an incorrect format`. On Windows, use 64-bit x86
Python: several of these dependencies publish no ARM64 wheels.

---

## Quickstart

```bash
# Run real-time processing on a NEXRAD radar
adapt run-nexrad --radar KLOT --base-dir ~/adapt_output

# Open the dashboard in a second terminal
adapt dashboard --repo ~/adapt_output
```

---

## Status and Compatibility

- **Status:** Alpha — provided for early testing and evaluation.
- No backward compatibility is guaranteed for code, APIs, configuration, or generated data products
  (SQLite, Parquet, NetCDF) until the first stable release.

---

## Funding

Adapt is supported by the U.S. Department of Energy as part of the Atmospheric Radiation Measurement
(ARM) User Facility within the Office of Science.

---

## License

Copyright © 2026, UChicago Argonne, LLC.
See the [LICENSE](https://github.com/ARM-DOE/Adapt/blob/main/LICENSE) file for terms and disclaimer.
