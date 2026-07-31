# Adapt

**Real-time detection, projection, and tracking of convective storms in weather radar data, built to support adaptive radar scanning at the DOE ARM User Facility.**

![Status](https://img.shields.io/badge/STATUS-ACTIVE%20DEVELOPMENT-orange?style=for-the-badge&logo=github)
![API](https://img.shields.io/badge/API-BREAKING%20CHANGES-red?style=for-the-badge&logo=dependabot)
![Stability](https://img.shields.io/badge/STABILITY-ALPHA-yellow?style=for-the-badge)

[![CI](https://github.com/ARM-DOE/Adapt/actions/workflows/ci.yml/badge.svg)](https://github.com/ARM-DOE/Adapt/actions/workflows/ci.yml)
[![Docs](https://github.com/ARM-DOE/Adapt/actions/workflows/docs.yml/badge.svg)](https://github.com/ARM-DOE/Adapt/actions/workflows/docs.yml)
[![codecov](https://img.shields.io/codecov/c/github/ARM-DOE/Adapt.svg?logo=codecov)](https://codecov.io/gh/ARM-DOE/Adapt)
[![CodeFactor](https://www.codefactor.io/repository/github/arm-doe/adapt/badge)](https://www.codefactor.io/repository/github/arm-doe/adapt)
[![PyPI Release](https://github.com/ARM-DOE/Adapt/actions/workflows/pypi-release.yml/badge.svg)](https://github.com/ARM-DOE/Adapt/actions/workflows/pypi-release.yml)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/arm-adapt.svg)](https://anaconda.org/conda-forge/arm-adapt)
[![Downloads](https://static.pepy.tech/personalized-badge/arm-adapt?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pypi.org/project/arm-adapt/)
[![License](https://img.shields.io/pypi/l/arm-adapt)](https://github.com/ARM-DOE/Adapt?tab=License-1-ov-file)
[![ARM Sponsor](https://img.shields.io/badge/Sponsor-ARM-blue.svg?colorA=00c1de&colorB=00539c)](https://www.arm.gov/)

> **Note.** Adapt is under active development. External pull requests are not being accepted at this
> time; contribution guidelines will follow the first stable release. APIs, configuration files,
> database schemas, and output formats are subject to change without notice.

**Documentation:** [arm-doe.github.io/Adapt](https://arm-doe.github.io/Adapt/) — [Installation](https://arm-doe.github.io/Adapt/installation.html) · [User Guide](https://arm-doe.github.io/Adapt/USAGE.html) · [CLI Reference](https://arm-doe.github.io/Adapt/cli_reference.html)

---

## Overview

Adapt is a modular framework for real-time convective cell detection, motion projection, and storm
lifecycle analysis. It processes a stream of radar volume scans and, for each cell, produces a tracked
identity, a set of measured properties, and a record of lifecycle events — initiation, growth,
splitting, merging, and decay. Results are written to a structured, queryable repository (NetCDF,
Parquet, and SQLite) exposed through a read-only Python API, rather than as loose output files.

The processing pipeline is composed of independently registered modules — ingest, detection, projection,
analysis, and tracking — assembled automatically at run time. New algorithms and data sources are added
by registering a module, not by modifying the pipeline. Every run is deterministic and reproducible:
runs are registered, derived quantities are traceable to their inputs, and identical inputs produce
identical tracks.

## Motivation

Adapt is developed to support adaptive scanning at the U.S. Department of Energy's Atmospheric Radiation
Measurement (ARM) User Facility, where the C-Band Scanning ARM Precipitation Radar (CSAPR2) requires a
real-time source of high-value scan targets, identified and prioritized automatically rather than by a
fixed volume-scan schedule. The same pipeline runs unchanged on historical data, supporting research
applications such as storm lifecycle studies, radar–lightning correlation analysis, and the construction
of analysis-ready, multi-sensor datasets combining radar, satellite, and Lightning Mapping Array
observations.

## Current status

Adapt is at an alpha stage of development.

- The reference NEXRAD pipeline (ingest, detection, projection, analysis, tracking) runs end to end in
  real-time and historical modes.
- A read-only query API and GUI dashboard provide access to pipeline output.
- Post-processing modules enrich completed storm tracks with observations from other instruments.
- Rule-based target selection for adaptive scanning is under active development.
- Supported platforms: macOS, Linux, and Windows. See [Installation](https://arm-doe.github.io/Adapt/installation.html).

No backward compatibility is guaranteed for APIs, configuration, or generated data products until the
first stable release.

## Roadmap

Planned work extends Adapt toward a general, multi-sensor storm-analysis framework: pluggable tracking
and segmentation backends, a data-agnostic variable layer so new instruments are a configuration change
rather than a code change, and integration with ARM radar-control systems to close the loop between
detection and adaptive scanning. See [Vision](https://arm-doe.github.io/Adapt/vision.html) for details.

---

## Funding

Adapt is supported by the U.S. Department of Energy as part of the Atmospheric Radiation Measurement
(ARM) User Facility, within the Office of Science.

## License

Copyright © 2026, UChicago Argonne, LLC.
See [LICENSE](https://github.com/ARM-DOE/Adapt/blob/main/LICENSE) for terms and disclaimer.
